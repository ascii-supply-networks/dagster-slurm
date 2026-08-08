"""Application-level helpers for workloads on a shared Ray cluster."""

from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from importlib import import_module
import math
import time
from typing import TypeVar


PrimaryResultT = TypeVar("PrimaryResultT")
ReserveResultT = TypeVar("ReserveResultT")


def _timeout_error(
    minimum_resources: Mapping[str, float],
    timeout_seconds: float,
    last_snapshot: Mapping[str, float],
) -> TimeoutError:
    return TimeoutError(
        "Ray resources did not stabilize before the "
        f"{timeout_seconds:g}s timeout: required {dict(minimum_resources)!r}, "
        f"last snapshot {dict(last_snapshot)!r}"
    )


def wait_for_stable_ray_resources(
    minimum_resources: Mapping[str, float],
    *,
    stable_polls: int = 2,
    poll_interval_seconds: float = 1.0,
    timeout_seconds: float | None = None,
    resource_provider: Callable[[], Mapping[str, float]] | None = None,
    provider_retry_attempts: int = 3,
    provider_retry_backoff_seconds: float = 1.0,
) -> dict[str, float]:
    """Wait until Ray resources stay available across consecutive polls.

    This observes capacity; it does not reserve it. Callers should submit the
    work that claims the returned capacity immediately. A common use is to
    launch a second Ray actor pool after co-tenant workloads release resources,
    because an actor pool that is already running cannot be resized.

    Args:
        minimum_resources: Resource quantities that must all be available, such
            as ``{"GPU": 1, "CPU": 4}``.
        stable_polls: Number of consecutive qualifying snapshots required.
        poll_interval_seconds: Delay between resource snapshots.
        timeout_seconds: Maximum wait time, or no limit when omitted.
        resource_provider: Optional provider compatible with
            ``ray.available_resources``. Primarily useful for custom Ray
            integrations and deterministic tests.
        provider_retry_attempts: Total attempts allowed for each resource
            snapshot when the provider raises.
        provider_retry_backoff_seconds: Initial retry delay. Subsequent delays
            use exponential backoff.

    Returns:
        The final qualifying resource snapshot.

    Raises:
        ImportError: If Ray is not installed and no provider is supplied.
        RuntimeError: If the resource provider exhausts its retry attempts.
        TimeoutError: If capacity does not stabilize before the timeout.
        ValueError: If polling or resource requirements are invalid.
    """
    if not minimum_resources:
        raise ValueError("minimum_resources must not be empty")
    if any(
        not name or not math.isfinite(quantity) or quantity <= 0
        for name, quantity in minimum_resources.items()
    ):
        raise ValueError(
            "resource names must be non-empty and quantities positive and finite"
        )
    if stable_polls < 1:
        raise ValueError("stable_polls must be at least 1")
    if not math.isfinite(poll_interval_seconds) or poll_interval_seconds <= 0:
        raise ValueError("poll_interval_seconds must be positive and finite")
    if timeout_seconds is not None and (
        not math.isfinite(timeout_seconds) or timeout_seconds < 0
    ):
        raise ValueError("timeout_seconds must be non-negative and finite")
    if provider_retry_attempts < 1:
        raise ValueError("provider_retry_attempts must be at least 1")
    if (
        not math.isfinite(provider_retry_backoff_seconds)
        or provider_retry_backoff_seconds < 0
    ):
        raise ValueError(
            "provider_retry_backoff_seconds must be non-negative and finite"
        )

    if resource_provider is None:
        try:
            ray_module = import_module("ray")
        except ImportError as exc:
            raise ImportError(
                "Ray capacity polling requires the 'ray' extra: "
                "install dagster-slurm[ray]"
            ) from exc
        resource_provider = ray_module.available_resources

    started_at = time.monotonic()
    qualifying_polls = 0
    last_snapshot: dict[str, float] = {}
    while True:
        for attempt in range(provider_retry_attempts):
            try:
                snapshot = dict(resource_provider())
                last_snapshot = snapshot
                break
            except Exception as exc:
                if attempt + 1 >= provider_retry_attempts:
                    raise RuntimeError(
                        "Ray resource provider failed after "
                        f"{provider_retry_attempts} attempts"
                    ) from exc

                elapsed = time.monotonic() - started_at
                if timeout_seconds is not None and elapsed >= timeout_seconds:
                    raise _timeout_error(
                        minimum_resources,
                        timeout_seconds,
                        last_snapshot,
                    ) from exc
                retry_delay = provider_retry_backoff_seconds * (2**attempt)
                if timeout_seconds is not None:
                    retry_delay = min(retry_delay, timeout_seconds - elapsed)
                time.sleep(retry_delay)

        if all(
            snapshot.get(name, 0.0) >= quantity
            for name, quantity in minimum_resources.items()
        ):
            qualifying_polls += 1
            if qualifying_polls >= stable_polls:
                return snapshot
        else:
            qualifying_polls = 0

        elapsed = time.monotonic() - started_at
        if timeout_seconds is not None and elapsed >= timeout_seconds:
            raise _timeout_error(
                minimum_resources,
                timeout_seconds,
                last_snapshot,
            )
        sleep_seconds = poll_interval_seconds
        if timeout_seconds is not None:
            sleep_seconds = min(sleep_seconds, timeout_seconds - elapsed)
        time.sleep(sleep_seconds)


def run_with_ray_reserve_topup(
    primary: Callable[[], PrimaryResultT],
    reserve: Callable[[Mapping[str, float]], ReserveResultT],
    minimum_resources: Mapping[str, float],
    *,
    stable_polls: int = 2,
    poll_interval_seconds: float = 1.0,
    timeout_seconds: float | None = None,
    resource_provider: Callable[[], Mapping[str, float]] | None = None,
    provider_retry_attempts: int = 3,
    provider_retry_backoff_seconds: float = 1.0,
) -> tuple[PrimaryResultT, ReserveResultT | None]:
    """Run primary work immediately and top it up after capacity stabilizes.

    ``primary`` and the capacity watcher start concurrently. Once the watcher
    confirms persistent idle resources, ``reserve`` receives the qualifying
    snapshot and runs alongside any primary work still in progress. If the
    capacity wait times out, the primary result is returned with no reserve
    result. The callbacks can each create and materialize an independent Ray
    actor pool; callers remain responsible for merging their returned data.

    Returns:
        A pair containing the primary result and the optional reserve result.
    """
    with ThreadPoolExecutor(max_workers=2) as executor:
        primary_future = executor.submit(primary)
        capacity_future = executor.submit(
            wait_for_stable_ray_resources,
            minimum_resources,
            stable_polls=stable_polls,
            poll_interval_seconds=poll_interval_seconds,
            timeout_seconds=timeout_seconds,
            resource_provider=resource_provider,
            provider_retry_attempts=provider_retry_attempts,
            provider_retry_backoff_seconds=provider_retry_backoff_seconds,
        )
        try:
            available_resources = capacity_future.result()
        except TimeoutError:
            return primary_future.result(), None

        if primary_future.done():
            primary_future.result()
        reserve_future = executor.submit(reserve, available_resources)
        return primary_future.result(), reserve_future.result()
