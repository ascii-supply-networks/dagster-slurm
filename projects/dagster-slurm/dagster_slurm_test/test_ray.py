from collections.abc import Callable, Mapping
from threading import Event

import pytest

import dagster_slurm.ray as ray_helpers
from dagster_slurm import (
    run_with_ray_reserve_topup,
    wait_for_stable_ray_resources,
)


def _resource_provider(
    snapshots: list[Mapping[str, float]],
) -> Callable[[], Mapping[str, float]]:
    iterator = iter(snapshots)
    return lambda: next(iterator)


def test_wait_for_stable_ray_resources_debounces_transient_capacity(monkeypatch):
    provider = _resource_provider(
        [
            {"GPU": 2.0, "CPU": 8.0, "poll": 1.0},
            {"GPU": 0.0, "CPU": 8.0, "poll": 2.0},
            {"GPU": 2.0, "CPU": 8.0, "poll": 3.0},
            {"GPU": 2.0, "CPU": 8.0, "poll": 4.0},
        ]
    )
    monkeypatch.setattr(ray_helpers.time, "sleep", lambda _seconds: None)

    result = wait_for_stable_ray_resources(
        {"GPU": 2.0, "CPU": 4.0},
        stable_polls=2,
        resource_provider=provider,
    )

    assert result == {"GPU": 2.0, "CPU": 8.0, "poll": 4.0}


def test_wait_for_stable_ray_resources_times_out_with_last_snapshot():
    with pytest.raises(TimeoutError, match="last snapshot.*'GPU': 0.0"):
        wait_for_stable_ray_resources(
            {"GPU": 1.0},
            timeout_seconds=0,
            resource_provider=lambda: {"GPU": 0.0},
        )


def test_wait_for_stable_ray_resources_retries_transient_provider_error(
    monkeypatch,
):
    responses = iter(
        [
            RuntimeError("transient RPC failure"),
            {"GPU": 1.0},
            {"GPU": 1.0},
        ]
    )

    def provider() -> Mapping[str, float]:
        response = next(responses)
        if isinstance(response, Exception):
            raise response
        return response

    sleeps: list[float] = []
    monkeypatch.setattr(ray_helpers.time, "sleep", sleeps.append)

    result = wait_for_stable_ray_resources(
        {"GPU": 1.0},
        stable_polls=2,
        resource_provider=provider,
        provider_retry_attempts=2,
        provider_retry_backoff_seconds=0.25,
    )

    assert result == {"GPU": 1.0}
    assert sleeps == [0.25, 1.0]


def test_wait_for_stable_ray_resources_reports_exhausted_provider_retries(
    monkeypatch,
):
    monkeypatch.setattr(ray_helpers.time, "sleep", lambda _seconds: None)

    with pytest.raises(RuntimeError, match="failed after 2 attempts"):
        wait_for_stable_ray_resources(
            {"GPU": 1.0},
            resource_provider=lambda: (_ for _ in ()).throw(RuntimeError("RPC")),
            provider_retry_attempts=2,
        )


def test_run_with_ray_reserve_topup_starts_primary_before_reserve(monkeypatch):
    primary_started = Event()
    reserve_started = Event()

    def primary() -> str:
        primary_started.set()
        assert reserve_started.wait(timeout=1)
        return "primary"

    def provider() -> Mapping[str, float]:
        assert primary_started.wait(timeout=1)
        return {"GPU": 2.0, "CPU": 8.0}

    def reserve(snapshot: Mapping[str, float]) -> int:
        reserve_started.set()
        return int(snapshot["GPU"])

    monkeypatch.setattr(ray_helpers.time, "sleep", lambda _seconds: None)

    result = run_with_ray_reserve_topup(
        primary,
        reserve,
        {"GPU": 1.0, "CPU": 4.0},
        resource_provider=provider,
    )

    assert result == ("primary", 2)


def test_run_with_ray_reserve_topup_skips_reserve_after_timeout():
    reserve_called = False

    def reserve(_snapshot: Mapping[str, float]) -> None:
        nonlocal reserve_called
        reserve_called = True

    result = run_with_ray_reserve_topup(
        lambda: "primary",
        reserve,
        {"GPU": 1.0},
        timeout_seconds=0,
        resource_provider=lambda: {},
    )

    assert result == ("primary", None)
    assert reserve_called is False


@pytest.mark.parametrize(
    ("minimum_resources", "kwargs", "message"),
    [
        ({}, {}, "must not be empty"),
        ({"GPU": 0.0}, {}, "quantities positive and finite"),
        ({"GPU": 1.0}, {"stable_polls": 0}, "at least 1"),
        ({"GPU": 1.0}, {"poll_interval_seconds": 0}, "must be positive"),
        ({"GPU": 1.0}, {"timeout_seconds": -1}, "must be non-negative"),
        ({"GPU": 1.0}, {"provider_retry_attempts": 0}, "at least 1"),
        (
            {"GPU": 1.0},
            {"provider_retry_backoff_seconds": -1},
            "must be non-negative",
        ),
    ],
)
def test_wait_for_stable_ray_resources_validates_configuration(
    minimum_resources,
    kwargs,
    message,
):
    with pytest.raises(ValueError, match=message):
        wait_for_stable_ray_resources(
            minimum_resources,
            resource_provider=lambda: {},
            **kwargs,
        )
