"""Hooks for scaling policies that live in the application.

The session observes its allocations, estimates when candidate worker
allocations could start, and applies a desired number of workers per intent.
How many workers help, which shapes to try and whether an allocation is worth
its startup cost stay with the workload.
"""

from __future__ import annotations

import re
import shlex
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal, Protocol

from .helpers.ssh_helpers import TERMINAL_STATES

if TYPE_CHECKING:
    from .resources.session import SlurmSessionResource, SlurmWorkerAllocationConfig

ENDED = "ENDED"
"""State of a job that left the queue without a final state in accounting."""

AllocationRole = Literal["head", "successor", "worker"]

_INTENT_KEY_RE = re.compile(r"[A-Za-z0-9_.-]{1,64}")
_START_RE = re.compile(r"to start at (?P<start>\S+)")
_NODES_RE = re.compile(r" on nodes (?P<nodes>\S+)")
_PARTITION_RE = re.compile(r" in partition (?P<partition>\S+)")


@dataclass(frozen=True)
class AllocationSnapshot:
    """One allocation of a session at the time of a snapshot.

    ``None`` means the value could not be observed: callers should not read
    it as zero, false or pending.
    """

    slurm_job_id: int
    role: AllocationRole
    state: str | None
    """Scheduler state, ``ENDED`` without an accounting record, ``None`` if unknown."""
    reason: str | None = None
    """Scheduler reason, such as ``Resources`` or ``Priority`` for a pending job."""
    time_left_seconds: float | None = None
    """Remaining walltime of a running job, as the scheduler reports it."""
    time_limit_seconds: int | None = None
    requested_nodes: int | None = None
    requested_gpus_per_node: int | None = None
    tres: Mapping[str, str] | None = None
    """Trackable resources from ``squeue``: the grant once the job runs."""
    nodes: tuple[str, ...] = ()
    ray_nodes: Mapping[str, str] | None = None
    """Ray node ID per Slurm node that joined the current head (workers only)."""
    draining: bool | None = None
    intent: str | None = None
    """Intent key of a worker allocation submitted by ``reconcile_workers``."""
    drain_deadline: float | None = None
    """Login-node epoch seconds after which ``reconcile_workers`` cancels it."""
    submitted_at: float | None = None
    """Login-node epoch seconds when ``reconcile_workers`` submitted it."""

    @property
    def ended(self) -> bool:
        return self.state is not None and (
            self.state in TERMINAL_STATES or self.state == ENDED
        )

    @property
    def committed(self) -> bool | None:
        """Whether the allocation holds or waits for resources (pending or running)."""
        return None if self.state is None else not self.ended

    @property
    def granted_gpus(self) -> int | None:
        """GPUs the scheduler granted to the running job, across its nodes."""
        if self.state != "RUNNING" or self.tres is None:
            return None
        total = 0
        for name, value in self.tres.items():
            if name == "gres/gpu" or name.startswith("gres/gpu:"):
                if name != "gres/gpu" and "gres/gpu" in self.tres:
                    continue  # Typed entries repeat the untyped total.
                try:
                    total += int(value)
                except ValueError:
                    return None
        return total

    @property
    def ready(self) -> bool | None:
        """Whether every node of a worker allocation runs Ray under the current head.

        A running job is not necessarily a ready Ray worker. ``None`` when the
        state or the Ray membership is unknown, and for heads and successors.
        """
        if self.role != "worker" or self.state is None:
            return None
        if self.state != "RUNNING":
            return False
        if self.ray_nodes is None:
            return None
        return bool(self.nodes) and set(self.nodes) <= set(self.ray_nodes)


@dataclass(frozen=True)
class SessionSnapshot:
    """Non-blocking observation of a session's allocations.

    Taken in two SSH round trips and one scheduler query. It reserves nothing:
    act on it promptly, and let ``reconcile_workers`` revalidate it.
    """

    observed_at: float
    """Login-node epoch seconds when the scheduler was queried."""
    head_job_id: int
    allocations: tuple[AllocationSnapshot, ...]
    scheduler_available: bool

    @property
    def head(self) -> AllocationSnapshot:
        return next(a for a in self.allocations if a.role == "head")

    def workers(self, intent: str | None = None) -> tuple[AllocationSnapshot, ...]:
        """Worker allocations, optionally only those of one intent."""
        return tuple(
            a
            for a in self.allocations
            if a.role == "worker" and (intent is None or a.intent == intent)
        )

    @property
    def certain(self) -> bool:
        """Whether the scheduler answered and every state is known."""
        return self.scheduler_available and all(
            a.state is not None for a in self.allocations
        )


@dataclass(frozen=True)
class StartEstimate:
    """The scheduler's estimate for one candidate worker allocation.

    An estimate is advisory: it reserves nothing, and other jobs can take the
    resources before a submission arrives.
    """

    config: SlurmWorkerAllocationConfig
    observed_at: float
    time_limit_seconds: int | None = None
    """Walltime the candidate would request, including an inherited one."""
    start_in_seconds: float | None = None
    """Seconds until the estimated start, by the login node's clock."""
    start_time: str | None = None
    """Estimated start as the scheduler printed it, in the cluster's timezone."""
    nodes: str | None = None
    partition: str | None = None
    error: str | None = None
    """Why the scheduler gave no estimate, for example an invalid request."""

    def starts_within(self, seconds: float) -> bool:
        return self.start_in_seconds is not None and self.start_in_seconds <= seconds


class StartEstimator(Protocol):
    """Estimates start times for candidate worker allocations.

    Replace the default with a site-specific provider, for example one that
    reads a reservation calendar or applies site knowledge to the answers.
    """

    def estimate(
        self,
        session: SlurmSessionResource,
        candidates: Sequence[SlurmWorkerAllocationConfig],
    ) -> list[StartEstimate]: ...


class SbatchTestOnlyEstimator:
    """Ask ``sbatch --test-only`` about every candidate in one SSH round trip.

    Each candidate is rendered with the options ``add_worker_allocation`` would
    submit, so inherited account, QoS, reservation and memory settings apply.
    Every candidate costs one scheduler RPC: ``max_candidates`` bounds a call,
    and answers are reused for ``cache_seconds``.
    """

    def __init__(self, *, max_candidates: int = 8, cache_seconds: float = 60.0):
        if max_candidates < 1:
            raise ValueError("max_candidates must be at least 1")
        if cache_seconds < 0:
            raise ValueError("cache_seconds must not be negative")
        self.max_candidates = max_candidates
        self.cache_seconds = cache_seconds
        self._cache: dict[tuple[str, ...], tuple[float, dict[str, Any]]] = {}

    def estimate(
        self,
        session: SlurmSessionResource,
        candidates: Sequence[SlurmWorkerAllocationConfig],
    ) -> list[StartEstimate]:
        if len(candidates) > self.max_candidates:
            raise ValueError(
                f"{len(candidates)} candidates exceed the budget of "
                f"{self.max_candidates}; rank them before probing"
            )
        options = [tuple(o) for o in session.worker_sbatch_options(candidates)]
        now = time.monotonic()
        self._cache = {
            key: entry
            for key, entry in self._cache.items()
            if now - entry[0] < self.cache_seconds
        }
        pending = list(dict.fromkeys(o for o in options if o not in self._cache))
        if pending:
            output = session._require_ssh_pool().run(_render_test_only_script(pending))
            answers = _parse_test_only_output(output, len(pending))
            for key, answer in zip(pending, answers):
                self._cache[key] = (now, answer)
        return [
            StartEstimate(
                config=config,
                time_limit_seconds=_requested_seconds(key),
                **self._cache[key][1],
            )
            for key, config in zip(options, candidates)
        ]


@dataclass(frozen=True)
class WorkerIntent:
    """Desired worker allocations under one caller-chosen key.

    Several step processes can express the same intent; ``reconcile_workers``
    serializes them so the same intent is never submitted twice.
    """

    key: str
    count: int
    """Allocations wanted, counting pending and running ones."""
    config: SlurmWorkerAllocationConfig | None = None
    """Shape of new allocations; unset fields inherit as in ``add_worker_allocation``."""
    drain_timeout: int | None = None
    """Seconds a released running allocation may drain before it is cancelled."""
    pending_timeout: float | None = None
    """Cancel an allocation of this intent still pending after this many seconds."""

    def __post_init__(self) -> None:
        _require_intent_key(self.key)
        if self.count < 0:
            raise ValueError("count must not be negative")
        if self.drain_timeout is not None and self.drain_timeout < 1:
            raise ValueError("drain_timeout must be at least one second")
        if self.pending_timeout is not None and self.pending_timeout <= 0:
            raise ValueError("pending_timeout must be positive")


@dataclass(frozen=True)
class ReconcileResult:
    """What one ``reconcile_workers`` call observed and did."""

    observed_at: float
    """Login-node epoch seconds of the revalidating scheduler query."""
    live: tuple[int, ...]
    """Allocations that count toward the intent after this call."""
    submitted: tuple[int, ...] = ()
    released: tuple[int, ...] = ()
    """Allocations released by this call: drained when running, else cancelled."""
    cancelled: tuple[int, ...] = ()
    """Allocations cancelled by this call, including drains past their deadline."""
    recovered: tuple[int, ...] = ()
    """Submissions of an interrupted earlier call, found by their token."""
    held_back: str | None = None
    """Why the call did not submit, although the intent wanted more allocations."""


def _require_intent_key(key: str) -> None:
    if not _INTENT_KEY_RE.fullmatch(key):
        raise ValueError("Intent keys use 1-64 letters, digits, '.', '_' or '-'")


def parse_tres(value: str) -> dict[str, str] | None:
    """Parse ``cpu=8,mem=64G,gres/gpu=4`` into a mapping."""
    pairs = [item.split("=", 1) for item in value.strip().split(",") if item]
    if not pairs or any(len(pair) != 2 for pair in pairs):
        return None
    return {name: amount for name, amount in pairs}


def _parse_test_only_output(output: str, count: int) -> list[dict[str, Any]]:
    """Answers to the script ``SbatchTestOnlyEstimator`` runs, as estimate fields."""
    observed_at = time.time()
    login_now: datetime | None = None
    sections: dict[int, list[str]] = {}
    current: list[str] | None = None
    for line in output.replace("\r", "").splitlines():
        if line.startswith("@@clock "):
            parts = line.split()
            try:
                observed_at = float(parts[1])
                login_now = datetime.fromisoformat(parts[2])
            except (IndexError, ValueError):
                login_now = None
            current = None
        elif line.startswith("@@probe "):
            current = sections.setdefault(int(line.split()[1]), [])
        elif current is not None and line.strip():
            current.append(line.strip())
    answers: list[dict[str, Any]] = []
    for index in range(count):
        lines = sections.get(index) or []
        text = " ".join(lines)
        match = _START_RE.search(text)
        if match is None:
            answers.append(
                {
                    "observed_at": observed_at,
                    "error": lines[-1] if lines else "no answer from the scheduler",
                }
            )
            continue
        start_in: float | None = None
        if login_now is not None:
            try:
                start_in = max(
                    0.0,
                    (
                        datetime.fromisoformat(match["start"]) - login_now
                    ).total_seconds(),
                )
            except ValueError:
                start_in = None
        nodes = _NODES_RE.search(text)
        partition = _PARTITION_RE.search(text)
        answers.append(
            {
                "observed_at": observed_at,
                "start_in_seconds": start_in,
                "start_time": match["start"],
                "nodes": nodes["nodes"] if nodes else None,
                "partition": partition["partition"] if partition else None,
            }
        )
    return answers


def _requested_seconds(options: Sequence[str]) -> int | None:
    from .resources.slurm import _slurm_time_limit_seconds

    for option in options:
        if option.startswith("--time="):
            try:
                return _slurm_time_limit_seconds(option.split("=", 1)[1])
            except ValueError:
                return None
    return None


def _render_test_only_script(candidates: Sequence[Sequence[str]]) -> str:
    lines = ['printf \'@@clock %s %s\\n\' "$(date +%s)" "$(date +%Y-%m-%dT%H:%M:%S)"']
    for index, options in enumerate(candidates):
        args = " ".join(shlex.quote(option) for option in options)
        lines.append(f"printf '@@probe {index}\\n'")
        lines.append(f"sbatch --test-only {args} --wrap=true 2>&1 || true")
    return "\n".join(lines)
