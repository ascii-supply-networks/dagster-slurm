"""Observation and coordination hooks for application-driven worker scaling."""

import json
import re
import shlex
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar, cast
from unittest.mock import MagicMock

import pytest
from dagster_slurm import (
    AllocationSnapshot,
    SbatchTestOnlyEstimator,
    SessionSnapshot,
    SlurmSessionResource,
    SlurmWorkerAllocationConfig,
    StartEstimate,
    WorkerIntent,
)
from dagster_slurm.helpers.ssh_helpers import TERMINAL_STATES
from dagster_slurm.helpers.ssh_pool import SSHConnectionPool
from dagster_slurm.resources.session import _parse_session_observation
from dagster_slurm.scaling import ENDED, _parse_test_only_output, parse_tres

from dagster_slurm_test.test_resources import _mock_slurm_resource
from dagster_slurm_test.test_session_relay import RelayPool, make_session


class ScalingPool(RelayPool):
    """Answers the session's scheduler queries from ``states``.

    The observation script runs locally against the fake remote filesystem,
    with ``squeue`` and ``sacct`` replaced by shell functions.
    """

    def __init__(self):
        super().__init__()
        self.comments: dict[int, str] = {}
        self.time_left: dict[int, str] = {}
        self.reasons: dict[int, str] = {}
        self.tres: dict[int, str] = {}
        self.scheduler_down = False
        self.lose_next_submission = False
        self.stuck_drains = False
        self.failing_drains = 0
        self.observations = 0

    def run(self, cmd: str, timeout: int | None = None) -> str:
        if cmd.startswith("sbatch "):
            output = super().run(cmd, timeout)
            match = re.search(r"--comment=(\S+)", cmd)
            if match:
                self.comments[self.submitted_jobs[-1]] = match[1]
            if self.lose_next_submission:
                self.lose_next_submission = False
                raise RuntimeError("connection lost after sbatch")
            return output
        if cmd.startswith("scancel --batch"):
            self.commands.append(cmd)
            if self.failing_drains:
                self.failing_drains -= 1
                raise RuntimeError("scancel timed out")
            if self.stuck_drains:
                return ""
        if "@@clock" in cmd:
            self.observations += 1
            rows = "\n".join(
                f"{job}|{state}|{self.reasons.get(job, 'None')}|"
                f"{self.time_left.get(job, '9:00:00')}|{self.tres.get(job, 'cpu=2')}|"
                f"{self.comments.get(job, '(null)')}|"
                for job, state in self.states.items()
                if state not in TERMINAL_STATES
            )
            squeue = (
                "squeue() { return 1; }"
                if self.scheduler_down
                else f"squeue() {{ printf '%s\\n' {shlex.quote(rows)}; }}"
            )
            return super().run(f"{squeue}\nsacct() {{ true; }}\n{cmd}", timeout)
        return super().run(cmd, timeout)


def _metadata(session: SlurmSessionResource) -> dict:
    path = Path(session.allocation.session_dir) / "allocation.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _write_metadata(session: SlurmSessionResource, metadata: dict) -> None:
    path = Path(session.allocation.session_dir) / "allocation.json"
    path.write_text(json.dumps(metadata), encoding="utf-8")


def _attach(tmp_path: Path, pool: ScalingPool) -> SlurmSessionResource:
    """A second step process attached to the same run allocation."""
    session = SlurmSessionResource(
        slurm=_mock_slurm_resource(remote_base=str(tmp_path)),
        num_nodes=1,
        signal_before_timeout="TERM@120",
        time_limit="00:10:00",
        enable_health_checks=False,
    )
    object.__setattr__(session, "_ssh_pool", cast(SSHConnectionPool, pool))
    context = SimpleNamespace(run=SimpleNamespace(run_id="relay", tags={}))
    object.__setattr__(session, "_allocation", session._create_allocation(context))
    return session


def test_parse_session_observation_reads_jobs_files_and_unknowns():
    output = (
        "@@clock 1000\r\n"
        "@@squeue\r\n"
        "7|RUNNING|None|1:00:00|cpu=8,mem=64G,node=2,gres/gpu=6|(null)|\r\n"
        "8|PENDING|Resources|2:00:00|cpu=4,node=1|dagster_slurm_submission:abc|\r\n"
        "@@sacct\r\n"
        "9|CANCELLED by 0\r\n"
        "@@files 7\r\n"
        "@drain\r\n"
        "@node n1\r\n"
        "@node n2\r\n"
        "@ray n1 aaaa 3\r\n"
        "@ray n2 bbbb"
    )
    clock, rows, files, accounted = _parse_session_observation(output)
    assert clock == 1000
    assert rows is not None
    assert rows[7]["tres"]["gres/gpu"] == "6"
    assert rows[7]["reason"] is None
    assert rows[8]["reason"] == "Resources"
    assert rows[8]["comment"] == "dagster_slurm_submission:abc"
    assert accounted == {9: "CANCELLED"}
    assert files[7] == {
        "draining": True,
        "nodes": ["n1", "n2"],
        "ray": {"n1": ("aaaa", 3), "n2": ("bbbb", None)},
    }

    _, rows, _, _ = _parse_session_observation("@@clock 1\n@@squeue-failed\n")
    assert rows is None


def test_allocation_snapshot_separates_running_from_ready():
    worker = AllocationSnapshot(
        slurm_job_id=1,
        role="worker",
        state="RUNNING",
        tres=parse_tres("cpu=8,gres/gpu=4,gres/gpu:a40=4"),
        nodes=("n1", "n2"),
        ray_nodes={"n1": "aaaa"},
    )
    assert worker.granted_gpus == 4
    assert worker.committed is True
    assert worker.ready is False  # n2 has not joined the current head yet.
    assert AllocationSnapshot(1, "worker", None).ready is None
    assert AllocationSnapshot(1, "worker", None).committed is None
    assert AllocationSnapshot(1, "worker", "PENDING").granted_gpus is None
    assert AllocationSnapshot(1, "worker", ENDED).ended
    assert parse_tres("garbage") is None


def test_inspect_reports_scheduler_facts_and_ray_membership(tmp_path):
    pool = ScalingPool()
    session = make_session(tmp_path, pool)
    worker = session.add_worker_allocation(
        SlurmWorkerAllocationConfig(gpus_per_node=4, time_limit="01:00:00")
    )
    pending = session.add_worker_allocation()
    pool.states[worker.slurm_job_id] = "RUNNING"
    pool.time_left[worker.slurm_job_id] = "42:00"
    pool.tres[worker.slurm_job_id] = "cpu=8,node=2,gres/gpu=8"
    pool.reasons[pending.slurm_job_id] = "Priority"
    nodes = Path(worker.working_dir)
    (nodes / "ray_nodes").mkdir(parents=True)
    (nodes / "ray_nodes" / "node-a.id").write_text("aaaa 700\n")
    (nodes / "ray_nodes" / "node-b.id").write_text("bbbb 1\n")  # An old head.
    (Path(session.allocation.working_dir) / "drain_signal").touch()

    snapshot = session.inspect()

    assert snapshot.scheduler_available and snapshot.certain
    assert snapshot.head_job_id == 700
    assert snapshot.head.draining is True
    running, queued = snapshot.workers()
    assert running.state == "RUNNING"
    assert running.time_left_seconds == 42 * 60
    assert running.time_limit_seconds == 3600
    assert running.requested_gpus_per_node == 4
    assert running.granted_gpus == 8
    assert running.nodes == ("node-a", "node-b")
    assert running.ray_nodes == {"node-a": "aaaa"}
    assert running.ready is False
    assert running.draining is False
    assert queued.state == "PENDING"
    assert queued.reason == "Priority"
    assert queued.time_left_seconds is None
    assert abs(snapshot.observed_at - time.time()) < 60


def test_inspect_marks_values_unknown_when_the_scheduler_is_down(tmp_path):
    pool = ScalingPool()
    session = make_session(tmp_path, pool)
    session.add_worker_allocation()
    pool.scheduler_down = True

    snapshot = session.inspect()

    assert not snapshot.scheduler_available and not snapshot.certain
    assert all(a.state is None for a in snapshot.allocations)
    assert all(a.committed is None for a in snapshot.allocations)


def test_reconcile_counts_pending_workers_and_never_submits_twice(tmp_path):
    pool = ScalingPool()
    session = make_session(tmp_path, pool)
    other = _attach(tmp_path, pool)
    intent = WorkerIntent(key="gpu-pool", count=2)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(lambda s: s.reconcile_workers(intent), [session, other])
        )

    submitted = [job for result in results for job in result.submitted]
    assert len(submitted) == 2
    assert all(pool.states[job] == "PENDING" for job in submitted)
    again = session.reconcile_workers(intent)
    assert again.submitted == ()
    assert sorted(again.live) == sorted(submitted)
    workers = _metadata(session)["workers"]
    assert [w["intent"] for w in workers] == ["gpu-pool", "gpu-pool"]
    assert {pool.comments[job].split(":")[0] for job in submitted} == {
        "dagster_slurm_submission"
    }
    # Allocations added directly do not count toward an intent.
    session.add_worker_allocation()
    assert session.reconcile_workers(intent).submitted == ()


def test_reconcile_recovers_a_submission_whose_job_id_was_lost(tmp_path):
    pool = ScalingPool()
    session = make_session(tmp_path, pool)
    intent = WorkerIntent(key="helpers", count=1)
    pool.lose_next_submission = True

    with pytest.raises(RuntimeError, match="connection lost"):
        session.reconcile_workers(intent)
    lost = pool.submitted_jobs[-1]
    assert _metadata(session)["worker_submissions"][0]["intent"] == "helpers"

    result = session.reconcile_workers(intent)

    assert result.recovered == (lost,)
    assert result.submitted == ()
    assert result.live == (lost,)
    metadata = _metadata(session)
    assert metadata["worker_submissions"] == []
    assert [w["slurm_job_id"] for w in metadata["workers"]] == [lost]
    assert session.worker_allocations[0].slurm_job_id == lost


def test_a_submission_that_never_reached_the_scheduler_is_dropped(tmp_path):
    pool = ScalingPool()
    session = make_session(tmp_path, pool)
    metadata = _metadata(session)
    metadata["worker_submissions"] = [
        {
            "token": "never-submitted",
            "intent": "helpers",
            "submitted_at": 0,
            "config": {},
            "ray_args": [],
            "rejoin_timeout": 600,
        }
    ]
    _write_metadata(session, metadata)

    result = session.reconcile_workers(WorkerIntent(key="helpers", count=1))

    assert result.recovered == ()
    assert len(result.submitted) == 1
    assert _metadata(session)["worker_submissions"] == []


def test_unknown_scheduler_state_holds_back_scale_out_but_not_cleanup(tmp_path):
    pool = ScalingPool()
    pool.stuck_drains = True
    session = make_session(tmp_path, pool)
    (job,) = session.reconcile_workers(WorkerIntent(key="w", count=1)).submitted
    pool.states[job] = "RUNNING"
    released = session.reconcile_workers(WorkerIntent(key="w", count=0))
    assert released.released == (job,)
    metadata = _metadata(session)
    metadata["workers"][0]["drain_deadline"] = time.time() - 1
    _write_metadata(session, metadata)
    pool.scheduler_down = True

    result = session.reconcile_workers(WorkerIntent(key="other", count=1))
    assert result.held_back == "the scheduler state is unknown"
    assert result.submitted == ()

    result = session.reconcile_workers(WorkerIntent(key="w", count=0))
    assert result.cancelled == (job,)
    assert f"scancel {job}" in pool.commands


def test_released_workers_keep_their_persisted_drain_deadline(tmp_path):
    pool = ScalingPool()
    pool.stuck_drains = True
    session = make_session(tmp_path, pool)
    first = session.reconcile_workers(WorkerIntent(key="w", count=2))
    early, late = first.submitted
    pool.states.update({early: "RUNNING", late: "RUNNING"})
    pool.time_left.update({early: "20:00", late: "3:00:00"})
    pool.failing_drains = 1

    shrink = WorkerIntent(key="w", count=1, drain_timeout=300)
    result = session.reconcile_workers(shrink)

    # The allocation with the least time left goes; its drain failed once.
    assert result.released == (early,)
    assert result.live == (late,)
    record = next(
        w for w in _metadata(session)["workers"] if w["slurm_job_id"] == early
    )
    deadline = record["drain_deadline"]
    assert deadline >= result.observed_at + 300
    assert not record.get("drain_requested")

    retry = session.reconcile_workers(shrink)
    assert retry.released == ()
    record = next(
        w for w in _metadata(session)["workers"] if w["slurm_job_id"] == early
    )
    assert record["drain_deadline"] == deadline
    assert record["drain_requested"] is True
    assert (
        Path(session.allocation.session_dir) / f"jobs/{early}/drain_timeout"
    ).read_text() == "300\n"
    drains = [c for c in pool.commands if c == f"scancel --batch --signal=TERM {early}"]
    assert len(drains) == 2  # The failed attempt and the retry.

    session.reconcile_workers(shrink)
    assert (
        len([c for c in pool.commands if c == f"scancel --batch --signal=TERM {early}"])
        == 2
    )

    metadata = _metadata(session)
    for worker in metadata["workers"]:
        if worker["slurm_job_id"] == early:
            worker["drain_deadline"] = time.time() - 1
    _write_metadata(session, metadata)
    assert session.reconcile_workers(shrink).cancelled == (early,)
    pool.states[early] = "CANCELLED"
    assert [w["slurm_job_id"] for w in _metadata(session)["workers"]] == [early, late]
    final = session.reconcile_workers(shrink)
    assert final.live == (late,)
    assert [w["slurm_job_id"] for w in _metadata(session)["workers"]] == [late]


def test_scale_in_cancels_queued_allocations_first(tmp_path):
    pool = ScalingPool()
    session = make_session(tmp_path, pool)
    jobs = session.reconcile_workers(WorkerIntent(key="w", count=2)).submitted
    pool.states[jobs[0]] = "RUNNING"

    result = session.reconcile_workers(WorkerIntent(key="w", count=1))

    assert result.released == (jobs[1],)
    assert result.cancelled == (jobs[1],)
    assert f"scancel {jobs[1]}" in pool.commands
    assert f"scancel --batch --signal=TERM {jobs[0]}" not in pool.commands


def test_pending_timeout_cancels_without_resubmitting_in_the_same_call(tmp_path):
    pool = ScalingPool()
    session = make_session(tmp_path, pool)
    intent = WorkerIntent(key="w", count=1, pending_timeout=60)
    (job,) = session.reconcile_workers(intent).submitted
    metadata = _metadata(session)
    metadata["workers"][0]["submitted_at"] = time.time() - 600
    _write_metadata(session, metadata)

    result = session.reconcile_workers(intent)

    assert result.cancelled == (job,)
    assert result.submitted == ()
    assert result.held_back == "pending allocations expired"
    pool.states[job] = "CANCELLED"
    assert len(session.reconcile_workers(intent).submitted) == 1


def test_stale_or_foreign_observations_hold_back_scale_out(tmp_path):
    pool = ScalingPool()
    session = make_session(tmp_path, pool)
    snapshot = session.inspect()
    intent = WorkerIntent(key="w", count=1)

    stale = SessionSnapshot(
        observed_at=snapshot.observed_at - 1000,
        head_job_id=snapshot.head_job_id,
        allocations=snapshot.allocations,
        scheduler_available=True,
    )
    assert session.reconcile_workers(intent, observed=stale).held_back == (
        "the observation is stale"
    )
    foreign = SessionSnapshot(
        observed_at=snapshot.observed_at,
        head_job_id=1,
        allocations=snapshot.allocations,
        scheduler_available=True,
    )
    assert session.reconcile_workers(intent, observed=foreign).held_back == (
        "the head changed since the observation"
    )
    result = session.reconcile_workers(intent, observed=snapshot)
    assert result.held_back is None
    assert len(result.submitted) == 1


def test_intent_survives_promotion_to_head(tmp_path):
    pool = ScalingPool()
    session = make_session(tmp_path, pool)
    (job,) = session.reconcile_workers(WorkerIntent(key="w", count=1)).submitted
    worker = session.worker_allocations[0]
    record = session._allocation_record(worker)
    assert record["intent"] == "w"
    assert session._allocation_from_record(record, worker.session_dir).intent == "w"
    assert worker.slurm_job_id == job


def test_reconcile_releases_the_named_allocations_whatever_their_order(tmp_path):
    pool = ScalingPool()
    pool.stuck_drains = True
    session = make_session(tmp_path, pool)
    jobs = session.reconcile_workers(WorkerIntent(key="w", count=3)).submitted
    named, short, queued = jobs
    pool.states.update({named: "RUNNING", short: "RUNNING"})
    pool.time_left.update({named: "5:00:00", short: "10:00"})
    shrink = WorkerIntent(key="w", count=2, drain_timeout=300)

    # Count alone would cancel the queued allocation; the named one goes instead.
    result = session.reconcile_workers(shrink, release=[named, 999_999])

    assert result.released == (named,)
    assert result.live == (short, queued)
    assert result.submitted == ()
    assert f"scancel --batch --signal=TERM {named}" in pool.commands
    assert f"scancel {queued}" not in pool.commands
    deadline = next(
        w["drain_deadline"]
        for w in _metadata(session)["workers"]
        if w["slurm_job_id"] == named
    )
    assert deadline >= result.observed_at + 300

    again = session.reconcile_workers(shrink, release=[named])
    assert again.released == ()
    assert again.live == (short, queued)
    assert [
        w["drain_deadline"]
        for w in _metadata(session)["workers"]
        if w["slurm_job_id"] == named
    ] == [deadline]


def test_a_named_release_is_replaced_while_the_count_holds(tmp_path):
    pool = ScalingPool()
    session = make_session(tmp_path, pool)
    config = SlurmWorkerAllocationConfig(gpus_per_node=2, time_limit="00:30:00")
    (job,) = session.reconcile_workers(
        WorkerIntent(key="w", count=1, config=config)
    ).submitted
    pool.states[job] = "RUNNING"

    result = session.reconcile_workers(
        WorkerIntent(key="w", count=1, config=config), release=[job]
    )

    assert result.released == (job,)
    (replacement,) = result.submitted
    assert result.live == (replacement,)


def test_max_submissions_bounds_what_one_call_submits(tmp_path):
    pool = ScalingPool()
    session = make_session(tmp_path, pool)
    intent = WorkerIntent(key="w", count=3)

    first = session.reconcile_workers(intent, max_submissions=1)
    assert len(first.submitted) == 1
    assert first.held_back == "max_submissions was reached"

    # A policy that adds nothing keeps the intent's count at what it observed.
    # An allocation that ends after that observation is not replaced.
    snapshot = session.inspect()
    (job,) = first.submitted
    pool.states[job] = "CANCELLED"
    keep = WorkerIntent(key="w", count=len(snapshot.workers("w")))
    result = session.reconcile_workers(keep, observed=snapshot, max_submissions=0)
    assert result.submitted == ()
    assert result.live == ()
    assert result.held_back == "max_submissions was reached"

    with pytest.raises(ValueError, match="max_submissions"):
        session.reconcile_workers(intent, max_submissions=-1)


def test_inspect_reports_when_reconcile_submitted_a_worker(tmp_path):
    pool = ScalingPool()
    session = make_session(tmp_path, pool)
    result = session.reconcile_workers(WorkerIntent(key="w", count=1))
    session.add_worker_allocation()

    by_intent, direct = session.inspect().workers()

    assert by_intent.intent == "w"
    assert by_intent.submitted_at == result.observed_at
    assert direct.intent is None
    assert direct.submitted_at is None
    assert session.inspect().head.submitted_at is None


def test_assign_worker_intent_moves_a_worker_into_and_out_of_an_intent(tmp_path):
    pool = ScalingPool()
    pool.stuck_drains = True
    session = make_session(tmp_path, pool)
    direct = session.add_worker_allocation()
    (kept,) = session.reconcile_workers(WorkerIntent(key="w", count=1)).submitted
    pool.states.update({direct.slurm_job_id: "RUNNING", kept: "RUNNING"})

    session.assign_worker_intent(direct.slurm_job_id, "w")
    session.assign_worker_intent(kept, None)
    session.assign_worker_intent(kept, None)  # Repeating a move changes nothing.

    assert {w.slurm_job_id: w.intent for w in session.inspect().workers()} == {
        direct.slurm_job_id: "w",
        kept: None,
    }
    result = session.reconcile_workers(WorkerIntent(key="w", count=0))
    assert result.released == (direct.slurm_job_id,)
    assert f"scancel --batch --signal=TERM {kept}" not in pool.commands
    # The adopted allocation has no submission time, so it never expires.
    assert session.inspect().workers("w")[0].submitted_at is None

    with pytest.raises(ValueError, match="is being released"):
        session.assign_worker_intent(direct.slurm_job_id, None)
    with pytest.raises(ValueError, match="not a worker allocation"):
        session.assign_worker_intent(session.allocation.slurm_job_id, "w")
    with pytest.raises(ValueError, match="Intent keys"):
        session.assign_worker_intent(kept, "has space")


def test_a_released_worker_never_leads_the_run(tmp_path):
    pool = ScalingPool()
    session = make_session(tmp_path, pool)
    released, kept = session.reconcile_workers(WorkerIntent(key="w", count=2)).submitted
    pool.states.update({released: "RUNNING", kept: "RUNNING"})
    pool.failing_drains = 1  # No drain marker: only the deadline says it leaves.

    session.reconcile_workers(WorkerIntent(key="w", count=1), release=[released])

    metadata = _metadata(session)
    leader = session._pick_worker_leader(metadata, session.allocation.session_dir)
    assert leader is not None
    assert leader["slurm_job_id"] == kept


@pytest.mark.parametrize(
    ("bad", "message"),
    [
        ({"key": "has space", "count": 1}, "Intent keys"),
        ({"key": "w", "count": -1}, "count"),
        ({"key": "w", "count": 1, "drain_timeout": 0}, "drain_timeout"),
        ({"key": "w", "count": 1, "pending_timeout": 0}, "pending_timeout"),
    ],
)
def test_invalid_intents_are_rejected(bad, message):
    with pytest.raises(ValueError, match=message):
        WorkerIntent(**bad)


def test_parse_test_only_output_uses_the_login_node_clock():
    output = (
        "@@clock 5000 2026-10-30T10:00:00\n"
        "@@probe 0\n"
        "sbatch: Job 3 to start at 2026-10-30T10:05:00 using 8 processors "
        "on nodes c[1-2] in partition gpu\n"
        "@@probe 1\n"
        "sbatch: error: invalid partition specified: nope\n"
        "allocation failure: Invalid partition name specified"
    )
    starts_soon, invalid, missing = _parse_test_only_output(output, 3)
    assert starts_soon == {
        "observed_at": 5000.0,
        "start_in_seconds": 300.0,
        "start_time": "2026-10-30T10:05:00",
        "nodes": "c[1-2]",
        "partition": "gpu",
    }
    assert invalid["error"] == "allocation failure: Invalid partition name specified"
    assert missing["error"] == "no answer from the scheduler"


def test_parse_test_only_output_reads_the_slurm_26_05_format():
    # Slurm 26.05 prints an extra token after the start time.
    output = (
        "@@clock 5000 2026-10-30T10:00:00\n"
        "@@probe 0\n"
        "sbatch: Job 5 to start at 2026-10-30T10:01:00 a using 2 processors "
        "on nodes c[1-2] in partition normal"
    )
    (estimate,) = _parse_test_only_output(output, 1)
    assert estimate == {
        "observed_at": 5000.0,
        "start_in_seconds": 60.0,
        "start_time": "2026-10-30T10:01:00",
        "nodes": "c[1-2]",
        "partition": "normal",
    }


def test_estimate_worker_starts_probes_inherited_shapes_within_budget(tmp_path):
    class ProbePool(ScalingPool):
        probes: ClassVar[list[str]] = []

        def run(self, cmd: str, timeout: int | None = None) -> str:
            if "@@probe" in cmd:
                self.probes.append(cmd)
                lines = ["@@clock 100 2026-10-30T10:00:00"]
                for index, probe in enumerate(re.findall(r"sbatch --test-only.*", cmd)):
                    lines.append(f"@@probe {index}")
                    if "--partition=big" in probe:
                        lines.append(
                            "allocation failure: Requested node configuration is not available"
                        )
                    else:
                        lines.append(
                            "sbatch: Job 9 to start at 2026-10-30T10:00:30 using 2 "
                            "processors on nodes c1 in partition test"
                        )
                return "\n".join(lines)
            return super().run(cmd, timeout)

    pool = ProbePool()
    session = make_session(tmp_path, pool)
    small = SlurmWorkerAllocationConfig(time_limit="00:30:00")
    big = SlurmWorkerAllocationConfig(partition="big", num_nodes=8)

    now, never = session.estimate_worker_starts([small, big])

    assert isinstance(now, StartEstimate)
    assert now.config is small and now.starts_within(60)
    assert now.partition == "test" and now.nodes == "c1"
    assert now.time_limit_seconds == 30 * 60
    assert never.time_limit_seconds == 10 * 60  # Inherited from the head.
    assert never.config is big and not never.starts_within(10**9)
    assert "Requested node configuration" in (never.error or "")
    (probe,) = pool.probes
    assert "--job-name=dagster_relay" in probe
    assert "--time=00:30:00" in probe
    assert "--cpus-per-task=2" in probe  # Inherited from the queue.
    assert "--wrap=true" in probe
    # Answers are cached, and the budget is strict.
    session.estimate_worker_starts([small])
    assert len(pool.probes) == 1
    with pytest.raises(ValueError, match="budget"):
        session.estimate_worker_starts([small] * 9)
    tight = SbatchTestOnlyEstimator(max_candidates=1, cache_seconds=0)
    session.estimate_worker_starts([small], estimator=tight)
    assert len(pool.probes) == 2


def _active(snapshot: SessionSnapshot, key: str) -> list[AllocationSnapshot]:
    return [
        w for w in snapshot.workers(key) if w.committed and w.drain_deadline is None
    ]


def batch_intent(
    remaining_tasks: int,
    snapshot: SessionSnapshot,
    estimates: list[StartEstimate],
    tasks_per_allocation: int = 4,
) -> WorkerIntent:
    """Checkpointed tasks with cheap startup, as in the scaling how-to."""
    live = _active(snapshot, "batch")
    wanted = -(-remaining_tasks // tasks_per_allocation)  # ceiling division
    starts_now = [e for e in estimates if e.starts_within(120)]
    # Grow onto a shape that starts now; shrink as soon as work runs out.
    return WorkerIntent(
        key="batch",
        count=min(wanted, len(live) + (1 if starts_now else 0)),
        config=starts_now[0].config if starts_now else None,
        pending_timeout=120,
    )


def model_intent(
    startup_seconds: float,
    work_left: bool,
    snapshot: SessionSnapshot,
    estimates: list[StartEstimate],
) -> WorkerIntent:
    """Work behind an expensive model initialization, as in the scaling how-to."""
    if not work_left:
        return WorkerIntent(key="model", count=0, drain_timeout=900)
    head_left = snapshot.head.time_left_seconds
    worth_it = [
        e
        for e in estimates
        if e.starts_within(300)
        and e.time_limit_seconds is not None
        and e.time_limit_seconds >= 4 * startup_seconds
        # Workers should not outlive the head they join.
        and (head_left is None or e.time_limit_seconds <= head_left - 600)
    ]
    return WorkerIntent(
        key="model",
        count=len(_active(snapshot, "model")) + (1 if worth_it else 0),
        config=worth_it[0].config if worth_it else None,
        drain_timeout=900,
    )


def test_contrasting_policies_share_the_same_hooks():
    """Two workloads decide differently on the same observations and estimates."""
    snapshot = SessionSnapshot(
        observed_at=0,
        head_job_id=1,
        allocations=(
            AllocationSnapshot(1, "head", "RUNNING", time_left_seconds=3 * 3600),
        ),
        scheduler_available=True,
    )
    short_config = SlurmWorkerAllocationConfig(time_limit="00:20:00")
    long_config = SlurmWorkerAllocationConfig(time_limit="02:00:00")
    short = StartEstimate(
        config=short_config, observed_at=0, time_limit_seconds=1200, start_in_seconds=0
    )
    long = StartEstimate(
        config=long_config, observed_at=0, time_limit_seconds=7200, start_in_seconds=0
    )

    assert batch_intent(10, snapshot, [short]).count == 1
    assert model_intent(900, True, snapshot, [short]).count == 0
    grown = model_intent(900, True, snapshot, [short, long])
    assert grown.count == 1 and grown.config is long_config
    assert batch_intent(0, snapshot, [short]).count == 0
    assert model_intent(900, False, snapshot, [long]).count == 0


@pytest.mark.needs_slurm_docker
def test_scaling_hooks_against_slurm(slurm_resource_for_testing, slurm_cluster_ready):
    """Snapshot, start probes and reconciliation on the Docker cluster's scheduler."""
    session = SlurmSessionResource(
        slurm=slurm_resource_for_testing,
        num_nodes=1,
        partition="normal",
        time_limit="00:10:00",
        cpus_per_task=1,
        mem="500M",
        gpus_per_node=0,
        enable_health_checks=False,
    )
    context = SimpleNamespace(
        run=SimpleNamespace(run_id=f"scaling_{uuid.uuid4().hex}", tags={}),
        instance=MagicMock(),
    )
    session.setup_for_execution(cast(Any, context))
    try:
        snapshot = session.inspect()
        assert snapshot.certain
        assert snapshot.head.state == "RUNNING"
        assert snapshot.head.time_left_seconds is not None
        assert snapshot.head.tres is not None and "cpu" in snapshot.head.tres

        fits = SlurmWorkerAllocationConfig(cpus_per_task=1, mem="500M")
        too_big = SlurmWorkerAllocationConfig(num_nodes=50, cpus_per_task=1)
        fits_estimate, too_big_estimate = session.estimate_worker_starts(
            [fits, too_big]
        )
        assert fits_estimate.start_time is not None, fits_estimate.error
        assert fits_estimate.start_in_seconds is not None
        assert too_big_estimate.error is not None

        intent = WorkerIntent(key="probe-check", count=1, config=fits)
        first = session.reconcile_workers(intent, observed=snapshot)
        assert len(first.submitted) == 1, first
        assert session.reconcile_workers(intent).submitted == ()
        (worker,) = session.inspect().workers("probe-check")
        assert worker.committed is True

        shrink = WorkerIntent(key="probe-check", count=0, drain_timeout=5)
        assert session.reconcile_workers(shrink).released == first.submitted
        deadline = time.monotonic() + 120
        while session.reconcile_workers(shrink).live or session.inspect().workers(
            "probe-check"
        ):
            assert time.monotonic() < deadline, "worker allocation did not leave"
            time.sleep(3)
    finally:
        session.teardown_after_execution(cast(Any, context))
