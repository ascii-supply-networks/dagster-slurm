"""Relay lifecycle, result finalization and startup race regressions."""

import os
import shlex
import shutil
import signal
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import dagster as dg
import pytest

from dagster_slurm import (
    BashLauncher,
    SlurmRunAllocationConfig,
    SlurmSessionResource,
    SlurmStepDrained,
    SlurmStepExecutionResult,
    SlurmAllocationEnded,
)
from dagster_slurm.helpers.message_readers import SSHMessageReader
from dagster_slurm.helpers.signals import build_pre_timeout_supervisor_script
from dagster_slurm.helpers.ssh_pool import SSHConnectionPool
from dagster_slurm.resources.session import SlurmAllocation
import dagster_slurm.resources.session as session_module
from dagster_slurm.pipes_clients.slurm_pipes_client import SlurmPipesClient
from dagster_slurm_test.test_env_caching import FakePool, configure_client_for_local_run
from dagster_slurm_test.test_resources import _mock_slurm_resource
from dagster_slurm_test.test_session_relay import RelayPool, make_session


@pytest.mark.parametrize("reattach", [False, True])
@pytest.mark.parametrize("exit_code", [0, 7])
def test_drained_pipes_preserves_final_results_and_logs(
    tmp_path, monkeypatch, capsys, reattach, exit_code
):
    finalized = []

    class FinalMessagesReader(SSHMessageReader):
        @contextmanager
        def read_messages(self, handler):
            handler.handle_message({"method": "opened", "params": {}})
            yield {"path": self.remote_path}
            # Deliver messages only on normal context exit, after wait_for_step.
            handler.handle_message(
                {
                    "method": "report_custom_message",
                    "params": {"payload": {"remaining": 0}},
                }
            )
            handler.handle_message(
                {
                    "method": "report_asset_materialization",
                    "params": {
                        "asset_key": "finished",
                        "metadata": {"rows": {"raw_value": 217, "type": "int"}},
                        "data_version": None,
                    },
                }
            )
            handler.handle_message({"method": "closed", "params": {}})
            finalized.append(True)

    pool = FakePool()
    session = SlurmSessionResource(slurm=_mock_slurm_resource())
    object.__setattr__(session, "_ssh_pool", pool)
    object.__setattr__(session, "_initialized", True)
    allocation = SlurmAllocation(42, ["node-a"], "/allocation", session)
    object.__setattr__(session, "_allocation", allocation)
    client = SlurmPipesClient(
        slurm_resource=session.slurm, launcher=BashLauncher(), session_resource=session
    )
    configure_client_for_local_run(client, monkeypatch, pool)
    monkeypatch.setattr(
        "dagster_slurm.pipes_clients.slurm_pipes_client.open_pipes_session",
        dg.open_pipes_session,
    )
    monkeypatch.setattr(
        "dagster_slurm.pipes_clients.slurm_pipes_client.SSHMessageReader",
        FinalMessagesReader,
    )
    monkeypatch.setattr(
        client,
        "_read_remote_files",
        lambda *a: {"out": "checkpoint flushed\n", "err": "final diagnostic\n"},
    )
    result = SlurmStepExecutionResult(
        job_id=42,
        stdout_path="out",
        stderr_path="err",
        drain_signal="TERM",
        exit_code=exit_code,
    )
    monkeypatch.setattr(client, "_execute_in_session", lambda **kwargs: result)
    monkeypatch.setattr(allocation, "wait_for_step", lambda *a, **kw: result)
    monkeypatch.setattr(
        client,
        "_find_reattachable_job",
        lambda *a: (
            {
                "job_id": "42",
                "run_dir": "/old/invocation",
                "dagster_slurm/session_step_id_path": "/step/id",
                "dagster_slurm/session_step_status_path": "/step/status",
                "dagster_slurm/session_step_stdout_path": "out",
                "dagster_slurm/session_step_stderr_path": "err",
            }
            if reattach
            else None
        ),
    )
    payload = tmp_path / "payload.py"
    payload.write_text("pass")

    @dg.asset
    def finished(context: dg.AssetExecutionContext):
        with pytest.raises(SlurmStepDrained) as caught:
            client.run(
                context=cast(Any, context), payload_path=str(payload), use_session=True
            )
        assert finalized == [True]
        assert caught.value.result.exit_code == exit_code
        assert list(caught.value.invocation.get_custom_messages()) == [{"remaining": 0}]
        results = caught.value.invocation.get_results(implicit_materializations=False)
        assert len(results) == 1
        assert isinstance(results[0], dg.MaterializeResult)
        assert results[0].asset_key == dg.AssetKey("finished")
        assert results[0].metadata is not None
        assert results[0].metadata["rows"] == dg.IntMetadataValue(217)
        return results[0]

    assert dg.materialize([finished]).success
    captured = capsys.readouterr()
    assert "checkpoint flushed" in captured.out
    assert "final diagnostic" in captured.err
    assert not any("scancel" in cmd or "rm -rf" in cmd for cmd in pool.commands)


def test_subclass_hooks_survive_submission_reattachment_and_promotion(
    tmp_path, monkeypatch
):
    calls = []

    class CustomAllocation(SlurmAllocation):
        def ensure_ray_cluster(self, **kwargs):
            calls.append(("cluster", self.slurm_job_id, self.config._context))
            return "custom-address"

        def _read_ray_start_options(self, ssh_pool):
            return {"launcher": object(), "activation_script": "custom-env"}

    class CustomSession(SlurmSessionResource):
        cluster_profile: str = "default"

        def _make_allocation(self, **kwargs):
            return CustomAllocation(config=self, **kwargs)

        def _submit_allocation(self, **kwargs):
            calls.append(("submit", self.cluster_profile, self._context))
            return super()._submit_allocation(**kwargs)

        def _load_allocation_nodes(self, allocation):
            calls.append(("nodes", self.cluster_profile, self._context))
            super()._load_allocation_nodes(allocation)

    class ContextPool(RelayPool):
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    pool = ContextPool()
    monkeypatch.setattr(session_module, "SSHConnectionPool", lambda *a: pool)
    context = SimpleNamespace(
        run=SimpleNamespace(run_id="relay", tags={}), instance=None
    )
    first = CustomSession(
        slurm=_mock_slurm_resource(remote_base=str(tmp_path)),
        num_nodes=1,
        cluster_profile="special",
        enable_health_checks=False,
    )
    first.setup_for_execution(cast(Any, context))
    assert isinstance(first.allocation, CustomAllocation)
    successor = first.submit_successor(SlurmRunAllocationConfig(num_nodes=2))
    assert isinstance(successor, CustomAllocation)
    assert isinstance(successor.config, CustomSession)
    assert successor.config.cluster_profile == "special"
    assert successor.config._context is context
    assert successor.config._heartbeat_thread is None
    assert not successor.config._initialized
    assert successor.config._heartbeat_stop is not first._heartbeat_stop

    restarted = CustomSession(slurm=first.slurm, cluster_profile="different")
    restarted.setup_for_execution(cast(Any, context))
    assert isinstance(restarted.allocation.config, CustomSession)
    assert restarted.allocation.config.cluster_profile == "special"
    assert isinstance(restarted.allocation, CustomAllocation)
    assert restarted.allocation.config._require_ssh_pool() is pool
    assert restarted.allocation.config._context is context
    # Publishing tags isn't relevant for this resource context without an instance.
    monkeypatch.setattr(restarted, "_publish_allocation_tags", lambda *a: None)
    pool.states[701] = "RUNNING"
    promoted = restarted.promote_successor()
    assert isinstance(promoted, CustomAllocation)
    assert isinstance(promoted.config, CustomSession)
    assert promoted.config.num_nodes == 2
    assert promoted.config.enable_health_checks is False
    assert promoted.config.cluster_profile == "special"
    assert isinstance(first.allocation, CustomAllocation)
    assert first.allocation.slurm_job_id == 701
    assert first.allocation.config._context is context
    assert calls.count(("submit", "special", context)) == 2
    assert ("cluster", 701, context) in calls
    assert ("nodes", "special", context) in calls


def test_successor_directives_apply_to_only_one_submission(tmp_path):
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    for supplied, expected in [
        (
            SlurmRunAllocationConfig(
                time_min="00:05:00",
                extra_sbatch_directives=[
                    "--begin=now+1hour",
                    "--dependency=afterany:123",
                ],
            ),
            ["--begin=now+1hour", "--dependency=afterany:123"],
        ),
        (None, []),
        (
            SlurmRunAllocationConfig(extra_sbatch_directives=["--begin=now+2hours"]),
            ["--begin=now+2hours"],
        ),
        (SlurmRunAllocationConfig(extra_sbatch_directives=[]), []),
    ]:
        successor = session.submit_successor(supplied)
        assert successor.config.extra_sbatch_directives == expected
        assert successor.config.time_min == "00:05:00"
        script = Path(successor.session_dir, "allocation.sh").read_text()
        assert ("#SBATCH --begin=" in script) == bool(expected)
        pool.states[successor.slurm_job_id] = "RUNNING"
        session.promote_successor()


@pytest.mark.parametrize(
    "race",
    ["new_payload", "concurrent_promotion", "ended_successor", "changed_successor"],
)
def test_promotion_releases_locks_for_startup_then_revalidates(
    tmp_path, monkeypatch, race
):
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    predecessor = session.allocation
    successor = session.submit_successor()
    pool.states[701] = "RUNNING"
    starting = threading.Event()
    resume = threading.Event()

    def start_cluster(self, **kwargs):
        starting.set()
        assert resume.wait(timeout=10)

    monkeypatch.setattr(SlurmAllocation, "ensure_ray_cluster", start_cluster)
    with ThreadPoolExecutor(max_workers=2) as executor:
        promotion = executor.submit(session.promote_successor, launcher=object())
        assert starting.wait(timeout=5)
        try:

            def register_or_promote():
                # Same local and remote locks used by allocation lookup/execute.
                assert session.allocation.slurm_job_id == 700
                with session._session_lock(predecessor.session_dir):
                    if race == "new_payload":
                        Path(predecessor.working_dir, "payloads", "new").mkdir(
                            parents=True
                        )
                    elif race == "concurrent_promotion":
                        metadata = session._allocation_record(successor)
                        metadata["predecessors"] = [
                            session._allocation_record(predecessor)
                        ]
                        session._write_session_metadata(
                            predecessor.session_dir, metadata
                        )
                    elif race == "ended_successor":
                        pool.states[701] = "FAILED"
                    else:
                        metadata = session._read_session_metadata(
                            predecessor.session_dir
                        )
                        assert metadata is not None
                        metadata["successor"]["slurm_job_id"] = 702
                        session._write_session_metadata(
                            predecessor.session_dir, metadata
                        )

            executor.submit(register_or_promote).result(timeout=5)
        finally:
            resume.set()
        if race == "concurrent_promotion":
            assert promotion.result(timeout=5).slurm_job_id == 701
        else:
            with pytest.raises(
                RuntimeError,
                match={
                    "new_payload": "still active",
                    "ended_successor": "not RUNNING",
                    "changed_successor": "changed",
                }[race],
            ):
                promotion.result(timeout=5)
            assert session.allocation.slurm_job_id == 700
            assert pool.states[700] == "RUNNING"


@pytest.mark.parametrize("completion", ["status", "terminal"])
def test_step_scheduler_checks_are_throttled_without_delaying_status(
    tmp_path, monkeypatch, completion
):
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    allocation = session.allocation
    clock = [0.0]
    scheduler_calls = []
    updates = []
    original_run = pool.run
    (tmp_path / "id").write_text("700.1")

    def run(cmd, timeout=None):
        if cmd.startswith(("squeue ", "sacct ")):
            scheduler_calls.append((clock[0], cmd.split()[0]))
            if completion == "terminal":
                return "TIMEOUT" if clock[0] >= 60 and cmd.startswith("sacct ") else ""
        return original_run(cmd, timeout)

    def advance(seconds):
        clock[0] += seconds
        if completion == "status" and clock[0] == 31:
            (tmp_path / "drain_signal").write_text("TERM")
            (tmp_path / "status").write_text("0")

    monkeypatch.setattr(pool, "run", run)
    monkeypatch.setattr(session_module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(session_module.time, "sleep", advance)

    def wait():
        return allocation.wait_for_step(
            result=SlurmStepExecutionResult(
                job_id=700,
                stdout_path="out",
                stderr_path="err",
                step_id_path=str(tmp_path / "id"),
                status_path=str(tmp_path / "status"),
            ),
            ssh_pool=cast(SSHConnectionPool, pool),
            step_update_callback=updates.append,
            timeout=65,
        )

    if completion == "status":
        result = wait()
        assert result.drained and result.exit_code == 0
        assert clock[0] == 31
        assert scheduler_calls == [(0, "squeue"), (30, "squeue")]
    else:
        with pytest.raises(SlurmAllocationEnded) as error:
            wait()
        assert error.value.result.allocation_state == "TIMEOUT"
        assert scheduler_calls == [
            (at, cmd) for at in (0, 30, 60) for cmd in ("squeue", "sacct")
        ]
    assert len(updates) == 1
    assert sum(f"cat {tmp_path}/id " in cmd for cmd in pool.commands) == 1


def test_signal_before_workload_launch_skips_work(tmp_path):
    workload = tmp_path / "workload.sh"
    started = tmp_path / "started"
    workload.write_text(f"touch {shlex.quote(str(started))}\nexit 3\n")
    marker = tmp_path / "drain_signal"
    script = build_pre_timeout_supervisor_script(
        str(workload), "TERM@120", str(marker), registration_script="kill -TERM $$"
    )
    assert script is not None
    result = subprocess.run(["bash", "-c", script], capture_output=True, timeout=5)
    assert result.returncode == 0
    assert marker.read_text().strip() == "TERM"
    assert not started.exists()


def test_signal_during_setsid_startup_is_forwarded(tmp_path):
    real_setsid = shutil.which("setsid")
    assert real_setsid
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    # Signal while the child still belongs to the parent's group; setsid is
    # deliberately held until the supervisor has received the signal.
    (fake_bin / "setsid").write_text(
        f'#!/bin/bash\nkill -TERM $PPID\nsleep 0.1\nexec {shlex.quote(real_setsid)} "$@"\n'
    )
    (fake_bin / "setsid").chmod(0o755)
    workload = tmp_path / "workload.sh"
    workload.write_text("trap 'exit 7' TERM\necho ready\nsleep 30 &\nwait\n")
    marker = tmp_path / "drain_signal"
    script = build_pre_timeout_supervisor_script(str(workload), "TERM@120", str(marker))
    assert script is not None
    process = subprocess.Popen(
        ["bash", "-c", script],
        env={**os.environ, "PATH": f"{fake_bin}:{os.environ['PATH']}"},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        process.communicate(timeout=5)
        # It may get TERM before installing its trap, but must never run unaware.
        assert process.returncode in (7, 128 + signal.SIGTERM)
        assert marker.read_text().strip() == "TERM"
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate(timeout=5)
