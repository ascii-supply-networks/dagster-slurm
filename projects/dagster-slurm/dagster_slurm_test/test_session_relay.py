"""Walltime relay contracts, including real Bash signal delivery."""

import json
import os
import selectors
import shlex
import signal
import subprocess
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest

from dagster_slurm import (
    ComputeResource,
    BashLauncher,
    RayLauncher,
    SlurmAllocationEnded,
    SlurmRunAllocationConfig,
    SlurmSessionResource,
    SlurmStepExecutionResult,
    SlurmStepDrained,
)
from dagster_slurm.helpers.signals import build_pre_timeout_supervisor_script
from dagster_slurm.helpers.ssh_pool import SSHConnectionPool
from dagster_slurm.launchers.base import ExecutionPlan
from dagster_slurm.config.runtime import RuntimeVariant
from dagster_slurm.resources.session import SlurmAllocation
from dagster_slurm.pipes_clients.slurm_pipes_client import SlurmPipesClient
from dagster_slurm.sensors import reconcile_orphaned_slurm_runs
from dagster_slurm_test.test_resources import (
    LocalSlurmFakeSSHPool,
    _mock_slurm_resource,
    _render_allocation_script,
)
from dagster_slurm_test.test_env_caching import (
    FakePool,
    configure_client_for_local_run,
    make_context,
)


class RelayPool(LocalSlurmFakeSSHPool):
    def __init__(self):
        super().__init__()
        self.states: dict[int, str] = {}
        self.end = "2026-09-28T20:00:00"

    def run(self, cmd: str, timeout: int | None = None) -> str:
        args = shlex.split(cmd)
        if cmd.startswith("squeue "):
            self.commands.append(cmd)
            if "%e" in args:
                return self.end
            return self.states.get(int(args[args.index("-j") + 1]), "")
        if cmd.startswith("scancel "):
            self.commands.append(cmd)
            ids = self.states if "--name=" in cmd else [int(args[-1])]
            for job_id in ids:
                self.states[job_id] = "CANCELLED"
            return ""
        output = super().run(cmd, timeout)
        if cmd.startswith("sbatch "):
            self.states[self.submitted_jobs[-1]] = (
                "RUNNING" if len(self.states) == 0 else "PENDING"
            )
        return output


def make_session(tmp_path: Path, pool: RelayPool) -> SlurmSessionResource:
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


@pytest.mark.parametrize(
    "directives",
    [
        ["--begin=now+1hour", "--dependency=afterany:123"],
        ["--licenses=solver:2", "--exclusive"],
    ],
)
def test_relay_scheduler_options_reach_run_allocation(
    tmp_path, monkeypatch, directives
):
    compute = ComputeResource(
        mode="slurm",
        slurm=_mock_slurm_resource(),
        allocation_scope="run",
        default_launcher=RayLauncher(num_gpus_per_node=0),
        run_allocation=SlurmRunAllocationConfig(
            time_limit="00:10:00",
            time_min="00:05:00",
            extra_sbatch_directives=directives,
        ),
    )
    monkeypatch.setattr(SlurmSessionResource, "setup_for_execution", lambda *args: None)
    session = compute.get_run_allocation_session(dg.build_init_resource_context())
    script, _ = _render_allocation_script(
        session, monkeypatch, run_id="relay", job_id=123
    )
    assert "#SBATCH --time-min=00:05:00" in script
    for directive in directives:
        assert f"#SBATCH {directive}" in script


@pytest.mark.parametrize(
    "directive",
    [
        "--begin=now\necho injected",
        "--begin=now --time=100",
        "-t=100",
        "--signal=KILL@30",
        "--sig=KILL@30",
        "--output=elsewhere",
        "--wrap=evil",
        "--array=1-10",
        "#SBATCH --begin=now",
        "--job-name=other",
        "--begin=now\x00",
    ],
)
def test_unsafe_or_managed_directives_are_rejected(directive):
    with pytest.raises(ValueError):
        SlurmRunAllocationConfig(extra_sbatch_directives=[directive])


def test_time_min_and_drain_validation():
    for time_min in ("0", "00:11:00", "invalid"):
        with pytest.raises(ValueError):
            SlurmRunAllocationConfig(time_limit="00:10:00", time_min=time_min)
    with pytest.raises(ValueError, match="lead time"):
        SlurmSessionResource(
            slurm=_mock_slurm_resource(),
            time_min="00:01:00",
            signal_before_timeout="TERM@120",
        )
    with pytest.raises(ValueError, match="trappable"):
        SlurmSessionResource(
            slurm=_mock_slurm_resource(), signal_before_timeout="KILL@120"
        )


def test_successor_publication_promotion_and_reattachment(tmp_path, monkeypatch):
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    first = session.allocation
    assert first.working_dir.endswith("/jobs/700")
    successor = session.submit_successor(
        SlurmRunAllocationConfig(time_limit="01:00:00", num_nodes=2)
    )
    metadata_path = Path(first.session_dir) / "allocation.json"
    metadata = json.loads(metadata_path.read_text())
    assert metadata["slurm_job_id"] == 700
    assert metadata["successor"]["slurm_job_id"] == 701
    assert successor.nodes == []
    assert successor.working_dir.endswith("/jobs/701")
    assert successor.config.time_limit == "01:00:00"
    assert successor.config.signal_before_timeout == "TERM@120"
    with pytest.raises(RuntimeError, match="already published"):
        session.submit_successor()
    with pytest.raises(RuntimeError, match="not RUNNING"):
        session.promote_successor()

    # A restart while the predecessor has ended must attach, never resubmit.
    pool.states[700] = "TIMEOUT"
    restarted = make_session(tmp_path, pool)
    assert restarted.allocation.slurm_job_id == 700
    assert pool.submitted_jobs == [700, 701]
    pool.states[701] = "RUNNING"
    instance = MagicMock()
    instance.get_run_by_id.return_value = SimpleNamespace(
        tags={"dagster_slurm/session_step_id": "700.1"}
    )
    object.__setattr__(
        restarted,
        "_context",
        SimpleNamespace(run=SimpleNamespace(run_id="relay"), instance=instance),
    )
    started_ray = []
    monkeypatch.setattr(
        SlurmAllocation,
        "_read_ray_start_options",
        lambda *args: {
            "launcher": RayLauncher(num_gpus_per_node=0),
            "activation_script": "source env.sh",
        },
    )
    monkeypatch.setattr(
        SlurmAllocation,
        "ensure_ray_cluster",
        lambda self, **kwargs: started_ray.append(self.slurm_job_id),
    )
    promoted = restarted.promote_successor()
    assert promoted.slurm_job_id == 701
    assert started_ray == [701]
    assert restarted.promote_successor().slurm_job_id == 701
    assert session.allocation.slurm_job_id == 701  # Another live process refreshes.
    tags = instance.add_run_tags.call_args.args[1]
    assert tags["dagster_slurm/job_id"] == "701"
    assert tags["dagster_slurm/session_allocation_dir"] == first.session_dir
    assert tags["dagster_slurm/session_step_id"] == ""
    assert "successor" not in json.loads(metadata_path.read_text())


def test_promotion_waits_for_registered_payload_including_pending_srun(tmp_path):
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    current = session.allocation
    session.submit_successor()
    pool.states[701] = "RUNNING"
    invocation = Path(current.working_dir) / "payloads" / "starting"
    invocation.mkdir(parents=True)
    with pytest.raises(RuntimeError, match="still active"):
        session.promote_successor()
    assert pool.states[700] == "RUNNING"
    (invocation / "status").write_text("0")
    assert session.promote_successor().slurm_job_id == 701
    assert pool.states[700] == "CANCELLED"


def test_end_time_is_reread_and_cleanup_cancels_successor(tmp_path):
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    assert session.allocation.end_time == "2026-09-28T20:00:00"
    pool.end = "2026-09-28T16:00:00"
    assert session.allocation.end_time == pool.end
    session.submit_successor()
    object.__setattr__(session, "_initialized", True)
    # Only prevent this filesystem fake's missing context-manager close method.
    session.teardown_after_execution(dg.build_init_resource_context())
    assert pool.states == {700: "CANCELLED", 701: "CANCELLED"}


@pytest.mark.parametrize("exit_code", [0, 7])
def test_wait_reports_drain_and_payload_exit_code(tmp_path, exit_code):
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    invocation = tmp_path / "step"
    invocation.mkdir()
    (invocation / "id").write_text("700.2")
    (invocation / "status").write_text(str(exit_code))
    (invocation / "drain_signal").write_text("TERM")
    result = session.allocation.wait_for_step(
        SlurmStepExecutionResult(
            job_id=700,
            stdout_path="out",
            stderr_path="err",
            step_id_path=str(invocation / "id"),
            status_path=str(invocation / "status"),
        ),
        ssh_pool=cast(SSHConnectionPool, pool),
        timeout=1,
    )
    assert result.drained and result.drain_signal == "TERM"
    assert result.exit_code == exit_code


def test_wait_reports_allocation_end_without_status(tmp_path):
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    pool.states[700] = "TIMEOUT"
    with pytest.raises(SlurmAllocationEnded) as error:
        session.allocation.wait_for_step(
            SlurmStepExecutionResult(
                job_id=700,
                stdout_path="out",
                stderr_path="err",
                step_id_path=str(tmp_path / "id"),
                status_path=str(tmp_path / "status"),
            ),
            ssh_pool=cast(SSHConnectionPool, pool),
            timeout=1,
        )
    assert error.value.result.allocation_state == "TIMEOUT"


def _ready(process: subprocess.Popen) -> None:
    assert process.stdout is not None
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        assert selector.select(timeout=10), "shell did not become ready"
        assert process.stdout.readline().strip() in {"ready", "Allocation started"}


@pytest.mark.parametrize("requested", [False, True])
def test_batch_signal_drains_only_payload_and_keeps_allocation_alive(
    tmp_path, monkeypatch, requested
):
    session = SlurmSessionResource(
        slurm=_mock_slurm_resource(remote_base=str(tmp_path)),
        signal_before_timeout="TERM@120",
    )
    script, _ = _render_allocation_script(
        session, monkeypatch, run_id="relay", job_id=42
    )
    job_dir = tmp_path / "allocations" / "dagster_relay" / "jobs" / "42"
    invocation = job_dir / "payloads" / "one"
    invocation.mkdir(parents=True)
    workload = tmp_path / "workload.sh"
    workload.write_text("trap 'sleep 0.1; exit 7' TERM\necho ready\nsleep 30 &\nwait\n")
    supervisor = tmp_path / "supervisor.sh"
    supervisor.write_text(
        build_pre_timeout_supervisor_script(
            str(workload), "TERM@120", str(invocation / "drain_signal")
        )
        or ""
    )
    payload = subprocess.Popen(
        ["bash", str(supervisor)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    batch = None
    try:
        _ready(payload)
        (invocation / "id").write_text("42.7")
        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        # The allocation trap must select driver step 7, never Ray step 99.
        (fake_bin / "scancel").write_text(
            f"#!/bin/bash\n[[ \"$*\" == '--signal=TERM 42.7' ]] || exit 19\nkill -TERM {payload.pid}\n"
        )
        (fake_bin / "scontrol").write_text("#!/bin/bash\necho node-a\n")
        for path in fake_bin.iterdir():
            path.chmod(0o755)
        batch_file = tmp_path / "batch.sh"
        batch_file.write_text(script)
        env = {
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "SLURM_JOB_ID": "42",
            "SLURM_JOB_NODELIST": "node-a",
        }
        batch = subprocess.Popen(
            ["bash", str(batch_file)],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        _ready(batch)
        if requested:
            pool = MagicMock()

            def send_batch_signal(command):
                assert command == "scancel --batch --signal=TERM 42"
                os.kill(batch.pid, signal.SIGTERM)

            pool.run.side_effect = send_batch_signal
            object.__setattr__(session, "_ssh_pool", pool)
            SlurmAllocation(42, ["node-a"], str(job_dir), session).request_drain()
        else:
            os.kill(batch.pid, signal.SIGTERM)
        payload.communicate(timeout=10)
        assert payload.returncode == 7
        assert (invocation / "drain_signal").read_text().strip() == "TERM"
        assert (job_dir / "drain_signal").read_text().strip() == "TERM"
        assert batch.poll() is None
    finally:
        for process in (batch, payload):
            if process is not None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.communicate(timeout=5)


@pytest.mark.parametrize(
    "state,healthy", [("PENDING", True), ("RUNNING", True), ("FAILED", False)]
)
def test_orphan_sensor_respects_published_successor(tmp_path, state, healthy):
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    first = session.allocation
    session.submit_successor()
    pool.states.update({700: "COMPLETED", 701: state})
    with dg.DagsterInstance.ephemeral() as instance:
        run = dg.DagsterRun(
            job_name="relay",
            run_id="relay",
            status=dg.DagsterRunStatus.STARTED,
            tags={
                "dagster_slurm/job_id": "700",
                "dagster_slurm/run_dir": str(tmp_path),
                "dagster_slurm/session_allocation_dir": first.session_dir,
            },
        )
        instance.add_run(run)
        context_pool = MagicMock()
        context_pool.__enter__.return_value = pool
        with patch(
            "dagster_slurm.sensors.SSHConnectionPool", return_value=context_pool
        ):
            requests = reconcile_orphaned_slurm_runs(instance, session.slurm)
        assert (requests == []) == healthy
        stored = instance.get_run_by_id("relay")
        assert stored is not None
        assert (stored.status == dg.DagsterRunStatus.STARTED) == healthy


def test_promotion_refreshes_under_lock_and_preserves_successor_step_tags(
    tmp_path, monkeypatch
):
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    first = session.allocation
    successor = session.submit_successor()
    successor.nodes = ["node-a"]
    pool.states[701] = "RUNNING"
    original_lock = session._session_lock

    @contextmanager
    def concurrent_promotion(session_dir):
        with original_lock(session_dir):
            # A second supervisor won the lock and promoted before this one.
            metadata = session._allocation_record(successor)
            metadata["predecessors"] = [session._allocation_record(first)]
            session._write_session_metadata(session_dir, metadata)
            yield

    with dg.DagsterInstance.ephemeral() as instance:
        step_tags = {
            "dagster_slurm/job_id": "701",
            "dagster_slurm/run_dir": "/successor/invocation",
            "dagster_slurm/session_step_id": "701.1",
            "dagster_slurm/session_step_status_path": "/successor/status",
        }
        run = dg.DagsterRun(job_name="relay", run_id="relay", tags=step_tags)
        instance.add_run(run)
        object.__setattr__(
            session, "_context", SimpleNamespace(run=run, instance=instance)
        )
        monkeypatch.setattr(session, "_session_lock", concurrent_promotion)
        assert session.promote_successor().slurm_job_id == 701
        stored = instance.get_run_by_id("relay")
        assert stored is not None
        assert all(stored.tags[key] == value for key, value in step_tags.items())


def test_stale_supervisor_with_live_successor_still_gets_recovery(tmp_path):
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    first = session.allocation
    session.submit_successor()
    pool.states[700] = "TIMEOUT"
    with dg.DagsterInstance.ephemeral() as instance:
        run = dg.DagsterRun(
            job_name="relay",
            run_id="relay",
            status=dg.DagsterRunStatus.STARTED,
            tags={
                "dagster_slurm/job_id": "700",
                "dagster_slurm/run_dir": str(tmp_path),
                "dagster_slurm/session_allocation_dir": first.session_dir,
                "dagster_slurm/last_supervisor_heartbeat": "1",
            },
        )
        instance.add_run(run)
        context_pool = MagicMock()
        context_pool.__enter__.return_value = pool
        with patch(
            "dagster_slurm.sensors.SSHConnectionPool", return_value=context_pool
        ):
            requests = reconcile_orphaned_slurm_runs(instance, session.slurm, now=1000)
        assert len(requests) == 1
        assert (
            requests[0].tags["dagster_slurm/session_allocation_dir"]
            == first.session_dir
        )


def test_drained_nonzero_step_is_reattached_but_retired_allocation_is_not(tmp_path):
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    client = SlurmPipesClient(
        slurm_resource=session.slurm, launcher=BashLauncher(), session_resource=session
    )
    status = tmp_path / "status"
    status.write_text("7")
    (tmp_path / "drain_signal").write_text("TERM")
    context = MagicMock()
    context.dagster_run.tags = {
        "dagster_slurm/job_id": "700",
        "dagster_slurm/run_dir": "/old",
        "dagster_slurm/session_step_status_path": str(status),
    }
    context.dagster_run.parent_run_id = None
    assert (
        client._find_reattachable_job(context, cast(SSHConnectionPool, pool), None)
        is not None
    )
    session.submit_successor()
    pool.states[701] = "RUNNING"
    session.promote_successor()
    assert (
        client._find_reattachable_job(context, cast(SSHConnectionPool, pool), None)
        is None
    )


def test_fresh_pipes_invocations_have_separate_streams_after_drain(
    monkeypatch, tmp_path
):
    pool = FakePool()
    slurm = _mock_slurm_resource()
    session = SlurmSessionResource(slurm=slurm)
    object.__setattr__(session, "_ssh_pool", pool)
    object.__setattr__(session, "_initialized", True)
    object.__setattr__(
        session, "_allocation", SlurmAllocation(42, ["node-a"], "/allocation", session)
    )
    client = SlurmPipesClient(
        slurm_resource=slurm, launcher=BashLauncher(), session_resource=session
    )
    configure_client_for_local_run(client, monkeypatch, pool)
    dirs = []
    streams = []

    def execute(**kwargs):
        dirs.append(kwargs["run_dir"])
        return SlurmStepExecutionResult(
            job_id=42,
            stdout_path="out",
            stderr_path="err",
            drain_signal="TERM",
            exit_code=0,
        )

    monkeypatch.setattr(client, "_execute_in_session", execute)
    monkeypatch.setattr(
        "dagster_slurm.pipes_clients.slurm_pipes_client.SSHMessageReader",
        lambda **kwargs: streams.append(kwargs["remote_path"]),
    )
    payload = tmp_path / "payload.py"
    payload.write_text("print('hello')")
    for _ in range(2):
        with pytest.raises(SlurmStepDrained):
            client.run(
                context=make_context(),
                payload_path=str(payload),
                use_session=True,
                defer_cleanup=True,
            )
    assert len(set(dirs)) == len(set(streams)) == 2
    assert streams == [f"{directory}/messages.jsonl" for directory in dirs]
    assert not any(
        "scancel" in command or "rm -rf" in command for command in pool.commands
    )


def test_attached_driver_finishes_python_checkpoint_before_step_exits(tmp_path):
    checkpoint = tmp_path / "checkpoint"
    payload = tmp_path / "driver.py"
    payload.write_text(
        "import signal, time\nfrom pathlib import Path\n"
        "def drain(*args):\n    time.sleep(0.3)\n"
        f"    Path({str(checkpoint)!r}).write_text('committed')\n"
        "    raise SystemExit(7)\n"
        "signal.signal(signal.SIGTERM, drain)\nprint('ready', flush=True)\nsignal.pause()\n"
    )
    launcher = RayLauncher(ray_address="127.0.0.1:6379", num_gpus_per_node=0)
    plan = launcher.prepare_execution(str(payload), sys.executable, str(tmp_path), {})
    workload = tmp_path / "workload.sh"
    workload.write_text("\n".join(plan.payload))
    supervisor = tmp_path / "supervisor.sh"
    supervisor.write_text(
        build_pre_timeout_supervisor_script(
            str(workload), "TERM@120", str(tmp_path / "signal")
        )
        or ""
    )
    process = subprocess.Popen(
        ["bash", str(supervisor)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        assert process.stdout is not None
        # Launcher headers precede the Python driver's readiness notification.
        for _ in range(10):
            if process.stdout.readline().strip() == "ready":
                break
        else:
            pytest.fail("driver never became ready")
        os.kill(process.pid, signal.SIGTERM)
        process.communicate(timeout=10)
        assert process.returncode == 7
        assert checkpoint.read_text() == "committed"
    finally:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.communicate(timeout=5)


@pytest.mark.needs_slurm_docker
@pytest.mark.parametrize(
    "walltime,persistent_cluster",
    [(False, False), pytest.param(True, False, marks=pytest.mark.slow), (False, True)],
)
def test_slurm_session_drain_and_successor(
    slurm_resource_for_testing,
    slurm_cluster_ready,
    walltime,
    persistent_cluster,
    request,
):
    """A real step drains while an unregistered control-plane step stays alive."""
    session = SlurmSessionResource(
        slurm=slurm_resource_for_testing,
        num_nodes=1,
        partition="normal",
        time_limit="00:03:00" if walltime else "00:10:00",
        signal_before_timeout="TERM@60",
        cpus_per_task=2 if persistent_cluster else 1,
        mem="3G" if persistent_cluster else "1G",
        enable_health_checks=False,
    )
    context = SimpleNamespace(
        run=SimpleNamespace(run_id=f"relay_{uuid.uuid4().hex}", tags={}),
        instance=MagicMock(),
    )
    session.setup_for_execution(cast(Any, context))
    try:
        allocation = session.allocation
        pool = session._require_ssh_pool()
        if persistent_cluster:
            activation = os.getenv("DAGSTER_SLURM_RELAY_TEST_ACTIVATE")
            if activation is None:
                deployment = request.getfixturevalue("deployment_metadata")
                activation = f"{deployment['deployment_path']}/activate.sh"
            launcher = RayLauncher(num_gpus_per_node=0, object_store_memory_gb=1)
            address = allocation.ensure_ray_cluster(
                ssh_pool=pool,
                launcher=launcher,
                activation_script=activation,
                startup_timeout=120,
            )
        run_dir = f"{allocation.working_dir}/test"
        pool.run(
            f"mkdir -p {shlex.quote(run_dir)} && mkfifo {shlex.quote(run_dir + '/ready')}"
        )
        control_id = f"{run_dir}/control.id"
        control_script = f"{run_dir}/control.sh"
        pool.write_file(
            f'#!/bin/bash\necho "$SLURM_JOB_ID.$SLURM_STEP_ID" > {shlex.quote(control_id)}\nsleep infinity\n',
            control_script,
        )
        pool.run(
            f"nohup srun --overlap --jobid={allocation.slurm_job_id} -N1 -n1 --job-name=ray_control bash {shlex.quote(control_script)} >/dev/null 2>&1 </dev/null &"
        )
        plan = ExecutionPlan(
            kind=RuntimeVariant.SHELL,
            payload=[
                "#!/bin/bash",
                f"trap 'sleep 1; echo flushed > {shlex.quote(run_dir + '/checkpoint')}; exit 7' TERM",
                f"echo ready > {shlex.quote(run_dir + '/ready')}",
                "sleep 180 &",
                "wait",
            ],
            environment={},
            resources={},
        )
        with (
            SSHConnectionPool(session.slurm.ssh) as observer,
            ThreadPoolExecutor(max_workers=1) as executor,
        ):
            future = executor.submit(
                allocation.execute,
                plan,
                asset_key="drain",
                run_dir=run_dir,
                ssh_pool=pool,
                timeout=180,
            )
            assert (
                observer.run(
                    f"timeout 30 cat {shlex.quote(run_dir + '/ready')}"
                ).strip()
                == "ready"
            )
            if not walltime:
                allocation.request_drain()
            result = future.result(timeout=180)
        assert result.drained and result.exit_code == 7
        assert (
            pool.run(f"cat {shlex.quote(run_dir + '/checkpoint')}").strip() == "flushed"
        )
        assert session._get_job_state(allocation.slurm_job_id) == "RUNNING"
        step_id = pool.run(f"cat {shlex.quote(control_id)}").strip()
        assert "RUNNING" in pool.run(f"scontrol show step {shlex.quote(step_id)}")
        assert allocation.end_time is not None
        if persistent_cluster:
            check = f"source {shlex.quote(activation)} && ray status --address={shlex.quote(address)}"
            assert "Healthy:" in pool.run(
                f"srun --overlap --jobid={allocation.slurm_job_id} -N1 -n1 bash -c {shlex.quote(check)}"
            )

        successor = session.submit_successor(
            SlurmRunAllocationConfig(
                time_limit="00:10:00", extra_sbatch_directives=["--begin=now+1hour"]
            )
        )
        assert session._get_job_state(successor.slurm_job_id) == "PENDING"
        metadata = json.loads(
            pool.run(f"cat {shlex.quote(allocation.session_dir + '/allocation.json')}")
        )
        assert metadata["successor"]["slurm_job_id"] == successor.slurm_job_id
        pool.run(f"scontrol update JobId={successor.slurm_job_id} StartTime=now")
        session._wait_for_allocation_start(
            successor.slurm_job_id, successor.working_dir, timeout=120
        )
        promoted = session.promote_successor()
        assert promoted.slurm_job_id == successor.slurm_job_id
        if persistent_cluster:
            assert promoted._ray_address is not None
            check = f"source {shlex.quote(activation)} && ray status --address={shlex.quote(promoted._ray_address)}"
            assert "Healthy:" in pool.run(
                f"srun --overlap --jobid={promoted.slurm_job_id} -N1 -n1 bash -c {shlex.quote(check)}"
            )
        next_result = promoted.execute(
            ExecutionPlan(
                kind=RuntimeVariant.SHELL,
                payload=["#!/bin/bash", "exit 0"],
                environment={},
                resources={},
            ),
            asset_key="next",
            run_dir=run_dir,
            ssh_pool=pool,
            timeout=30,
        )
        assert next_result.job_id == promoted.slurm_job_id and not next_result.drained
    finally:
        session.teardown_after_execution(cast(Any, context))
