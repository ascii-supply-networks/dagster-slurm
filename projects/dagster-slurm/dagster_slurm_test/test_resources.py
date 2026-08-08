"""Tests for resources."""

import os
import shlex
import signal
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from dagster import build_init_resource_context
import dagster_slurm.resources.session as session_module
from dagster_slurm import (
    SlurmQueueConfig,
    SlurmResource,
    SSHConnectionResource,
    BashLauncher,
    ComputeResource,
    RayLauncher,
    RayPortConfig,
    SlurmAllocationScope,
    SlurmRunAllocationConfig,
)
from dagster_slurm.config.environment import ExecutionMode
from dagster_slurm.config.runtime import RuntimeVariant
from dagster_slurm.helpers.message_readers import SSHMessageReader
from dagster_slurm.helpers.ssh_pool import SSHConnectionPool
from dagster_slurm.launchers.base import ExecutionPlan
from dagster_slurm.pipes_clients.slurm_pipes_client import SlurmPipesClient
from dagster_slurm.resources.session import (
    SlurmAllocation,
    SlurmSessionResource,
    SlurmStepExecutionResult,
    _try_acquire_remote_lock,
)


def _mock_slurm_resource(
    *,
    gpus_per_node: int = 0,
    remote_base: str = "/tmp/dagster_test",
    signal_before_timeout: str | None = None,
) -> SlurmResource:
    ssh = SSHConnectionResource(
        host="localhost",
        port=2223,
        user="testuser",
        password="test",
    )
    return SlurmResource(
        ssh=ssh,
        queue=SlurmQueueConfig(
            partition="test",
            time_limit="00:10:00",
            cpus=2,
            mem="1G",
            gpus_per_node=gpus_per_node,
            signal_before_timeout=signal_before_timeout,
        ),
        remote_base=remote_base,
    )


class _RecordingAllocationSSHPool:
    def __init__(self, job_id: int):
        self.job_id = job_id
        self.commands: list[str] = []
        self.writes: list[tuple[str, str]] = []

    def write_file(self, content: str, remote_path: str) -> None:
        self.writes.append((remote_path, content))

    def run(self, cmd: str) -> str:
        self.commands.append(cmd)
        if ".allocation.lock" in cmd and "printf acquired" in cmd:
            return "acquired"
        if cmd.startswith("sbatch "):
            return f"Submitted batch job {self.job_id}"
        if cmd.startswith("cat ") and "nodes.txt" in cmd:
            return "c1\n"
        return ""


def _render_allocation_script(
    session: SlurmSessionResource,
    monkeypatch,
    *,
    run_id: str,
    job_id: int,
) -> tuple[str, _RecordingAllocationSSHPool]:
    ssh_pool = _RecordingAllocationSSHPool(job_id)
    object.__setattr__(session, "_ssh_pool", cast(SSHConnectionPool, ssh_pool))
    monkeypatch.setattr(session, "_resolve_run_id", lambda context: run_id)
    monkeypatch.setattr(session, "_log_estimated_start_time", lambda job_id: None)
    monkeypatch.setattr(
        session,
        "_wait_for_allocation_start",
        lambda job_id, working_dir, timeout: None,
    )

    allocation = session._create_allocation(build_init_resource_context())

    assert allocation.slurm_job_id == job_id
    assert ssh_pool.writes
    return ssh_pool.writes[0][1], ssh_pool


@pytest.fixture
def mock_ssh_key_path(tmp_path: Path) -> Path:
    key_path = tmp_path / "id_rsa"
    key_path.touch()
    return key_path


def test_ssh_resource_creation(mock_ssh_key_path: Path):
    """Test SSH resource creation."""
    ssh = SSHConnectionResource(
        host="example.com",
        port=22,
        user="testuser",
        key_path=str(mock_ssh_key_path),
    )

    assert ssh.host == "example.com"
    assert ssh.port == 22
    assert ssh.user == "testuser"
    assert ssh.key_path == str(mock_ssh_key_path)


def test_ssh_resource_creation_password():
    """Test SSH resource creation."""
    ssh = SSHConnectionResource(
        host="example.com",
        port=22,
        user="testuser",
        password="testpassword",
    )

    assert ssh.host == "example.com"
    assert ssh.port == 22
    assert ssh.user == "testuser"
    assert ssh.password == "testpassword"


def test_slurm_resource_creation(mock_ssh_key_path: Path):
    """Test Slurm resource creation."""
    ssh = SSHConnectionResource(
        host="localhost",
        port=2223,
        user="submitter",
        password="submitter",
    )

    queue = SlurmQueueConfig(
        partition="batch",
        time_limit="01:00:00",
        cpus=4,
        mem="8G",
    )

    slurm = SlurmResource(
        ssh=ssh,
        queue=queue,
        remote_base="/home/submitter/submitter",
    )

    assert slurm.ssh.host == "localhost"
    assert slurm.queue.partition == "batch"
    assert slurm.remote_base == "/home/submitter/submitter"


def test_compute_resource_local_mode(local_compute_resource):
    """Test compute resource in local mode."""
    compute = local_compute_resource

    assert compute.mode == "local"
    assert compute.slurm is None
    assert isinstance(compute.default_launcher, BashLauncher)


def test_compute_resource_slurm_mode(slurm_compute_resource):
    """Test compute resource in Slurm mode."""

    compute = slurm_compute_resource
    assert compute.mode == ExecutionMode.SLURM
    assert compute.slurm is not None


def test_compute_resource_run_allocation_scope_defaults_to_asset():
    """Default SLURM mode remains one allocation per asset."""
    compute = ComputeResource(
        mode=ExecutionMode.SLURM,
        slurm=_mock_slurm_resource(),
        default_launcher=RayLauncher(num_gpus_per_node=0),
    )

    assert compute.allocation_scope == SlurmAllocationScope.ASSET


def test_launcher_override_merge_preserves_nested_ray_port_config():
    compute = ComputeResource(
        mode=ExecutionMode.LOCAL,
        default_launcher=RayLauncher(
            port_config=RayPortConfig(
                range_start=30000,
                range_end=31999,
                block_size=1000,
            ),
        ),
    )

    merged_launcher = compute._resolve_launcher(
        compute._resolve_launcher(RayLauncher(num_gpus_per_node=2))
    )
    plan = merged_launcher.prepare_execution(
        payload_path="/remote/payload.py",
        python_executable="/remote/env/bin/python",
        working_dir="/remote/run",
        pipes_context={},
    )
    script = "\n".join(plan.payload)

    assert "_port_candidate=$(( 30000 + (_port_slot * 1000) ))" in script
    assert "No free Ray port block in 30000-31999" in script


def test_launcher_override_merges_nested_port_config_fields():
    """A nested override must not reset its siblings to class defaults.

    model_dump(exclude_unset=True) is recursive, so overriding one field of
    port_config used to replace the whole nested model - silently moving a
    firewall-restricted 30000-31999 range back to the 10000-29999 default.
    """
    compute = ComputeResource(
        mode=ExecutionMode.LOCAL,
        default_launcher=RayLauncher(
            port_config=RayPortConfig(
                range_start=30000,
                range_end=31999,
                block_size=1000,
            ),
        ),
    )

    merged_launcher = cast(
        RayLauncher,
        compute._resolve_launcher(
            RayLauncher(port_config=RayPortConfig(block_size=500))
        ),
    )

    assert merged_launcher.port_config.block_size == 500
    assert merged_launcher.port_config.range_start == 30000
    assert merged_launcher.port_config.range_end == 31999


def test_launcher_override_wins_over_nested_site_default():
    """Explicitly set nested fields still take precedence over the default."""
    compute = ComputeResource(
        mode=ExecutionMode.LOCAL,
        default_launcher=RayLauncher(
            port_config=RayPortConfig(range_start=30000, range_end=31999),
        ),
    )

    merged_launcher = cast(
        RayLauncher,
        compute._resolve_launcher(
            RayLauncher(port_config=RayPortConfig(range_start=40000, range_end=41999))
        ),
    )

    assert merged_launcher.port_config.range_start == 40000
    assert merged_launcher.port_config.range_end == 41999


def test_compute_resource_run_allocation_scope_requires_slurm_mode():
    """Run-owned allocations are only valid for the additive SLURM mode API."""
    with pytest.raises(ValueError, match="allocation_scope='run'"):
        ComputeResource(
            mode=ExecutionMode.LOCAL,
            slurm=_mock_slurm_resource(),
            default_launcher=RayLauncher(num_gpus_per_node=0),
            allocation_scope=SlurmAllocationScope.RUN,
        )


def test_run_allocation_rejects_incompatible_asset_overrides():
    """Per-asset resource changes must not silently create another allocation shape."""
    compute = ComputeResource(
        mode=ExecutionMode.SLURM,
        slurm=_mock_slurm_resource(),
        default_launcher=RayLauncher(num_gpus_per_node=0),
        allocation_scope=SlurmAllocationScope.RUN,
        run_allocation=SlurmRunAllocationConfig(
            num_nodes=2,
            cpus_per_task=4,
            mem="8G",
            time_limit="01:00:00",
            partition="test",
        ),
    )

    with pytest.raises(ValueError, match="one allocation shape"):
        compute._validate_run_allocation_overrides({"nodes": 1})


def test_run_allocation_scope_requires_ray_launcher():
    compute = ComputeResource(
        mode=ExecutionMode.SLURM,
        slurm=_mock_slurm_resource(),
        default_launcher=BashLauncher(),
        allocation_scope=SlurmAllocationScope.RUN,
    )

    with pytest.raises(ValueError, match="RayLauncher assets"):
        compute.get_pipes_client(context=build_init_resource_context())


def test_run_allocation_rejects_ray_gpu_request_above_allocation():
    compute = ComputeResource(
        mode=ExecutionMode.SLURM,
        slurm=_mock_slurm_resource(),
        default_launcher=RayLauncher(num_gpus_per_node=2),
        allocation_scope=SlurmAllocationScope.RUN,
        run_allocation=SlurmRunAllocationConfig(gpus_per_node=1),
    )

    with pytest.raises(ValueError, match="GPU settings"):
        compute.get_pipes_client(context=build_init_resource_context())


def test_slurm_allocation_execute_uses_per_step_log_paths():
    class FakeSSHPool:
        def __init__(self):
            self.commands: list[str] = []
            self.writes: list[tuple[str, str]] = []

        def write_file(self, content: str, remote_path: str):
            self.writes.append((remote_path, content))

        def run(self, cmd: str):
            self.commands.append(cmd)
            if ".slurm-step-1.id" in cmd:
                return "42.7"
            if ".slurm-step-2.id" in cmd:
                return "42.8"
            if ".slurm-step-" in cmd and ".status" in cmd:
                return "0"
            return ""

    session = SlurmSessionResource(slurm=_mock_slurm_resource())
    allocation = SlurmAllocation(
        slurm_job_id=42,
        nodes=["c1"],
        working_dir="/remote/session",
        config=session,
    )
    plan = ExecutionPlan(
        kind=RuntimeVariant.RAY,
        payload=["#!/bin/bash", "echo hello"],
        environment={},
        resources={},
    )
    fake_ssh_pool = FakeSSHPool()
    ssh_pool = cast(SSHConnectionPool, fake_ssh_pool)
    updates: list[SlurmStepExecutionResult] = []

    first = allocation.execute(
        plan,
        asset_key="asset/one",
        run_dir="/remote/run/one",
        ssh_pool=ssh_pool,
        step_update_callback=updates.append,
    )
    second = allocation.execute(
        plan,
        asset_key="asset/two",
        run_dir="/remote/run/two",
        ssh_pool=ssh_pool,
    )

    assert first.job_id == second.job_id == 42
    assert first.stdout_path != second.stdout_path
    assert first.stderr_path != second.stderr_path
    assert first.stdout_path.endswith("slurm-42-step-1_asset_one.out")
    assert second.stdout_path.endswith("slurm-42-step-2_asset_two.out")
    assert any(first.stdout_path in command for command in fake_ssh_pool.commands)
    assert any(second.stdout_path in command for command in fake_ssh_pool.commands)
    assert first.step_id == "42.7"
    assert second.step_id == "42.8"
    assert [update.step_id for update in updates] == [None, "42.7"]
    assert updates[0].status_path == "/remote/run/one/.slurm-step-1.status"


def test_slurm_session_allocation_honors_zero_gpu_override(monkeypatch):
    session = SlurmSessionResource(
        slurm=_mock_slurm_resource(gpus_per_node=4),
        gpus_per_node=0,
    )
    allocation_script, _ = _render_allocation_script(
        session,
        monkeypatch,
        run_id="run_with_no_gpus",
        job_id=123,
    )

    assert "#SBATCH --gres=gpu:" not in allocation_script


def test_slurm_session_allocation_inherits_queue_gpu_default(monkeypatch):
    session = SlurmSessionResource(slurm=_mock_slurm_resource(gpus_per_node=4))
    allocation_script, _ = _render_allocation_script(
        session,
        monkeypatch,
        run_id="run_with_gpus",
        job_id=124,
    )

    assert "#SBATCH --gres=gpu:4" in allocation_script


def test_run_allocation_nodelist_pins_sbatch_submission(monkeypatch):
    compute = ComputeResource(
        mode=ExecutionMode.SLURM,
        slurm=_mock_slurm_resource(gpus_per_node=8),
        default_launcher=RayLauncher(num_gpus_per_node=8),
        allocation_scope=SlurmAllocationScope.RUN,
        run_allocation=SlurmRunAllocationConfig(nodelist="gpu-[01-02]"),
    )
    monkeypatch.setattr(
        SlurmSessionResource,
        "setup_for_execution",
        lambda self, context: object.__setattr__(self, "_initialized", True),
    )
    session = compute._get_or_create_run_allocation_session(
        build_init_resource_context()
    )

    allocation_script, _ = _render_allocation_script(
        session,
        monkeypatch,
        run_id="run_pinned_to_nodes",
        job_id=125,
    )

    assert "#SBATCH --nodelist=gpu-[01-02]" in allocation_script
    assert "#SBATCH --gres=gpu:8" in allocation_script


@pytest.mark.parametrize("nodelist", ["", "gpu-01\ngpu-02", "gpu-01\n"])
def test_run_allocation_nodelist_rejects_invalid_expression(nodelist):
    with pytest.raises(ValueError, match="nodelist"):
        SlurmRunAllocationConfig(nodelist=nodelist)


def test_slurm_session_allocation_quotes_remote_working_dir_paths(monkeypatch):
    remote_base = "/remote/base dir;touch pwned"
    run_id = "run_with_spaces"
    expected_working_dir = f"{remote_base}/allocations/dagster_{run_id}"
    session = SlurmSessionResource(slurm=_mock_slurm_resource(remote_base=remote_base))
    allocation_script, fake_ssh_pool = _render_allocation_script(
        session,
        monkeypatch,
        run_id=run_id,
        job_id=125,
    )

    assert "#SBATCH --output=allocation_%j.log" in allocation_script
    assert f"working_dir={shlex.quote(expected_working_dir)}" in allocation_script
    assert 'hostname > "${working_dir}/head_node.txt"' in allocation_script
    assert (
        'scontrol show hostname $SLURM_JOB_NODELIST > "${working_dir}/nodes.txt"'
        in allocation_script
    )
    assert (
        f"chmod +x {shlex.quote(f'{expected_working_dir}/allocation.sh')}"
        in fake_ssh_pool.commands
    )
    assert (
        f"sbatch -D {shlex.quote(expected_working_dir)} "
        f"{shlex.quote(f'{expected_working_dir}/allocation.sh')}"
        in fake_ssh_pool.commands
    )
    assert (
        f"cat {shlex.quote(f'{expected_working_dir}/nodes.txt')}"
        in fake_ssh_pool.commands
    )


def test_asset_sbatch_command_emits_pre_timeout_signal(slurm_pipes_client):
    command = slurm_pipes_client._build_sbatch_command(
        job_name="checkpointing",
        working_dir="/remote/run",
        output_file="/remote/run/stdout",
        error_file="/remote/run/stderr",
        script_path="/remote/run/job.sh",
        extra_opts={
            "time_limit": "90",
            "signal_before_timeout": "usr1@3600",
        },
    )

    assert "--signal=B:USR1@3600" in shlex.split(command)


def test_pre_timeout_signal_flushes_checkpoint_but_returns_nonzero(
    slurm_pipes_client,
    tmp_path: Path,
):
    ready_path = tmp_path / "ready"
    checkpoint_path = tmp_path / "checkpoint"
    workload_path = tmp_path / "workload.sh"
    workload_path.write_text(
        "\n".join(
            [
                "#!/bin/bash",
                "set -uo pipefail",
                f"trap 'printf flushed > {shlex.quote(str(checkpoint_path))}; exit 0' USR1",
                f"touch {shlex.quote(str(ready_path))}",
                "while true; do sleep 0.05; done",
            ]
        ),
        encoding="utf-8",
    )
    marker_path = tmp_path / "signal_marker"
    supervisor = slurm_pipes_client._build_pre_timeout_supervisor_script(
        str(workload_path),
        "USR1@120",
        str(marker_path),
    )
    assert supervisor is not None
    supervisor_path = tmp_path / "supervisor.sh"
    supervisor_path.write_text(supervisor, encoding="utf-8")

    process = subprocess.Popen(["bash", str(supervisor_path)])
    deadline = time.monotonic() + 5
    while not ready_path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert ready_path.exists()

    os.kill(process.pid, signal.SIGUSR1)
    return_code = process.wait(timeout=5)

    assert checkpoint_path.read_text(encoding="utf-8") == "flushed"
    # The signal is recorded, but the workload's own exit code is preserved:
    # "exited 0 after the signal" cannot distinguish a job that finished from
    # one that gave up, so the Pipes session settles it instead.
    assert return_code == 0
    assert marker_path.read_text(encoding="utf-8").strip() == "USR1"


def test_standalone_job_uses_pre_timeout_supervisor(
    slurm_pipes_client,
    monkeypatch,
):
    class RecordingSSHPool:
        def __init__(self):
            self.uploads: dict[str, str] = {}

        def upload_file(self, local_path: str, remote_path: str) -> None:
            self.uploads[remote_path] = Path(local_path).read_text(encoding="utf-8")

        def run(self, command: str) -> str:
            if command.startswith("sbatch "):
                return "Submitted batch job 12345"
            return ""

    ssh_pool = RecordingSSHPool()
    monkeypatch.setattr(
        slurm_pipes_client,
        "_wait_for_job_with_streaming",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(slurm_pipes_client, "_store_job_tags", lambda *args: None)
    monkeypatch.setattr(
        slurm_pipes_client,
        "_store_supervisor_heartbeat",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        slurm_pipes_client,
        "_log_estimated_start_time",
        lambda *args: None,
    )

    slurm_pipes_client._execute_standalone(
        execution_plan=ExecutionPlan(
            kind=RuntimeVariant.SHELL,
            payload=["#!/bin/bash", "echo workload"],
            environment={},
            resources={},
        ),
        run_dir="/remote/run",
        ssh_pool=cast(SSHConnectionPool, ssh_pool),
        message_reader=SimpleNamespace(),
        extra_slurm_opts={"signal_before_timeout": "USR1@120"},
    )

    assert "_dagster_slurm_forward_signal" in ssh_pool.uploads["/remote/run/job.sh"]
    assert (
        ssh_pool.uploads["/remote/run/dagster_slurm_workload.sh"]
        == "#!/bin/bash\necho workload"
    )


def test_untrappable_pre_timeout_signal_needs_no_supervisor(slurm_pipes_client):
    assert (
        slurm_pipes_client._build_pre_timeout_supervisor_script(
            "/remote/workload.sh",
            "KILL@120",
            "/remote/run/.dagster_slurm_pre_timeout_signal",
        )
        is None
    )


def test_run_allocation_script_inherits_pre_timeout_signal(monkeypatch):
    session = SlurmSessionResource(
        slurm=_mock_slurm_resource(signal_before_timeout="TERM@120"),
        time_limit="00:10:00",
    )

    allocation_script, _ = _render_allocation_script(
        session,
        monkeypatch,
        run_id="run_with_signal",
        job_id=126,
    )

    assert "#SBATCH --signal=B:TERM@120" in allocation_script


def test_pre_timeout_signal_must_fit_within_walltime():
    with pytest.raises(ValueError, match="must be shorter than time_limit"):
        SlurmQueueConfig(
            time_limit="00:02:00",
            signal_before_timeout="TERM@120",
        )


def test_remote_lock_does_not_steal_stale_locks():
    class FakeSSHPool:
        def __init__(self):
            self.commands: list[str] = []

        def run(self, cmd: str):
            self.commands.append(cmd)
            return "busy"

    fake_ssh_pool = FakeSSHPool()

    acquired = _try_acquire_remote_lock(
        cast(SSHConnectionPool, fake_ssh_pool),
        lock_dir="/remote/.allocation.lock",
        owner="owner",
    )

    assert acquired is False
    assert len(fake_ssh_pool.commands) == 1
    assert "mmin" not in fake_ssh_pool.commands[0]
    assert "rm -rf" not in fake_ssh_pool.commands[0]


def test_allocation_lock_timeout_reports_existing_owner(monkeypatch):
    class FakeSSHPool:
        def __init__(self):
            self.commands: list[str] = []

        def run(self, cmd: str):
            self.commands.append(cmd)
            if "if [ -d" in cmd and ".allocation.lock" in cmd:
                return (
                    "lock_dir=/remote/allocations/dagster_wait/.allocation.lock\n"
                    "owner=1234-5678-owner\n"
                    "mtime=2026-05-20 05:00:00 +0000\n"
                )
            return ""

    session = SlurmSessionResource(slurm=_mock_slurm_resource(remote_base="/remote"))
    fake_ssh_pool = FakeSSHPool()
    object.__setattr__(session, "_ssh_pool", cast(SSHConnectionPool, fake_ssh_pool))
    monkeypatch.setattr(session, "_resolve_run_id", lambda context: "wait")
    monkeypatch.setattr(session_module, "_REMOTE_LOCK_WAIT_TIMEOUT_SECONDS", 0)

    with pytest.raises(TimeoutError) as exc_info:
        session._create_allocation(build_init_resource_context())

    message = str(exc_info.value)
    assert "Timed out waiting for run-scoped Slurm allocation lock" in message
    assert "owner=1234-5678-owner" in message
    assert "mtime=2026-05-20 05:00:00 +0000" in message


def test_run_allocation_scope_rejects_disabled_session_execution():
    slurm = _mock_slurm_resource()
    session = SlurmSessionResource(slurm=slurm)
    client = SlurmPipesClient(
        slurm_resource=slurm,
        launcher=RayLauncher(num_gpus_per_node=0),
        session_resource=session,
        run_allocation_scope=True,
    )
    context = SimpleNamespace(run=SimpleNamespace(run_id="run-scoped"))

    with pytest.raises(ValueError, match="requires use_session=True"):
        client.run(
            context=cast(Any, context),
            payload_path="payload.py",
            use_session=False,
        )


def test_multiplexing_report_is_routed_to_the_asset_log():
    """The pool reports; the client only decides where the message lands."""
    client = SlurmPipesClient(
        slurm_resource=_mock_slurm_resource(),
        launcher=BashLauncher(),
    )
    errors: list[str] = []
    warnings: list[str] = []
    context = SimpleNamespace(
        log=SimpleNamespace(error=errors.append, warning=warnings.append)
    )
    ssh_pool = SimpleNamespace(reporter=None)

    client._attach_multiplexing_reporter(
        cast(Any, context),
        cast(SSHConnectionPool, ssh_pool),
    )
    assert callable(ssh_pool.reporter)

    ssh_pool.reporter("error", "SSH MULTIPLEXING FAILED for example.com")
    ssh_pool.reporter("warning", "SSH multiplexing is unavailable for example.com")

    assert errors == ["SSH MULTIPLEXING FAILED for example.com"]
    assert warnings == ["SSH multiplexing is unavailable for example.com"]


def test_healthy_pool_reports_nothing():
    client = SlurmPipesClient(
        slurm_resource=_mock_slurm_resource(),
        launcher=BashLauncher(),
    )
    errors: list[str] = []
    context = SimpleNamespace(log=SimpleNamespace(error=errors.append))
    ssh_pool = SSHConnectionPool(_mock_slurm_resource().ssh)
    client._attach_multiplexing_reporter(
        cast(Any, context),
        ssh_pool,
    )

    # A pool that never degrades has nothing to describe.
    ssh_pool._fallback_mode = False
    assert ssh_pool.describe_multiplexing() is None
    assert errors == []


def test_attaching_a_reporter_tolerates_test_doubles():
    """Pool stubs without a reporter attribute must not break submission."""
    client = SlurmPipesClient(
        slurm_resource=_mock_slurm_resource(),
        launcher=BashLauncher(),
    )
    context = SimpleNamespace(log=SimpleNamespace(error=lambda _message: None))

    client._attach_multiplexing_reporter(
        cast(Any, context),
        cast(SSHConnectionPool, SimpleNamespace()),
    )


def test_final_log_fallback_shell_quotes_remote_paths():
    slurm = _mock_slurm_resource()
    client = SlurmPipesClient(slurm_resource=slurm, launcher=BashLauncher())
    reader = SSHMessageReader(
        remote_path="/remote/run dir/messages.jsonl",
        ssh_config=slurm.ssh,
    )

    class FakeSSHPool:
        def __init__(self):
            self.commands: list[str] = []

        def run(self, cmd: str):
            self.commands.append(cmd)
            return ""

    ssh_pool = FakeSSHPool()

    client._maybe_emit_final_logs(
        message_reader=reader,
        ssh_pool=cast(SSHConnectionPool, ssh_pool),
        run_dir="/remote/run dir",
        job_id=123,
        stdout_path="/remote/run dir/slurm;123.out",
        stderr_path="/remote/run dir/slurm;123.err",
    )

    # Both files are fetched in a single round trip...
    assert len(ssh_pool.commands) == 1
    command = ssh_pool.commands[0]
    # ...and each path is still shell-quoted, so a space or a ';' in the run
    # directory cannot break out of the command.
    assert "cat '/remote/run dir/slurm;123.out' 2>/dev/null || true" in command
    assert "cat '/remote/run dir/slurm;123.err' 2>/dev/null || true" in command
    assert "; cat /remote" not in command


def test_final_log_fallback_skips_forwarded_lines(capsys: pytest.CaptureFixture[str]):
    slurm = _mock_slurm_resource()
    client = SlurmPipesClient(slurm_resource=slurm, launcher=BashLauncher())
    reader = SSHMessageReader(
        remote_path="/remote/run/messages.jsonl",
        ssh_config=slurm.ssh,
    )
    reader._forwarded_lines = {"stdout": 2, "stderr": 1}
    stdout_path = "/remote/run/slurm-123.out"
    stderr_path = "/remote/run/slurm-123.err"

    class FakeSSHPool:
        def run(self, cmd: str) -> str:
            first_print = cmd.split("; ", maxsplit=1)[0]
            first_marker_path = shlex.split(first_print)[2]
            marker = first_marker_path.removesuffix(stdout_path)
            return "\n".join(
                [
                    f"{marker}{stdout_path}",
                    "already forwarded stdout 1",
                    "already forwarded stdout 2",
                    "remaining stdout 1",
                    "remaining stdout 2",
                    f"{marker}{stderr_path}",
                    "already forwarded stderr",
                    "remaining stderr",
                ]
            )

    client._maybe_emit_final_logs(
        message_reader=reader,
        ssh_pool=cast(SSHConnectionPool, FakeSSHPool()),
        run_dir="/remote/run",
        job_id=123,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
    )

    captured = capsys.readouterr()
    assert captured.out == (
        "[SLURM STDOUT fallback] remaining stdout 1\n"
        "[SLURM STDOUT fallback] remaining stdout 2\n"
    )
    assert captured.err == "[SLURM STDERR fallback] remaining stderr\n"


@pytest.mark.parametrize(
    "auxiliary_script_name",
    [
        "../escape.sh",
        "nested/script.sh",
        "..\\escape.sh",
        "bad name.sh",
        "",
        "..",
    ],
)
def test_slurm_allocation_rejects_unsafe_auxiliary_script_names(
    auxiliary_script_name: str,
):
    class FakeSSHPool:
        def __init__(self):
            self.commands: list[str] = []
            self.writes: list[tuple[str, str]] = []

        def write_file(self, content: str, remote_path: str):
            self.writes.append((remote_path, content))

        def run(self, cmd: str):
            self.commands.append(cmd)
            return ""

    allocation = SlurmAllocation(
        slurm_job_id=42,
        nodes=["c1"],
        working_dir="/remote/session",
        config=SlurmSessionResource(slurm=_mock_slurm_resource()),
    )
    plan = ExecutionPlan(
        kind=RuntimeVariant.SHELL,
        payload=["#!/bin/bash", "echo ok"],
        environment={},
        resources={},
        auxiliary_scripts={auxiliary_script_name: "echo unsafe"},
    )
    fake_ssh_pool = FakeSSHPool()

    with pytest.raises(
        ValueError, match="Unsafe auxiliary script name|non-empty strings"
    ):
        allocation.execute(
            plan,
            asset_key="asset",
            run_dir="/remote/run",
            ssh_pool=cast(SSHConnectionPool, fake_ssh_pool),
        )

    assert fake_ssh_pool.writes == []
    assert fake_ssh_pool.commands == []


def test_slurm_allocation_accepts_safe_auxiliary_script_names():
    class FakeSSHPool:
        def __init__(self):
            self.commands: list[str] = []
            self.writes: list[tuple[str, str]] = []

        def write_file(self, content: str, remote_path: str):
            self.writes.append((remote_path, content))

        def run(self, cmd: str):
            self.commands.append(cmd)
            if cmd.startswith("cat /remote/run/.slurm-step-1.id"):
                return "42.7"
            if cmd.startswith("cat /remote/run/.slurm-step-1.status"):
                return "0"
            return ""

    allocation = SlurmAllocation(
        slurm_job_id=42,
        nodes=["c1"],
        working_dir="/remote/session",
        config=SlurmSessionResource(slurm=_mock_slurm_resource()),
    )
    plan = ExecutionPlan(
        kind=RuntimeVariant.SHELL,
        payload=["#!/bin/bash", "echo ok"],
        environment={},
        resources={},
        auxiliary_scripts={
            "ray_driver.sh": "echo driver",
            "ray_worker-1.sh": "echo worker",
        },
    )
    fake_ssh_pool = FakeSSHPool()

    result = allocation.execute(
        plan,
        asset_key="asset",
        run_dir="/remote/run",
        ssh_pool=cast(SSHConnectionPool, fake_ssh_pool),
    )

    written_paths = [remote_path for remote_path, _content in fake_ssh_pool.writes]
    assert written_paths == [
        "/remote/run/asset_1_asset.sh",
        "/remote/run/ray_driver.sh",
        "/remote/run/ray_worker-1.sh",
    ]
    assert any(
        "srun --overlap --jobid=42 --job-name=asset_1" in command
        and "nohup bash" in command
        for command in fake_ssh_pool.commands
    )
    asset_script = fake_ssh_pool.writes[0][1].splitlines()
    assert asset_script[0] == "#!/bin/bash"
    assert "${SLURM_STEP_ID:?}" in asset_script[1]
    assert result.step_id == "42.7"


def test_slurm_allocation_srun_failure_reports_step_logs():
    class FakeSSHPool:
        def __init__(self):
            self.commands: list[str] = []
            self.writes: list[tuple[str, str]] = []

        def write_file(self, content: str, remote_path: str):
            self.writes.append((remote_path, content))

        def run(self, cmd: str):
            self.commands.append(cmd)
            if cmd.startswith("cat /remote/run/.slurm-step-1.id"):
                return "42.7"
            if cmd.startswith("cat /remote/run/.slurm-step-1.status"):
                return "1"
            if "slurm-42-step-1_asset.out" in cmd:
                return "captured stdout"
            if "slurm-42-step-1_asset.err" in cmd:
                return "captured stderr"
            return ""

    allocation = SlurmAllocation(
        slurm_job_id=42,
        nodes=["c1"],
        working_dir="/remote/session",
        config=SlurmSessionResource(slurm=_mock_slurm_resource()),
    )
    plan = ExecutionPlan(
        kind=RuntimeVariant.SHELL,
        payload=["#!/bin/bash", "exit 1"],
        environment={},
        resources={},
    )
    fake_ssh_pool = FakeSSHPool()

    with pytest.raises(RuntimeError) as exc_info:
        allocation.execute(
            plan,
            asset_key="asset",
            run_dir="/remote/run",
            ssh_pool=cast(SSHConnectionPool, fake_ssh_pool),
        )

    message = str(exc_info.value)
    assert "srun step failed in allocation 42" in message
    assert "stdout_path=/remote/run/slurm-42-step-1_asset.out" in message
    assert "stderr_path=/remote/run/slurm-42-step-1_asset.err" in message
    assert "captured stdout" in message
    assert "captured stderr" in message


def test_slurm_allocation_wait_for_step_reattaches_to_existing_markers(monkeypatch):
    class FakeSSHPool:
        status_reads = 0

        def run(self, cmd: str) -> str:
            if cmd.startswith("cat /remote/run/.step.id"):
                return "42.7"
            if cmd.startswith("cat /remote/run/.step.status"):
                self.status_reads += 1
                return "0" if self.status_reads > 1 else ""
            return ""

    allocation = SlurmAllocation(
        slurm_job_id=42,
        nodes=["c1"],
        working_dir="/remote/session",
        config=SlurmSessionResource(slurm=_mock_slurm_resource()),
    )
    updates: list[SlurmStepExecutionResult] = []
    polls: list[str | None] = []
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)

    result = allocation.wait_for_step(
        SlurmStepExecutionResult(
            job_id=42,
            stdout_path="/remote/run/stdout",
            stderr_path="/remote/run/stderr",
            step_id_path="/remote/run/.step.id",
            status_path="/remote/run/.step.status",
        ),
        ssh_pool=cast(SSHConnectionPool, FakeSSHPool()),
        step_update_callback=updates.append,
        poll_callback=lambda current: polls.append(current.step_id),
        timeout=10,
    )

    assert result.step_id == "42.7"
    assert [update.step_id for update in updates] == ["42.7"]
    assert polls == ["42.7"]


def test_persistent_ray_scripts_use_node_local_temp_dirs_and_hostname_address():
    allocation = SlurmAllocation(
        slurm_job_id=42,
        nodes=["head", "worker"],
        working_dir="/remote/session",
        config=SlurmSessionResource(slurm=_mock_slurm_resource()),
    )
    launcher = RayLauncher(num_gpus_per_node=0, use_head_ip=False)

    head_script = allocation._render_ray_head_script(
        launcher=launcher,
        activation_script="/remote/env/activate.sh",
        ray_dir="/remote/session/ray_cluster",
    )
    worker_script = allocation._render_ray_worker_script(
        launcher=launcher,
        activation_script="/remote/env/activate.sh",
    )

    assert 'head_bind_addr="$head_node_name"' in head_script
    assert 'if [[ "false" == "true" ]]; then' in head_script
    assert '--temp-dir="$RAY_TMP_DIR"' in head_script
    assert 'export RAY_TMPDIR="${RAY_TMP_DIR}"' in head_script
    assert 'export RAY_NODE_IP_ADDRESS="$head_bind_addr"' in head_script
    assert 'rm -rf "$RAY_TMP_DIR"' in head_script

    assert '--temp-dir="$RAY_TMP_DIR"' in worker_script
    assert '--ray-client-server-port="$ray_client_server_port"' in worker_script
    assert 'export RAY_TMPDIR="${RAY_TMP_DIR}"' in worker_script
    assert 'rm -rf "$RAY_TMP_DIR"' in worker_script


def test_run_allocation_session_creation_is_thread_safe(monkeypatch):
    setup_calls: list[int] = []

    def fake_setup(self, context):
        setup_calls.append(id(self))
        time.sleep(0.05)
        object.__setattr__(self, "_initialized", True)

    monkeypatch.setattr(SlurmSessionResource, "setup_for_execution", fake_setup)

    compute = ComputeResource(
        mode=ExecutionMode.SLURM,
        slurm=_mock_slurm_resource(),
        default_launcher=RayLauncher(num_gpus_per_node=0),
        allocation_scope=SlurmAllocationScope.RUN,
    )
    context = build_init_resource_context()
    barrier = threading.Barrier(8)

    def get_client_session_id() -> int:
        barrier.wait()
        client = compute.get_pipes_client(context)
        return id(client.session)

    with ThreadPoolExecutor(max_workers=8) as executor:
        session_ids = list(executor.map(lambda _: get_client_session_id(), range(8)))

    assert len(set(session_ids)) == 1
    assert len(setup_calls) == 1


def test_session_teardown_preserves_allocation_for_reattach(monkeypatch):
    session = SlurmSessionResource(slurm=_mock_slurm_resource())
    allocation = SimpleNamespace(slurm_job_id=42, cancel=lambda _pool: None)
    ssh_pool = SimpleNamespace(__exit__=lambda *_args: None)
    cleanup_calls: list[int] = []
    monkeypatch.setattr(
        session,
        "_schedule_shared_allocation_cleanup",
        lambda: cleanup_calls.append(42),
    )
    object.__setattr__(session, "_allocation", allocation)
    object.__setattr__(session, "_ssh_pool", ssh_pool)
    object.__setattr__(session, "_initialized", True)
    object.__setattr__(session, "_shared_lifecycle", True)
    object.__setattr__(session, "_preserve_allocation_on_teardown", True)

    session.teardown_after_execution(build_init_resource_context())

    assert cleanup_calls == []


class LocalSlurmFakeSSHPool:
    def __init__(self, *, first_job_id: int = 700):
        self.commands: list[str] = []
        self.writes: list[tuple[str, str]] = []
        self.submitted_jobs: list[int] = []
        self._next_job_id = first_job_id
        self._lock = threading.RLock()

    def write_file(self, content: str, remote_path: str):
        with self._lock:
            self.writes.append((remote_path, content))
            path = Path(remote_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")

    def run(self, cmd: str, timeout: int | None = None) -> str:
        with self._lock:
            self.commands.append(cmd)
            if cmd.startswith("sbatch "):
                script_path = Path(shlex.split(cmd)[-1])
                working_dir = script_path.parent
                job_id = self._next_job_id
                self._next_job_id += 1
                self.submitted_jobs.append(job_id)
                (working_dir / "head_node.txt").write_text("node-a\n", encoding="utf-8")
                (working_dir / "nodes.txt").write_text(
                    "node-a\nnode-b\n", encoding="utf-8"
                )
                return f"Submitted batch job {job_id}\n"
            if cmd.startswith("squeue -h -j "):
                return "RUNNING\n"
            if cmd.startswith("sacct "):
                return ""

            result = subprocess.run(
                ["bash", "-lc", cmd],
                capture_output=True,
                check=False,
                text=True,
                timeout=timeout,
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f"local fake SSH command failed: {cmd}\n"
                    f"stdout: {result.stdout}\nstderr: {result.stderr}"
                )
            return result.stdout


def test_run_allocation_attaches_to_existing_remote_session(tmp_path: Path):
    ssh_pool = LocalSlurmFakeSSHPool()
    context = SimpleNamespace(run=SimpleNamespace(run_id="shared-run"))

    first_session = SlurmSessionResource(
        slurm=_mock_slurm_resource(remote_base=str(tmp_path)),
        num_nodes=2,
    )
    second_session = SlurmSessionResource(
        slurm=_mock_slurm_resource(remote_base=str(tmp_path)),
        num_nodes=2,
    )
    object.__setattr__(first_session, "_ssh_pool", cast(SSHConnectionPool, ssh_pool))
    object.__setattr__(second_session, "_ssh_pool", cast(SSHConnectionPool, ssh_pool))

    first_allocation = first_session._create_allocation(context)
    second_allocation = second_session._create_allocation(context)

    assert first_allocation.slurm_job_id == second_allocation.slurm_job_id
    assert first_allocation.nodes == second_allocation.nodes
    assert ssh_pool.submitted_jobs == [700]


def test_run_allocation_retry_reattaches_to_tagged_session(tmp_path: Path):
    ssh_pool = LocalSlurmFakeSSHPool()
    original_context = SimpleNamespace(
        run=SimpleNamespace(run_id="original-run", tags={})
    )
    original_session = SlurmSessionResource(
        slurm=_mock_slurm_resource(remote_base=str(tmp_path)),
        num_nodes=2,
    )
    object.__setattr__(
        original_session,
        "_ssh_pool",
        cast(SSHConnectionPool, ssh_pool),
    )
    original = original_session._create_allocation(original_context)

    retry_session = SlurmSessionResource(
        slurm=_mock_slurm_resource(remote_base=str(tmp_path)),
        num_nodes=2,
    )
    object.__setattr__(retry_session, "_ssh_pool", cast(SSHConnectionPool, ssh_pool))
    retry_context = SimpleNamespace(
        run=SimpleNamespace(
            run_id="retry-run",
            tags={"dagster_slurm/session_allocation_dir": original.working_dir},
        )
    )

    reattached = retry_session._create_allocation(retry_context)

    assert reattached.slurm_job_id == original.slurm_job_id
    assert reattached.working_dir == original.working_dir
    assert ssh_pool.submitted_jobs == [700]


def test_persistent_ray_cluster_start_is_remote_lock_safe(
    tmp_path: Path,
    monkeypatch,
):
    ssh_pool = LocalSlurmFakeSSHPool()
    working_dir = tmp_path / "allocation"
    starts: list[int] = []

    def fake_start_ray_cluster(
        self,
        *,
        ssh_pool,
        launcher,
        activation_script,
        startup_timeout,
    ):
        starts.append(id(self))
        ray_dir = Path(self.working_dir) / "ray_cluster"
        ray_dir.mkdir(parents=True, exist_ok=True)
        time.sleep(0.1)
        (ray_dir / "ray_address").write_text("node-a:6379\n", encoding="utf-8")
        (ray_dir / "ray_ready").touch()
        return "node-a:6379"

    monkeypatch.setattr(
        SlurmAllocation,
        "_start_ray_cluster",
        fake_start_ray_cluster,
    )

    launcher = RayLauncher(num_gpus_per_node=0)
    barrier = threading.Barrier(2)

    def ensure_from_new_process_model() -> str:
        allocation = SlurmAllocation(
            slurm_job_id=701,
            nodes=["node-a", "node-b"],
            working_dir=str(working_dir),
            config=SlurmSessionResource(slurm=_mock_slurm_resource()),
        )
        barrier.wait()
        return allocation.ensure_ray_cluster(
            ssh_pool=cast(SSHConnectionPool, ssh_pool),
            launcher=launcher,
            activation_script="/remote/env/activate.sh",
            startup_timeout=10,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        addresses = list(
            executor.map(lambda _: ensure_from_new_process_model(), range(2))
        )

    assert addresses == ["node-a:6379", "node-a:6379"]
    assert len(starts) == 1


def _run_pre_timeout_supervisor(
    slurm_pipes_client,
    tmp_path: Path,
    workload_body: list[str],
    *,
    send_signal: bool,
) -> tuple[int, str | None]:
    """Run the supervisor around a workload and report (exit code, marker)."""
    ready_path = tmp_path / "ready"
    workload_path = tmp_path / "workload.sh"
    marker_path = tmp_path / "signal_marker"
    workload_path.write_text(
        "\n".join(
            ["#!/bin/bash", "set -uo pipefail", f"touch {shlex.quote(str(ready_path))}"]
            + workload_body
        ),
        encoding="utf-8",
    )
    supervisor = slurm_pipes_client._build_pre_timeout_supervisor_script(
        str(workload_path), "USR1@120", str(marker_path)
    )
    assert supervisor is not None
    supervisor_path = tmp_path / "supervisor.sh"
    supervisor_path.write_text(supervisor, encoding="utf-8")

    process = subprocess.Popen(["bash", str(supervisor_path)])
    deadline = time.monotonic() + 5
    while not ready_path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert ready_path.exists()

    if send_signal:
        os.kill(process.pid, signal.SIGUSR1)

    return_code = process.wait(timeout=10)
    marker = (
        marker_path.read_text(encoding="utf-8").strip()
        if marker_path.exists()
        else None
    )
    return return_code, marker


def test_workload_finishing_after_the_signal_is_not_a_failure(
    slurm_pipes_client, tmp_path: Path
):
    """A job that completes all its work inside the walltime margin succeeded.

    Slurm may deliver the pre-timeout signal up to 60s earlier than requested,
    so a job sized close to its walltime routinely finishes after being warned.
    Forcing a failure there discarded complete, materialised runs.
    """
    return_code, marker = _run_pre_timeout_supervisor(
        slurm_pipes_client,
        tmp_path,
        ['trap "" USR1', "sleep 0.4", "exit 0"],
        send_signal=True,
    )

    assert return_code == 0
    assert marker == "USR1"


def test_workload_exit_code_survives_the_signal(slurm_pipes_client, tmp_path: Path):
    """A specific failure code must not be replaced by 128+signum."""
    return_code, marker = _run_pre_timeout_supervisor(
        slurm_pipes_client,
        tmp_path,
        ['trap "exit 3" USR1', "sleep 5 & wait"],
        send_signal=True,
    )

    assert return_code == 3
    assert marker == "USR1"


def test_workload_immediate_signal_exit_is_reaped(slurm_pipes_client, tmp_path: Path):
    """A fast signal handler must not expose the interrupted wait status."""
    return_code, marker = _run_pre_timeout_supervisor(
        slurm_pipes_client,
        tmp_path,
        ['trap "exit 0" USR1', "while true; do sleep 0.01; done"],
        send_signal=True,
    )

    assert return_code == 0
    assert marker == "USR1"


def test_workload_ignoring_the_signal_still_fails(slurm_pipes_client, tmp_path: Path):
    """An unhandled signal kills the workload, which stays a failure."""
    return_code, marker = _run_pre_timeout_supervisor(
        slurm_pipes_client,
        tmp_path,
        ["sleep 30"],
        send_signal=True,
    )

    assert return_code == 128 + signal.SIGUSR1
    assert marker == "USR1"


def test_no_marker_is_written_without_a_signal(slurm_pipes_client, tmp_path: Path):
    return_code, marker = _run_pre_timeout_supervisor(
        slurm_pipes_client,
        tmp_path,
        ["exit 0"],
        send_signal=False,
    )

    assert return_code == 0
    assert marker is None


def test_pre_timeout_marker_is_reported_on_failure(slurm_pipes_client):
    """A timed-out job should say so instead of just "did not complete"."""
    pool = SimpleNamespace(run=lambda cmd, timeout=None: "TERM\n")

    assert (
        slurm_pipes_client._read_pre_timeout_signal(
            cast(SSHConnectionPool, pool), "/remote/run"
        )
        == "TERM"
    )

    empty_pool = SimpleNamespace(run=lambda cmd, timeout=None: "")
    assert (
        slurm_pipes_client._read_pre_timeout_signal(
            cast(SSHConnectionPool, empty_pool), "/remote/run"
        )
        is None
    )


def test_status_poll_never_sleeps_past_the_deadline():
    """A job that finishes just inside poll_timeout must be seen, not timed out."""
    bounded = SlurmPipesClient._bounded_poll_sleep

    # Plenty of budget left: the backed-off interval is used as-is.
    assert bounded(5.0, elapsed=10.0, poll_timeout=60.0) == 5.0

    # Close to the deadline: the sleep shrinks so one more poll still happens
    # before poll_timeout, instead of stepping straight over it.
    assert bounded(5.0, elapsed=58.0, poll_timeout=60.0) == 1.75
    assert bounded(15.0, elapsed=50.0, poll_timeout=60.0) == 9.75

    # Never sleeps a negative or silly-small amount.
    assert bounded(5.0, elapsed=59.9, poll_timeout=60.0) == 0.1
    assert bounded(5.0, elapsed=61.0, poll_timeout=60.0) == 0.0


def test_status_poll_cap_stays_responsive_by_default():
    """The default cap has to survive short jobs, not just multi-hour ones."""
    slurm = SlurmResource(
        ssh=SSHConnectionResource(host="h", user="u", password="p"),
        queue=SlurmQueueConfig(),
    )

    interval = slurm.status_poll_interval_seconds
    elapsed = 0.0
    for _ in range(40):
        elapsed += interval
        interval = slurm.next_status_poll_interval(interval)

    # A four-hour job still drops from 14400 squeue calls to under 3000, while
    # completion is never noticed more than the cap late.
    assert slurm.status_poll_max_interval_seconds <= 5.0
    assert interval == slurm.status_poll_max_interval_seconds


def test_status_poll_interval_backs_off_and_is_capped():
    slurm = SlurmResource(
        ssh=SSHConnectionResource(host="h", user="u", password="p"),
        queue=SlurmQueueConfig(),
        status_poll_interval_seconds=2.0,
        status_poll_max_interval_seconds=8.0,
        status_poll_backoff_factor=2.0,
    )

    assert slurm.next_status_poll_interval(2.0) == 4.0
    assert slurm.next_status_poll_interval(4.0) == 8.0
    assert slurm.next_status_poll_interval(8.0) == 8.0


def test_status_poll_max_interval_must_not_be_below_the_floor():
    with pytest.raises(Exception, match="status_poll_max_interval_seconds"):
        SlurmResource(
            ssh=SSHConnectionResource(host="h", user="u", password="p"),
            queue=SlurmQueueConfig(),
            status_poll_interval_seconds=10.0,
            status_poll_max_interval_seconds=5.0,
        )


def test_status_poll_cadence_reads_environment(monkeypatch):
    monkeypatch.setenv("SLURM_SSH_HOST", "example.com")
    monkeypatch.setenv("SLURM_SSH_USER", "testuser")
    monkeypatch.setenv("SLURM_SSH_PASSWORD", "secret")
    monkeypatch.setenv("SLURM_STATUS_POLL_INTERVAL", "5")
    monkeypatch.setenv("SLURM_STATUS_POLL_MAX_INTERVAL", "30")
    monkeypatch.setenv("SLURM_STATUS_POLL_BACKOFF", "2")

    slurm = SlurmResource.from_env()

    assert slurm.status_poll_interval_seconds == 5.0
    assert slurm.status_poll_max_interval_seconds == 30.0
    assert slurm.status_poll_backoff_factor == 2.0


def test_multiplexing_loss_mid_run_is_reported_to_dagster():
    """A master lost mid-run escalates through the same reporter as a failed start."""
    client = SlurmPipesClient(
        slurm_resource=_mock_slurm_resource(),
        launcher=BashLauncher(),
    )
    errors: list[str] = []
    context = SimpleNamespace(log=SimpleNamespace(error=errors.append))
    ssh_pool = SimpleNamespace(reporter=None)

    client._attach_multiplexing_reporter(
        cast(Any, context),
        cast(SSHConnectionPool, ssh_pool),
    )

    assert errors == []
    assert callable(ssh_pool.reporter)

    ssh_pool.reporter("error", "SSH MULTIPLEXING FAILED for example.com")
    assert errors == ["SSH MULTIPLEXING FAILED for example.com"]


def test_pipes_closed_detection_ignores_readers_without_the_signal():
    """A payload that never opens a Pipes session keeps the normal cadence."""
    client = SlurmPipesClient(
        slurm_resource=_mock_slurm_resource(),
        launcher=BashLauncher(),
    )

    assert client._pipes_session_closed(SimpleNamespace()) is False
    assert client._pipes_session_closed(SimpleNamespace(closed_message=None)) is False
    assert (
        client._pipes_session_closed(
            SimpleNamespace(closed_message={"method": "closed", "params": {}})
        )
        is True
    )


def test_status_polling_stops_backing_off_once_pipes_reports_closed(monkeypatch):
    """The 'closed' message means the job is about to end - stop backing off.

    Slurm reflects the exit a beat after the payload does, so continuing to
    grow the interval only adds latency to a transition we already know is
    imminent.
    """
    slurm = _mock_slurm_resource()
    client = SlurmPipesClient(slurm_resource=slurm, launcher=BashLauncher())

    states = iter(["RUNNING", "RUNNING", "RUNNING", "RUNNING", "COMPLETED"])
    monkeypatch.setattr(client, "_get_job_state", lambda *_a, **_k: next(states))
    monkeypatch.setattr(client, "_store_supervisor_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(client, "_is_run_canceling", lambda *a, **k: False)
    monkeypatch.setattr(client, "_maybe_emit_final_logs", lambda *a, **k: None)

    sleeps: list[float] = []
    monkeypatch.setattr(
        client, "_interruptible_sleep", lambda seconds, job_id: sleeps.append(seconds)
    )
    # Stream-drain sampling also sleeps; it is not what this test measures.
    monkeypatch.setattr(client, "_await_stream_quiescence", lambda *a, **k: None)

    # The payload reports 'closed' after the third status poll.
    class Reader:
        polls = 0
        closed_message = None

        def __getattribute__(self, name):
            if name == "closed_message" and Reader.polls >= 3:
                return {"method": "closed", "params": {}}
            return object.__getattribute__(self, name)

    reader = Reader()

    real_state = client._get_job_state

    def counting_state(*a, **k):
        Reader.polls += 1
        return real_state(*a, **k)

    monkeypatch.setattr(client, "_get_job_state", counting_state)
    monkeypatch.setattr(
        "dagster_slurm.pipes_clients.slurm_pipes_client.threading.Thread",
        lambda *a, **k: SimpleNamespace(
            start=lambda: None, join=lambda timeout=None: None, is_alive=lambda: False
        ),
    )

    client._wait_for_job_with_streaming(
        1,
        cast(Any, SimpleNamespace(run=lambda *a, **k: "")),
        "/remote/run",
        message_reader=reader,
        poll_timeout=600,
    )

    floor = slurm.status_poll_interval_seconds
    # It backed off at some point while the job was just running...
    assert max(sleeps) > floor, sleeps
    # ...and every wait after 'closed' arrived is back at the floor.
    assert all(s == pytest.approx(floor) for s in sleeps[-2:]), sleeps
