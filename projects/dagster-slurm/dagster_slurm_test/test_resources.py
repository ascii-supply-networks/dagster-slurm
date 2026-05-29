"""Tests for resources."""

import shlex
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
    _try_acquire_remote_lock,
)


def _mock_slurm_resource(
    *,
    gpus_per_node: int = 0,
    remote_base: str = "/tmp/dagster_test",
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
        ),
        remote_base=remote_base,
    )


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

    first = allocation.execute(
        plan,
        asset_key="asset/one",
        run_dir="/remote/run/one",
        ssh_pool=ssh_pool,
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
    assert first.stdout_path in fake_ssh_pool.commands[1]
    assert second.stdout_path in fake_ssh_pool.commands[3]


def test_slurm_session_allocation_honors_zero_gpu_override(monkeypatch):
    class FakeSSHPool:
        def __init__(self):
            self.commands: list[str] = []
            self.writes: list[tuple[str, str]] = []

        def write_file(self, content: str, remote_path: str):
            self.writes.append((remote_path, content))

        def run(self, cmd: str):
            self.commands.append(cmd)
            if ".allocation.lock" in cmd and "printf acquired" in cmd:
                return "acquired"
            if cmd.startswith("sbatch "):
                return "Submitted batch job 123"
            if cmd.startswith("cat ") and "nodes.txt" in cmd:
                return "c1\n"
            return ""

    session = SlurmSessionResource(
        slurm=_mock_slurm_resource(gpus_per_node=4),
        gpus_per_node=0,
    )
    fake_ssh_pool = FakeSSHPool()
    object.__setattr__(session, "_ssh_pool", cast(SSHConnectionPool, fake_ssh_pool))
    monkeypatch.setattr(session, "_resolve_run_id", lambda context: "run_with_no_gpus")
    monkeypatch.setattr(session, "_log_estimated_start_time", lambda job_id: None)
    monkeypatch.setattr(
        session,
        "_wait_for_allocation_start",
        lambda job_id, working_dir, timeout: None,
    )

    allocation = session._create_allocation(build_init_resource_context())

    assert allocation.slurm_job_id == 123
    assert fake_ssh_pool.writes
    allocation_script = fake_ssh_pool.writes[0][1]
    assert "#SBATCH --gres=gpu:" not in allocation_script


def test_slurm_session_allocation_inherits_queue_gpu_default(monkeypatch):
    class FakeSSHPool:
        def __init__(self):
            self.commands: list[str] = []
            self.writes: list[tuple[str, str]] = []

        def write_file(self, content: str, remote_path: str):
            self.writes.append((remote_path, content))

        def run(self, cmd: str):
            self.commands.append(cmd)
            if ".allocation.lock" in cmd and "printf acquired" in cmd:
                return "acquired"
            if cmd.startswith("sbatch "):
                return "Submitted batch job 124"
            if cmd.startswith("cat ") and "nodes.txt" in cmd:
                return "c1\n"
            return ""

    session = SlurmSessionResource(slurm=_mock_slurm_resource(gpus_per_node=4))
    fake_ssh_pool = FakeSSHPool()
    object.__setattr__(session, "_ssh_pool", cast(SSHConnectionPool, fake_ssh_pool))
    monkeypatch.setattr(session, "_resolve_run_id", lambda context: "run_with_gpus")
    monkeypatch.setattr(session, "_log_estimated_start_time", lambda job_id: None)
    monkeypatch.setattr(
        session,
        "_wait_for_allocation_start",
        lambda job_id, working_dir, timeout: None,
    )

    allocation = session._create_allocation(build_init_resource_context())

    assert allocation.slurm_job_id == 124
    assert fake_ssh_pool.writes
    allocation_script = fake_ssh_pool.writes[0][1]
    assert "#SBATCH --gres=gpu:4" in allocation_script


def test_slurm_session_allocation_quotes_remote_working_dir_paths(monkeypatch):
    class FakeSSHPool:
        def __init__(self):
            self.commands: list[str] = []
            self.writes: list[tuple[str, str]] = []

        def write_file(self, content: str, remote_path: str):
            self.writes.append((remote_path, content))

        def run(self, cmd: str):
            self.commands.append(cmd)
            if ".allocation.lock" in cmd and "printf acquired" in cmd:
                return "acquired"
            if cmd.startswith("sbatch "):
                return "Submitted batch job 125"
            if cmd.startswith("cat ") and "nodes.txt" in cmd:
                return "c1\n"
            return ""

    remote_base = "/remote/base dir;touch pwned"
    run_id = "run_with_spaces"
    expected_working_dir = f"{remote_base}/allocations/dagster_{run_id}"
    session = SlurmSessionResource(slurm=_mock_slurm_resource(remote_base=remote_base))
    fake_ssh_pool = FakeSSHPool()
    object.__setattr__(session, "_ssh_pool", cast(SSHConnectionPool, fake_ssh_pool))
    monkeypatch.setattr(session, "_resolve_run_id", lambda context: run_id)
    monkeypatch.setattr(session, "_log_estimated_start_time", lambda job_id: None)
    monkeypatch.setattr(
        session,
        "_wait_for_allocation_start",
        lambda job_id, working_dir, timeout: None,
    )

    allocation = session._create_allocation(build_init_resource_context())

    assert allocation.slurm_job_id == 125
    allocation_script = fake_ssh_pool.writes[0][1]
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

    assert ssh_pool.commands == [
        "cat '/remote/run dir/slurm;123.out' 2>/dev/null || true",
        "cat '/remote/run dir/slurm;123.err' 2>/dev/null || true",
    ]


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

    allocation.execute(
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
    assert fake_ssh_pool.commands[-1].startswith(
        "srun --overlap --jobid=42 --job-name=asset_1"
    )


def test_slurm_allocation_srun_failure_reports_step_logs():
    class FakeSSHPool:
        def __init__(self):
            self.commands: list[str] = []
            self.writes: list[tuple[str, str]] = []

        def write_file(self, content: str, remote_path: str):
            self.writes.append((remote_path, content))

        def run(self, cmd: str):
            self.commands.append(cmd)
            if cmd.startswith("srun --overlap"):
                raise RuntimeError("ssh command failed")
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
    assert "srun step 1 failed in allocation 42" in message
    assert "stdout_path=/remote/run/slurm-42-step-1_asset.out" in message
    assert "stderr_path=/remote/run/slurm-42-step-1_asset.err" in message
    assert "captured stdout" in message
    assert "captured stderr" in message


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
