"""Elastic worker allocations that join a run's Ray cluster."""

import json
import os
import shlex
import shutil
import signal
import subprocess
import sys
import textwrap
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest

from dagster_slurm import (
    ComputeResource,
    RayLauncher,
    RayPortConfig,
    SlurmAllocationEnded,
    SlurmQueueConfig,
    SlurmResource,
    SlurmRunAllocationConfig,
    SlurmSessionResource,
    SlurmStepExecutionResult,
    SlurmWorkerAllocationConfig,
)
from dagster_slurm.helpers import ray_worker_monitor
from dagster_slurm.helpers.ssh_helpers import TERMINAL_STATES
from dagster_slurm.helpers.ssh_pool import SSHConnectionPool
from dagster_slurm.launchers.base import ExecutionPlan
from dagster_slurm.config.runtime import RuntimeVariant
import dagster_slurm.resources.session as session_module
from dagster_slurm.resources.session import SlurmAllocation
from dagster_slurm.sensors import reconcile_orphaned_slurm_runs
from dagster_slurm_test.test_resources import _mock_slurm_resource
from dagster_slurm_test.test_session_relay import RelayPool, make_session

NODE_ID = "f" * 56
NODE_IDS = {"node-a": NODE_ID, "node-b": "e" * 56}
MONITOR_SOURCE = Path(ray_worker_monitor.__file__)
needs_procfs = pytest.mark.skipif(
    not Path("/proc/self/stat").exists(), reason="the drain monitor reads /proc"
)


class StartingPool(RelayPool):
    """Every submitted job starts at once."""

    def run(self, cmd: str, timeout: int | None = None) -> str:
        output = super().run(cmd, timeout)
        if cmd.startswith("sbatch "):
            self.states[self.submitted_jobs[-1]] = "RUNNING"
        return output


class ElectionPool(StartingPool):
    """Also answers time-left queries: ten hours unless a test sets one."""

    def __init__(self):
        super().__init__()
        self.time_left: dict[int, str] = {}

    def run(self, cmd: str, timeout: int | None = None) -> str:
        if cmd.startswith("squeue ") and "%L" in cmd:
            self.commands.append(cmd)
            args = shlex.split(cmd)
            return self.time_left.get(int(args[args.index("-j") + 1]), "10:00:00")
        if cmd.startswith("scancel ") and "." in cmd.split()[-1]:
            self.commands.append(cmd)  # Cancelling one step leaves the job alone.
            return ""
        return super().run(cmd, timeout)


def _last_batch_script(pool: RelayPool) -> str:
    return [content for path, content in pool.writes if path.endswith(".sh")][-1]


def test_worker_inherits_head_shape_and_joins_its_ray_head(tmp_path):
    pool = RelayPool()
    session = SlurmSessionResource(
        slurm=_mock_slurm_resource(remote_base=str(tmp_path)),
        num_nodes=1,
        gpus_per_node=0,
        mem="3G",
        nodelist="cpu-01",
        extra_sbatch_directives=["--comment=head"],
        signal_before_timeout="TERM@120",
        time_limit="00:10:00",
        enable_health_checks=False,
    )
    object.__setattr__(session, "_ssh_pool", cast(SSHConnectionPool, pool))
    context = SimpleNamespace(run=SimpleNamespace(run_id="elastic", tags={}))
    object.__setattr__(session, "_allocation", session._create_allocation(context))
    head = session.allocation

    worker = session.add_worker_allocation(
        SlurmWorkerAllocationConfig(
            partition="GPU-a40",
            gpus_per_node=2,
            mem_per_cpu="4G",
            extra_sbatch_directives=["--begin=now+10minutes"],
            ray_resources={"accelerator:a40": 1, "vram_48gb": 1},
            ray_start_args=["--num-cpus=8"],
        )
    )

    # Submission does not wait for the job to start.
    assert worker.slurm_job_id == 701 and worker.nodes == []
    assert worker.working_dir == f"{head.session_dir}/jobs/701"
    script = _last_batch_script(pool)
    for line in (
        "#SBATCH --job-name=dagster_elastic",
        "#SBATCH --time=00:10:00",
        "#SBATCH --signal=B:TERM@120",
        "#SBATCH --partition=GPU-a40",
        "#SBATCH --gres=gpu:2",
        "#SBATCH --mem-per-cpu=4G",
        "#SBATCH --begin=now+10minutes",
        f"session_dir={head.session_dir}",
        "fallback_head=700",
        "rejoin_timeout=600",
        'ray_args=(--num-gpus=2 \'--resources={"accelerator:a40": 1, '
        '"dagster_slurm_worker": 1, "vram_48gb": 1}\' --num-cpus=8)',
    ):
        assert line in script
    for inherited_only_by_head in ("--nodelist", "--mem=", "--comment=head"):
        assert inherited_only_by_head not in script
    subprocess.run(["bash", "-n"], input=script, text=True, check=True)

    metadata = json.loads((Path(head.session_dir) / "allocation.json").read_text())
    assert metadata["slurm_job_id"] == 700
    [record] = metadata["workers"]
    assert record["slurm_job_id"] == 701
    assert record["rejoin_timeout"] == 600
    assert record["config"]["partition"] == "GPU-a40"
    assert record["config"]["nodelist"] is None
    assert (Path(head.session_dir) / "ray_head").read_text() == "700\n"


def test_worker_defaults_inherit_head_and_explicit_mem_beats_queue_default(tmp_path):
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    session.add_worker_allocation()
    script = _last_batch_script(pool)
    assert (
        "ray_args=(--num-gpus=0 '--resources={\"dagster_slurm_worker\": 1}')" in script
    )
    assert "#SBATCH --mem=1G" in script  # The queue default.

    session.add_worker_allocation(SlurmWorkerAllocationConfig(mem_per_cpu="2G"))
    script = _last_batch_script(pool)
    assert "#SBATCH --mem-per-cpu=2G" in script
    assert "#SBATCH --mem=" not in script


@pytest.mark.parametrize(
    "overrides",
    [
        {"ray_resources": {"GPU": 1}},
        {"ray_resources": {"node:10.0.0.1": 1}},
        {"ray_resources": {"accelerator:a40": -1}},
        {"ray_resources": {"": 1}},
        {"ray_start_args": ["--address=10.0.0.1:6379"]},
        {"ray_start_args": ["--head"]},
        {"ray_start_args": ["--resources={}"]},
        {"ray_start_args": ["--temp-dir=/tmp/x"]},
        {"ray_start_args": ["num-cpus=1"]},
        {"extra_sbatch_directives": ["--nodes=4"]},
    ],
)
def test_invalid_worker_config_is_rejected(overrides):
    with pytest.raises(ValueError):
        SlurmWorkerAllocationConfig(**overrides)


def test_remove_drains_running_worker_and_cancels_pending_one(tmp_path):
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    running = session.add_worker_allocation()
    pending = session.add_worker_allocation()
    pool.states[running.slurm_job_id] = "RUNNING"
    Path(running.working_dir).mkdir(parents=True, exist_ok=True)

    assert session.remove_worker_allocation(running, drain_timeout=90) == "CANCELLED"
    assert (Path(running.working_dir) / "drain_timeout").read_text() == "90\n"
    assert f"scancel --batch --signal=TERM {running.slurm_job_id}" in pool.commands

    assert session.remove_worker_allocation(pending) == "CANCELLED"
    assert f"scancel {pending.slurm_job_id}" in pool.commands
    assert f"scancel --batch --signal=TERM {pending.slurm_job_id}" not in pool.commands

    metadata = json.loads(
        (Path(running.session_dir) / "allocation.json").read_text(encoding="utf-8")
    )
    assert metadata["workers"] == []
    assert pool.states[700] == "RUNNING"
    with pytest.raises(ValueError):
        session.remove_worker_allocation(running, drain_timeout=0)


def test_remove_cancels_worker_that_does_not_release(tmp_path, monkeypatch):
    class StuckPool(RelayPool):
        def run(self, cmd, timeout=None):
            if cmd.startswith("scancel --batch"):
                self.commands.append(cmd)
                return ""
            return super().run(cmd, timeout)

    pool = StuckPool()
    session = make_session(tmp_path, pool)
    worker = session.add_worker_allocation()
    pool.states[worker.slurm_job_id] = "RUNNING"
    monkeypatch.setattr(session_module, "_WORKER_RELEASE_GRACE_SECONDS", 0)
    monkeypatch.setattr(session_module.time, "sleep", lambda seconds: None)

    assert session.remove_worker_allocation(worker, drain_timeout=1) == "CANCELLED"
    assert pool.commands.index(
        f"scancel --batch --signal=TERM {worker.slurm_job_id}"
    ) < pool.commands.index(f"scancel {worker.slurm_job_id}")


def test_restart_finds_workers_and_promotion_keeps_them(tmp_path, monkeypatch):
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    worker = session.add_worker_allocation()
    ended = session.add_worker_allocation()
    pool.states[ended.slurm_job_id] = "COMPLETED"
    pool.states[worker.slurm_job_id] = "RUNNING"

    restarted = make_session(tmp_path, pool)
    [found] = restarted.worker_allocations
    assert found.slurm_job_id == worker.slurm_job_id
    assert found.nodes == ["node-a", "node-b"]
    assert found.config.time_limit == "00:10:00"

    successor = restarted.submit_successor()
    pool.states[successor.slurm_job_id] = "RUNNING"
    object.__setattr__(
        restarted,
        "_context",
        SimpleNamespace(run=SimpleNamespace(run_id="relay"), instance=MagicMock()),
    )
    monkeypatch.setattr(SlurmAllocation, "_read_ray_start_options", lambda *a: None)
    restarted.promote_successor()
    # Workers follow the head pointer to the successor instead of ending.
    assert pool.states[worker.slurm_job_id] == "RUNNING"
    assert pool.states[700] == "CANCELLED"
    assert [found.slurm_job_id for found in restarted.worker_allocations] == [
        worker.slurm_job_id
    ]
    pointer = Path(worker.session_dir) / "ray_head"
    assert pointer.read_text() == f"{successor.slurm_job_id}\n"
    metadata = json.loads((Path(worker.session_dir) / "allocation.json").read_text())
    assert metadata["slurm_job_id"] == successor.slurm_job_id
    assert [record["slurm_job_id"] for record in metadata["workers"]] == [
        worker.slurm_job_id,
        ended.slurm_job_id,
    ]


def test_teardown_cancels_workers_with_the_session_job_name(tmp_path):
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    worker = session.add_worker_allocation()
    object.__setattr__(session, "_initialized", True)
    object.__setattr__(session, "_owns_allocation", True)
    session.teardown_after_execution(cast(Any, None))
    assert "scancel --name=dagster_relay" in pool.commands
    assert pool.states[worker.slurm_job_id] == "CANCELLED"


def test_ray_node_ids_tolerate_password_ssh_output(tmp_path):
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    noisy = MagicMock()
    noisy.run.return_value = " \r\nc2 abc\r\n\r\n"
    object.__setattr__(session, "_ssh_pool", noisy)
    assert session.allocation.ray_node_ids == {"c2": "abc"}


@pytest.mark.parametrize(
    "signal_before_timeout,seconds", [(None, 60), ("TERM@120", 105), ("USR1@10", 1)]
)
def test_walltime_drain_window_ends_before_the_walltime(signal_before_timeout, seconds):
    session = SlurmSessionResource(
        slurm=_mock_slurm_resource(),
        time_limit="00:10:00",
        signal_before_timeout=signal_before_timeout,
    )
    assert session._worker_drain_seconds() == seconds


def test_persistent_worker_script_takes_worker_arguments(tmp_path):
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    script = session.allocation._render_ray_worker_script(
        launcher=RayLauncher(num_gpus_per_node=1, object_store_memory_gb=2),
        activation_script="/env/activate.sh",
    )
    assert 'ray_address="$1"\n\t# Further arguments' in script
    assert '--object-store-memory=2000000000 $worker_cpus_arg "$@" --block &' in script
    assert 'worker_cpus_arg="--num-cpus=$SLURM_CPUS_ON_NODE"' in script
    assert f"{session.allocation.working_dir}/ray_cluster/ray_worker_monitor.py" in (
        script
    )
    subprocess.run(["bash", "-n"], input=script, text=True, check=True)


def _write_executable(path: Path, content: str) -> None:
    path.write_text(textwrap.dedent(content).lstrip())
    path.chmod(0o755)


def _write_fake_raylet(path: Path) -> None:
    # A script keeps its file name as the process name the monitor looks for.
    _write_executable(path, "#!/bin/bash\nwhile true; do sleep 0.1; done\n")


@pytest.fixture
def fake_cluster(tmp_path, monkeypatch):
    """Fake Slurm and Ray commands that run the real scripts on this host."""
    monkeypatch.setattr(session_module, "_WORKER_JOIN_POLL_SECONDS", 1)
    monkeypatch.setattr(session_module, "_WORKER_HEAD_CHECK_SECONDS", 1)
    monkeypatch.setattr(session_module, "_WORKER_NODE_RESTART_DELAY_SECONDS", 1)
    states = tmp_path / "states"
    states.mkdir()
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _write_fake_raylet(bin_dir / "raylet")
    (bin_dir / "python3").symlink_to(sys.executable)
    _write_executable(
        bin_dir / "srun",
        """
        #!/bin/bash
        while [[ $# -gt 0 ]]; do
          case "$1" in
            -w) export SLURMD_NODENAME="$2"; shift 2 ;;
            -*) shift ;;
            *) break ;;
          esac
        done
        exec "$@"
        """,
    )
    _write_executable(
        bin_dir / "squeue",
        """
        #!/bin/bash
        while [[ $# -gt 0 && "$1" != -j ]]; do shift; done
        cat "$FAKE_STATE_DIR/$2" 2>/dev/null || echo RUNNING
        """,
    )
    _write_executable(
        bin_dir / "scontrol", '#!/bin/bash\nprintf "%s\\n" ${FAKE_NODES:-node-a}\n'
    )
    # `ray start --block` keeps running after its raylet exits, like the real one.
    _write_executable(
        bin_dir / "ray",
        """
        #!/bin/bash
        printf '%s: %s\\n' "$SLURMD_NODENAME" "$*" >> "$FAKE_RAY_LOG"
        case "$1" in
          start)
            echo $$ > "$FAKE_STATE_DIR/ray-$SLURMD_NODENAME.pid"
            raylet &
            echo $! > "$RAY_TMPDIR/raylet.pid"
            mkdir -p "$RAY_TMPDIR/session_1/sockets"
            touch "$RAY_TMPDIR/session_1/sockets/raylet"
            trap 'kill "$(cat "$RAY_TMPDIR/raylet.pid")" 2>/dev/null; exit 0' TERM
            while true; do sleep 0.1; done ;;
          drain-node)
            kill "$(cat "$RAY_TMPDIR/raylet.pid")" ;;
        esac
        """,
    )
    fake_ray = tmp_path / "py" / "ray"
    fake_ray.mkdir(parents=True)
    (fake_ray / "__init__.py").write_text(
        textwrap.dedent(
            f"""
            import os
            def init(**kwargs): pass
            def shutdown(): pass
            def nodes():
                socket_name = os.environ["RAY_TMPDIR"] + "/session_1/sockets/raylet"
                node_id = {NODE_IDS!r}[os.environ["SLURMD_NODENAME"]]
                return [
                    {{"Alive": False, "NodeID": "0" * 56, "RayletSocketName": socket_name}},
                    {{"Alive": True, "NodeID": node_id, "RayletSocketName": socket_name}},
                ]
            """
        )
    )
    activation = tmp_path / "activate.sh"
    activation.write_text(
        f"export PATH={shlex.quote(str(bin_dir))}:$PATH\n"
        f"export PYTHONPATH={shlex.quote(str(tmp_path / 'py'))}\n"
    )
    # Unix socket paths are short, so keep the node-local Ray dir out of tmp_path.
    node_tmp = Path("/tmp") / f"dsw{uuid.uuid4().hex[:8]}"
    node_tmp.mkdir()
    env = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "SLURM_JOB_ID": "701",
        "SLURM_JOB_NODELIST": "node-a",
        "SLURM_TMPDIR": str(node_tmp),
        "FAKE_RAY_LOG": str(tmp_path / "ray.log"),
        "FAKE_STATE_DIR": str(states),
    }
    try:
        yield SimpleNamespace(
            activation=activation, env=env, ray_log=tmp_path / "ray.log", states=states
        )
    finally:
        shutil.rmtree(node_tmp, ignore_errors=True)


def _prepare_head(
    head: SlurmAllocation,
    activation: Path,
    address: str = "10.0.0.1:6379",
    launcher: RayLauncher | None = None,
) -> Path:
    ray_dir = Path(head.working_dir) / "ray_cluster"
    ray_dir.mkdir(parents=True)
    worker_script = ray_dir / "ray_worker.sh"
    worker_script.write_text(
        head._render_ray_worker_script(
            launcher=launcher
            or RayLauncher(
                num_gpus_per_node=1,
                object_store_memory_gb=1,
                port_strategy="hash_jobid",
                grace_period=2,
            ),
            activation_script=str(activation),
        )
    )
    worker_script.chmod(0o755)
    shutil.copy(MONITOR_SOURCE, ray_dir / "ray_worker_monitor.py")
    (ray_dir / "ray_address").write_text(f"{address}\n")
    (ray_dir / "ray_ready").touch()
    return ray_dir


def _wait_for(predicate, timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise TimeoutError("condition not met")
        time.sleep(0.1)


@needs_procfs
def test_worker_batch_joins_ray_then_drains_and_releases(tmp_path, fake_cluster):
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    _prepare_head(session.allocation, fake_cluster.activation)
    worker = session.add_worker_allocation(
        SlurmWorkerAllocationConfig(
            ray_resources={"accelerator:test": 1}, ray_start_args=["--num-cpus=1"]
        )
    )
    batch_file = tmp_path / "worker_batch.sh"
    batch_file.write_text(_last_batch_script(pool))
    batch = subprocess.Popen(
        ["bash", str(batch_file)],
        env={**fake_cluster.env, "SLURM_CPUS_ON_NODE": "3"},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    try:
        node_file = Path(worker.working_dir) / "ray_nodes" / "node-a.id"
        _wait_for(node_file.exists)
        pool.states[worker.slurm_job_id] = "RUNNING"
        assert session.wait_for_worker_allocation(
            worker, timeout=5, poll_interval=0.1
        ) == {"node-a": NODE_ID}
        start = fake_cluster.ray_log.read_text().splitlines()[0]
        assert start.startswith("node-a: start --address=10.0.0.1:6379 --num-gpus=1 ")
        # Worker allocation arguments come last, so they override the head's.
        # Without an explicit value Ray would get the CPUs Slurm granted (3).
        assert start.endswith(
            "--object-store-memory=1000000000 --num-cpus=3 --num-gpus=0 "
            '--resources={"accelerator:test": 1, "dagster_slurm_worker": 1} '
            "--num-cpus=1 --block"
        )
        node_tmp = Path(fake_cluster.env["SLURM_TMPDIR"])
        raylet_pid = int(next(node_tmp.glob("r701-*/raylet.pid")).read_text())

        (Path(worker.working_dir) / "drain_timeout").write_text("33\n")
        os.kill(batch.pid, signal.SIGTERM)
        output, _ = batch.communicate(timeout=30)
        assert batch.returncode == 0, output
        assert "Ray workers left the cluster" in output
        drain = fake_cluster.ray_log.read_text().splitlines()[1]
        assert drain == (
            f"node-a: drain-node --address=10.0.0.1:6379 --node-id={NODE_ID} "
            "--reason=DRAIN_NODE_REASON_PREEMPTION --reason-message=Slurm worker "
            "allocation 701 is draining --deadline-remaining-seconds=33"
        )
        assert not ray_worker_monitor.process_alive(raylet_pid)
        node_log = (Path(worker.working_dir) / "ray_nodes" / "node-a.log").read_text()
        assert "Ray node drained" in node_log
        assert not list(node_tmp.glob("r701-*"))
    finally:
        if batch.poll() is None:
            os.killpg(batch.pid, signal.SIGKILL)
            batch.communicate(timeout=5)


def test_worker_batch_releases_after_rejoin_timeout_without_a_head(
    tmp_path, fake_cluster
):
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    session.add_worker_allocation(SlurmWorkerAllocationConfig(rejoin_timeout=2))
    (fake_cluster.states / "700").write_text("CANCELLED\n")
    batch_file = tmp_path / "worker_batch.sh"
    batch_file.write_text(_last_batch_script(pool))
    started = time.monotonic()
    result = subprocess.run(
        ["bash", str(batch_file)],
        env=fake_cluster.env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert "No live Ray head for 2s; releasing the allocation" in result.stdout
    assert time.monotonic() - started >= 2
    assert not fake_cluster.ray_log.exists()


@needs_procfs
def test_worker_batch_rejoins_a_replacement_head(tmp_path, fake_cluster):
    """The head allocation is killed; the workers join its replacement."""
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    _prepare_head(session.allocation, fake_cluster.activation, "10.0.0.1:6379")
    worker = session.add_worker_allocation(
        SlurmWorkerAllocationConfig(rejoin_timeout=60)
    )
    batch_file = tmp_path / "worker_batch.sh"
    batch_file.write_text(_last_batch_script(pool))
    batch = subprocess.Popen(
        ["bash", str(batch_file)],
        env=fake_cluster.env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    node_file = Path(worker.working_dir) / "ray_nodes" / "node-a.id"
    try:
        _wait_for(
            lambda: node_file.exists() and node_file.read_text().split()[1:] == ["700"]
        )

        # The head allocation dies. A replacement starts Ray and takes over.
        (fake_cluster.states / "700").write_text("NODE_FAIL\n")
        replacement = session._make_allocation(
            slurm_job_id=702,
            nodes=["node-c"],
            working_dir=f"{worker.session_dir}/jobs/702",
            session_dir=worker.session_dir,
        )
        _prepare_head(replacement, fake_cluster.activation, "10.0.0.2:6379")
        session._publish_ray_head(replacement)

        _wait_for(
            lambda: node_file.exists() and node_file.read_text().split()[1:] == ["702"]
        )
        starts = [
            line.split()[2]
            for line in fake_cluster.ray_log.read_text().splitlines()
            if line.startswith("node-a: start ")
        ]
        assert starts == ["--address=10.0.0.1:6379", "--address=10.0.0.2:6379"]
        assert batch.poll() is None
    finally:
        os.killpg(batch.pid, signal.SIGKILL)
        output, _ = batch.communicate(timeout=5)
    assert "Joining Ray at 10.0.0.2:6379 (head allocation 702)" in output


def test_a_drained_allocation_stays_while_it_hosts_the_head(tmp_path, fake_cluster):
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    worker = session.add_worker_allocation()
    pointer = Path(worker.session_dir) / "ray_head"
    pointer.write_text(f"{worker.slurm_job_id}\n")  # This allocation leads.
    batch_file = tmp_path / "worker_batch.sh"
    batch_file.write_text(_last_batch_script(pool))
    batch = subprocess.Popen(
        ["bash", str(batch_file)],
        env=fake_cluster.env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    try:
        _wait_for((Path(worker.working_dir) / "ray_nodes" / "drain").exists)
        os.kill(batch.pid, signal.SIGTERM)  # Its walltime approaches.
        # It keeps the head alive until the session moves the head away.
        with pytest.raises(subprocess.TimeoutExpired):
            batch.wait(timeout=3)
        pointer.write_text("700\n")
        output, _ = batch.communicate(timeout=20)
    finally:
        if batch.poll() is None:
            os.killpg(batch.pid, signal.SIGKILL)
            batch.communicate(timeout=5)
    assert batch.returncode == 0, output
    assert "Ray workers left the cluster" in output


def test_read_drain_seconds(tmp_path):
    path = tmp_path / "drain"
    assert ray_worker_monitor.read_drain_seconds(str(path)) is None
    path.write_text("")
    assert ray_worker_monitor.read_drain_seconds(str(path)) is None
    path.write_text("45\n")
    assert ray_worker_monitor.read_drain_seconds(str(path)) == 45


@needs_procfs
def test_monitor_stops_at_the_deadline_when_work_remains(tmp_path, monkeypatch):
    raylet = tmp_path / "raylet"
    _write_fake_raylet(raylet)
    ray_start = subprocess.Popen(
        ["bash", "-c", f"{shlex.quote(str(raylet))} & wait"],
        start_new_session=True,
    )
    drains = []
    monkeypatch.setenv("SLURMD_NODENAME", "node-b")
    (tmp_path / "drain").write_text("")
    try:
        _wait_for(lambda: ray_worker_monitor.child_pids(ray_start.pid, "raylet"))
        result: list[int] = []
        thread = threading.Thread(
            target=lambda: result.append(
                ray_worker_monitor.monitor(
                    address="head:1",
                    temp_dir="/unused",
                    node_dir=str(tmp_path),
                    ray_start_pid=ray_start.pid,
                    resolve=lambda *args, **kwargs: "abc",
                    drain=lambda *args: drains.append(args),
                    sleep=lambda seconds: time.sleep(0.05),
                )
            )
        )
        thread.start()
        _wait_for((tmp_path / "node-b.id").exists)
        (tmp_path / "drain").write_text("1\n")
        thread.join(timeout=10)
        assert result == [0]
        assert drains == [("head:1", "abc", 1)]
        # The raylet still runs; the worker script's exit trap stops Ray.
        assert ray_worker_monitor.child_pids(ray_start.pid, "raylet")
    finally:
        os.killpg(ray_start.pid, signal.SIGKILL)
        ray_start.wait(timeout=5)


def test_worker_config_is_a_run_allocation_config():
    config = SlurmWorkerAllocationConfig(time_limit="01:00:00", time_min="00:30:00")
    assert isinstance(config, SlurmRunAllocationConfig)
    assert config.ray_resources == {} and config.ray_start_args == []


@needs_procfs
def test_monitor_leaves_when_the_head_moves(tmp_path, monkeypatch):
    raylet = tmp_path / "raylet"
    _write_fake_raylet(raylet)
    ray_start = subprocess.Popen(
        ["bash", "-c", f"{shlex.quote(str(raylet))} & wait"],
        start_new_session=True,
    )
    monkeypatch.setenv("SLURMD_NODENAME", "node-b")
    pointer = tmp_path / "ray_head"
    pointer.write_text("700\n")
    (tmp_path / "drain").write_text("")
    node_file = tmp_path / "node-b.id"
    drains = []
    try:
        result: list[int] = []
        thread = threading.Thread(
            target=lambda: result.append(
                ray_worker_monitor.monitor(
                    address="head:1",
                    temp_dir="/unused",
                    node_dir=str(tmp_path),
                    ray_start_pid=ray_start.pid,
                    head_pointer=str(pointer),
                    head_job="700",
                    resolve=lambda *args, **kwargs: "abc",
                    drain=lambda *args: drains.append(args),
                    sleep=lambda seconds: time.sleep(0.05),
                )
            )
        )
        thread.start()
        _wait_for(node_file.exists)
        assert node_file.read_text() == "abc 700\n"
        pointer.write_text("702\n")
        thread.join(timeout=10)
        assert result == [0]
        # It leaves without draining: the old head's work is gone anyway.
        assert drains == []
        assert not node_file.exists()
    finally:
        os.killpg(ray_start.pid, signal.SIGKILL)
        ray_start.wait(timeout=5)


@pytest.mark.parametrize("state", ["RUNNING", "CANCELLED"])
def test_a_step_killed_with_its_allocation_reports_the_allocation_end(tmp_path, state):
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    (tmp_path / "status").write_text("143\n")
    pool.states[700] = state
    expected = SlurmAllocationEnded if state == "CANCELLED" else RuntimeError
    with pytest.raises(expected, match="ended" if state == "CANCELLED" else "failed"):
        session.allocation.wait_for_step(
            SlurmStepExecutionResult(
                job_id=700,
                stdout_path=str(tmp_path / "out"),
                stderr_path=str(tmp_path / "err"),
                step_id_path=str(tmp_path / "id"),
                status_path=str(tmp_path / "status"),
            ),
            ssh_pool=cast(SSHConnectionPool, pool),
            timeout=5,
        )


def test_head_only_head_advertises_no_resources(tmp_path):
    with pytest.raises(ValueError, match="one node"):
        SlurmSessionResource(slurm=_mock_slurm_resource(), ray_head_only=True)
    session = SlurmSessionResource(
        slurm=_mock_slurm_resource(remote_base=str(tmp_path)),
        num_nodes=1,
        ray_head_only=True,
    )
    allocation = SlurmAllocation(700, ["cpu-01"], str(tmp_path), session)
    script = allocation._render_ray_head_script(
        launcher=RayLauncher(num_gpus_per_node=4, ray_start_args=["--num-cpus=8"]),
        activation_script="/env/activate.sh",
        ray_dir=str(tmp_path),
    )
    # Appended after the launcher's arguments, so Ray uses these values.
    assert script.index("--num-cpus=8") < script.index("--num-cpus=0 --num-gpus=0")
    subprocess.run(["bash", "-n"], input=script, text=True, check=True)


def _head_only_session(tmp_path, pool, **worker):
    session = SlurmSessionResource(
        slurm=_mock_slurm_resource(remote_base=str(tmp_path)),
        num_nodes=1,
        gpus_per_node=0,
        partition="cpu",
        time_limit="3-00:00:00",
        ray_head_only=True,
        enable_health_checks=False,
        worker_allocation=SlurmWorkerAllocationConfig(
            partition="GPU-rtx6000",
            gpus_per_node=3,
            time_limit="08:00:00",
            **worker,
        ),
    )
    object.__setattr__(session, "_ssh_pool", cast(SSHConnectionPool, pool))
    context = SimpleNamespace(run=SimpleNamespace(run_id="relay", tags={}))
    object.__setattr__(session, "_allocation", session._create_allocation(context))
    return session


def test_separate_head_submits_compute_as_its_first_worker(tmp_path):
    pool = RelayPool()
    session = _head_only_session(tmp_path, pool)
    assert session.worker_allocation is not None
    session._ensure_worker_allocation(session.worker_allocation)
    session._ensure_worker_allocation(session.worker_allocation)
    head_script, worker_script = [
        content for path, content in pool.writes if path.endswith(".sh")
    ]
    assert "#SBATCH --partition=cpu" in head_script
    assert "--gres" not in head_script
    for line in (
        "#SBATCH --partition=GPU-rtx6000",
        "#SBATCH --gres=gpu:3",
        "#SBATCH --time=08:00:00",
        "ray_args=(--num-gpus=3 '--resources={\"dagster_slurm_worker\": 1}')",
    ):
        assert line in worker_script
    assert pool.submitted_jobs == [700, 701]  # One live worker is enough.

    # Later workers inherit the compute shape, not the head's.
    session.add_worker_allocation(SlurmWorkerAllocationConfig(partition="GPU-a40"))
    script = _last_batch_script(pool)
    assert "#SBATCH --partition=GPU-a40" in script
    assert "#SBATCH --time=08:00:00" in script
    assert "#SBATCH --gres=gpu:3" in script

    # With every worker gone, the next setup brings compute back.
    pool.states.update({701: "TIMEOUT", 702: "COMPLETED"})
    session._ensure_worker_allocation(session.worker_allocation)
    assert pool.submitted_jobs == [700, 701, 702, 703]


def test_a_retried_run_elects_a_live_worker_to_lead(tmp_path):
    pool = ElectionPool()
    session = _head_only_session(tmp_path, pool)
    assert session.worker_allocation is not None
    session._ensure_worker_allocation(session.worker_allocation)
    pool.states.update({700: "NODE_FAIL", 701: "RUNNING"})

    # A retry after the supervisor died: the running worker leads, no new job.
    retried = _head_only_session(tmp_path, pool)
    assert retried.allocation.slurm_job_id == 701
    assert retried.allocation.worker_ray_args is not None
    assert retried.worker_allocation is not None
    retried._ensure_worker_allocation(retried.worker_allocation)
    assert pool.submitted_jobs == [700, 701]
    assert retried.worker_allocations == []
    session_dir = Path(retried.allocation.session_dir)
    assert (session_dir / "ray_head").read_text() == "701\n"
    metadata = json.loads((session_dir / "allocation.json").read_text())
    assert metadata["ray_args"] == [
        "--num-gpus=3",
        '--resources={"dagster_slurm_worker": 1}',
    ]
    assert metadata["head_config"]["partition"] == "cpu"
    assert [record["slurm_job_id"] for record in metadata["predecessors"]] == [700]


def _record_ray_starts(monkeypatch) -> list[int]:
    started: list[int] = []
    monkeypatch.setattr(
        SlurmAllocation,
        "_read_ray_start_options",
        lambda self, ssh_pool: {"launcher": RayLauncher(), "activation_script": "a"},
    )
    monkeypatch.setattr(
        SlurmAllocation,
        "ensure_ray_cluster",
        lambda self, **kwargs: started.append(self.slurm_job_id),
    )
    return started


def test_replace_head_elects_the_running_worker_and_keeps_it(tmp_path, monkeypatch):
    pool = ElectionPool()
    session = _head_only_session(tmp_path, pool)
    assert session.worker_allocation is not None
    session._ensure_worker_allocation(session.worker_allocation)
    started = _record_ray_starts(monkeypatch)
    pool.states[700] = "NODE_FAIL"

    head = session.replace_head(timeout=10)

    # No queue wait: worker allocation 701 now also hosts the head.
    assert head.slurm_job_id == 701 and started == [701]
    assert head.worker_ray_args is not None
    assert session.allocation.slurm_job_id == 701
    assert pool.submitted_jobs == [700, 701]
    assert pool.states[701] == "RUNNING"
    assert (Path(head.session_dir) / "ray_head").read_text() == "701\n"
    # The elected worker is the head now; later heads still use the head shape.
    metadata = json.loads((Path(head.session_dir) / "allocation.json").read_text())
    assert metadata["workers"] == [] and "election" not in metadata
    assert metadata["head_config"]["partition"] == "cpu"


def test_replace_head_submits_a_head_when_no_worker_runs(tmp_path, monkeypatch):
    pool = ElectionPool()
    session = _head_only_session(tmp_path, pool)
    assert session.worker_allocation is not None
    session._ensure_worker_allocation(session.worker_allocation)
    started = _record_ray_starts(monkeypatch)
    pool.states.update({700: "NODE_FAIL", 701: "PENDING"})
    monkeypatch.setattr(session_module, "_HEAD_ELECTION_RETRY_SECONDS", 0.01)

    head = session.replace_head(timeout=10)

    # A new allocation shaped like the first head, not like the worker.
    assert head.slurm_job_id == 702 and started == [702]
    assert head.config.ray_head_only and head.config.partition == "cpu"
    assert [worker.slurm_job_id for worker in session.worker_allocations] == [701]


def test_concurrent_callers_elect_one_head(tmp_path, monkeypatch):
    """Every step whose driver ran on the lost head calls replace_head at once."""
    pool = ElectionPool()
    first = _head_only_session(tmp_path, pool)
    assert first.worker_allocation is not None
    first._ensure_worker_allocation(first.worker_allocation)
    # Another step process attached to the same session.
    second = _head_only_session(tmp_path, pool)
    started = _record_ray_starts(monkeypatch)
    monkeypatch.setattr(session_module, "_HEAD_ELECTION_RETRY_SECONDS", 0.01)
    pool.states[700] = "NODE_FAIL"

    with ThreadPoolExecutor(max_workers=4) as executor:
        heads = list(
            executor.map(
                lambda session: session.replace_head(timeout=20).slurm_job_id,
                [first, second, first, second],
            )
        )

    assert heads == [701, 701, 701, 701]
    assert started == [701]
    assert pool.submitted_jobs == [700, 701]
    metadata = json.loads(
        (Path(first.allocation.session_dir) / "allocation.json").read_text()
    )
    assert [record["slurm_job_id"] for record in metadata["predecessors"]] == [700]
    # A caller that arrives after the election gets the same head.
    assert second.replace_head(timeout=1).slurm_job_id == 701


def test_watchdog_hands_over_a_draining_head_after_its_payloads(tmp_path, monkeypatch):
    pool = ElectionPool()
    session = _head_only_session(tmp_path, pool)
    assert session.worker_allocation is not None
    session._ensure_worker_allocation(session.worker_allocation)
    started = _record_ray_starts(monkeypatch)
    head = session.allocation
    (Path(head.working_dir) / "drain_signal").write_text("TERM\n")
    payload = Path(head.working_dir) / "payloads" / "one"
    payload.mkdir(parents=True)

    assert session._keep_head() is None  # Its driver is still checkpointing.
    assert started == []
    (payload / "status").write_text("0\n")
    assert session._keep_head() is not None
    assert session.allocation.slurm_job_id == 701 and started == [701]
    # The phased-out head is released once the worker leads.
    assert pool.states[700] == "CANCELLED"


def test_no_extra_worker_is_submitted_while_another_process_elects(
    tmp_path, monkeypatch
):
    pool = ElectionPool()
    session = _head_only_session(tmp_path, pool)
    assert session.worker_allocation is not None
    session._ensure_worker_allocation(session.worker_allocation)
    stale = session.allocation
    _record_ray_starts(monkeypatch)
    pool.states[700] = "NODE_FAIL"
    # Another step process elects worker 701 ...
    other = _head_only_session(tmp_path, pool)
    assert other.replace_head(timeout=10).slurm_job_id == 701
    # ... right after this process read its head without the lock.
    monkeypatch.setattr(
        SlurmSessionResource, "allocation", property(lambda self: stale)
    )
    session._ensure_worker_allocation(session.worker_allocation)
    assert pool.submitted_jobs == [700, 701]


def test_a_payload_prepared_for_a_replaced_head_reports_the_end(tmp_path):
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    old_head = session.allocation
    session.submit_successor()
    pool.states[701] = "RUNNING"
    object.__setattr__(session, "_context", None)
    session.promote_successor()  # No Ray was recorded, so none is started.
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    with pytest.raises(SlurmAllocationEnded) as error:
        old_head.execute(
            ExecutionPlan(
                kind=RuntimeVariant.SHELL,
                payload=["#!/bin/bash", "exit 0"],
                environment={},
                resources={},
            ),
            asset_key="late",
            run_dir=str(run_dir),
            ssh_pool=cast(SSHConnectionPool, pool),
        )
    # The failover loop catches it, then launches in the current head.
    assert error.value.result.allocation_state == "REPLACED"


def test_sessions_without_workers_keep_their_allocation(tmp_path):
    pool = ElectionPool()
    session = make_session(tmp_path, pool)
    held = session.allocation
    # A sibling step process promotes a relay successor meanwhile.
    sibling = make_session(tmp_path, pool)
    sibling.submit_successor()
    object.__setattr__(sibling, "_context", None)
    sibling.promote_successor()
    assert session._keep_head() is held  # Not swapped behind the step's back.


def test_only_sessions_with_workers_watch_their_head(tmp_path, monkeypatch):
    class ContextPool(RelayPool):
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    pool = ContextPool()
    monkeypatch.setattr(session_module, "SSHConnectionPool", lambda *a: pool)
    monkeypatch.setattr(
        SlurmSessionResource, "_start_supervisor_heartbeat", lambda *a: None
    )
    watching: list[SlurmSessionResource] = []
    monkeypatch.setattr(
        SlurmSessionResource, "_start_head_watch", lambda self: watching.append(self)
    )
    context = SimpleNamespace(
        run=SimpleNamespace(run_id="relay", tags={}), instance=None
    )
    plain = SlurmSessionResource(
        slurm=_mock_slurm_resource(remote_base=str(tmp_path)),
        num_nodes=1,
        enable_health_checks=False,
    )
    plain.setup_for_execution(cast(Any, context))
    assert watching == []  # As before worker allocations existed.

    plain.add_worker_allocation()  # The first worker makes the session elastic.
    assert watching == [plain]
    # A step process that starts later finds the worker in the metadata.
    sibling = SlurmSessionResource(slurm=plain.slurm, enable_health_checks=False)
    sibling.setup_for_execution(cast(Any, context))
    assert watching == [plain, sibling]


def test_the_election_claim_is_refreshed_while_ray_starts(tmp_path, monkeypatch):
    pool = ElectionPool()
    session = _head_only_session(tmp_path, pool)
    assert session.worker_allocation is not None
    session._ensure_worker_allocation(session.worker_allocation)
    monkeypatch.setattr(session_module, "_ELECTION_CLAIM_SECONDS", 0.4)
    metadata_path = Path(session.allocation.session_dir) / "allocation.json"
    claims: list[float] = []

    def slow_start(self, **kwargs):
        for _ in range(3):
            claims.append(json.loads(metadata_path.read_text())["election"]["since"])
            time.sleep(0.25)

    monkeypatch.setattr(
        SlurmAllocation,
        "_read_ray_start_options",
        lambda self, ssh_pool: {"launcher": RayLauncher(), "activation_script": "a"},
    )
    monkeypatch.setattr(SlurmAllocation, "ensure_ray_cluster", slow_start)
    pool.states[700] = "NODE_FAIL"

    assert session.replace_head(timeout=10).slurm_job_id == 701
    assert claims[-1] > claims[0]  # The live elector kept its claim fresh.
    assert "election" not in json.loads(metadata_path.read_text())


def test_a_dead_electors_claim_expires_before_workers_give_up(tmp_path, monkeypatch):
    assert (
        session_module._ELECTION_CLAIM_SECONDS
        < session_module._DEFAULT_REJOIN_TIMEOUT_SECONDS / 2
    )
    pool = ElectionPool()
    session = _head_only_session(tmp_path, pool)
    assert session.worker_allocation is not None
    session._ensure_worker_allocation(session.worker_allocation)
    _record_ray_starts(monkeypatch)
    metadata_path = Path(session.allocation.session_dir) / "allocation.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["election"] = {
        "slurm_job_id": 701,
        "owner": "killed-elector",
        "since": time.time() - session_module._ELECTION_CLAIM_SECONDS - 1,
    }
    metadata_path.write_text(json.dumps(metadata))
    pool.states[700] = "NODE_FAIL"

    assert session.replace_head(timeout=10).slurm_job_id == 701


def test_an_aborted_election_stops_the_head_it_started(tmp_path, monkeypatch):
    pool = ElectionPool()
    session = _head_only_session(tmp_path, pool)
    assert session.worker_allocation is not None
    session._ensure_worker_allocation(session.worker_allocation)

    def start_then_lose_the_worker(self, **kwargs):
        ray_dir = Path(self.working_dir) / "ray_cluster"
        ray_dir.mkdir(parents=True, exist_ok=True)
        (ray_dir / "head_step").write_text(f"{self.slurm_job_id}.4\n")
        (ray_dir / "ray_ready").touch()
        (ray_dir / "ray_address").write_text("10.0.0.9:20000\n")
        pool.states[self.slurm_job_id] = "CANCELLED"  # Ends before publication.

    monkeypatch.setattr(
        SlurmAllocation,
        "_read_ray_start_options",
        lambda self, ssh_pool: {"launcher": RayLauncher(), "activation_script": "a"},
    )
    monkeypatch.setattr(
        SlurmAllocation, "ensure_ray_cluster", start_then_lose_the_worker
    )
    pool.states[700] = "NODE_FAIL"

    assert session._keep_head() is None
    assert "scancel 701.4" in pool.commands
    ray_dir = (
        tmp_path / "allocations" / "dagster_relay" / "jobs" / "701" / "ray_cluster"
    )
    assert not (ray_dir / "ray_ready").exists()
    assert not (ray_dir / "ray_address").exists()
    metadata = json.loads((ray_dir.parents[2] / "allocation.json").read_text())
    assert metadata["slurm_job_id"] == 700 and "election" not in metadata


def test_a_head_is_drained_before_its_walltime(tmp_path):
    pool = ElectionPool()
    session = _head_only_session(tmp_path, pool)
    assert session.worker_allocation is not None
    session._ensure_worker_allocation(session.worker_allocation)
    pool.time_left[700] = "04:00"  # Four minutes, no pre-walltime signal.

    head = session._keep_head()

    assert head is not None and head.slurm_job_id == 700
    assert "scancel --batch --signal=TERM 700" in pool.commands


def test_election_prefers_the_worker_with_most_time_left(tmp_path, monkeypatch):
    pool = ElectionPool()
    session = _head_only_session(tmp_path, pool)
    short, long, draining = (session.add_worker_allocation() for _ in range(3))
    pool.states.update(
        {worker.slurm_job_id: "RUNNING" for worker in (short, long, draining)}
    )
    pool.time_left.update(
        {
            short.slurm_job_id: "1:00:00",
            long.slurm_job_id: "5:00:00",
            draining.slurm_job_id: "9:00:00",
        }
    )
    Path(draining.working_dir).mkdir(parents=True, exist_ok=True)
    (Path(draining.working_dir) / "drain_signal").write_text("TERM\n")
    _record_ray_starts(monkeypatch)
    pool.states[700] = "TIMEOUT"

    assert session.replace_head(timeout=10).slurm_job_id == long.slurm_job_id


def test_a_head_whose_ray_stopped_is_replaced(tmp_path, monkeypatch):
    pool = ElectionPool()
    session = _head_only_session(tmp_path, pool)
    assert session.worker_allocation is not None
    session._ensure_worker_allocation(session.worker_allocation)
    started = _record_ray_starts(monkeypatch)
    ray_dir = Path(session.allocation.working_dir) / "ray_cluster"
    ray_dir.mkdir(parents=True)
    (ray_dir / "ray_exited").touch()

    # The head's job still runs, but its Ray is gone: a worker takes over.
    head = session._keep_head()
    assert head is not None and head.slurm_job_id == 701
    assert started == [701] and pool.states[700] == "CANCELLED"


def test_sessions_without_workers_are_left_to_their_caller(tmp_path):
    pool = ElectionPool()
    session = make_session(tmp_path, pool)
    held = session.allocation
    pool.states[700] = "NODE_FAIL"
    assert session._keep_head() is held
    assert pool.submitted_jobs == [700]


def test_a_worker_allocation_hosts_a_head_only_head_next_to_its_worker(tmp_path):
    pool = ElectionPool()
    session = make_session(tmp_path, pool)
    allocation = session.add_worker_allocation()
    allocation.nodes = ["node-a", "node-b"]
    script = allocation._render_ray_head_script(
        launcher=RayLauncher(num_gpus_per_node=4),
        activation_script="/env/activate.sh",
        ray_dir=str(tmp_path / "ray"),
    )
    assert "--num-cpus=0 --num-gpus=0" in script
    assert "export RAY_PORT_SEED=$((_head_seed + 1))" in script
    assert "ray_exited" in script
    subprocess.run(["bash", "-n"], input=script, text=True, check=True)


def test_compute_resource_runs_the_head_in_a_separate_allocation(monkeypatch):
    monkeypatch.setattr(SlurmSessionResource, "setup_for_execution", lambda *a: None)
    compute = ComputeResource(
        mode="slurm",
        slurm=_mock_slurm_resource(gpus_per_node=1),
        allocation_scope="run",
        default_launcher=RayLauncher(num_gpus_per_node=4),
        ray_head_allocation=SlurmRunAllocationConfig(
            partition="cpu", mem="8G", time_limit="3-00:00:00"
        ),
        run_allocation=SlurmRunAllocationConfig(
            partition="GPU-a40", num_nodes=2, gpus_per_node=4, time_limit="08:00:00"
        ),
    )
    session = compute.get_run_allocation_session(dg.build_init_resource_context())
    assert session.ray_head_only
    # The queue's GPU default describes compute, so the head asks for none.
    assert (session.partition, session.num_nodes, session.gpus_per_node) == (
        "cpu",
        1,
        0,
    )
    assert session.time_limit == "3-00:00:00" and session.mem == "8G"
    worker = session.worker_allocation
    assert worker is not None
    assert (worker.partition, worker.num_nodes, worker.gpus_per_node) == (
        "GPU-a40",
        2,
        4,
    )
    assert worker.time_limit == "08:00:00" and worker.mem == "1G"
    # The launcher is checked against the compute shape, not the CPU head.
    compute._validate_run_allocation_launcher(RayLauncher(num_gpus_per_node=4))
    with pytest.raises(ValueError, match="allocation_scope='run'"):
        ComputeResource(
            mode="slurm",
            slurm=_mock_slurm_resource(),
            default_launcher=RayLauncher(),
            ray_head_allocation=SlurmRunAllocationConfig(),
        )


@pytest.mark.parametrize("state,healthy", [("RUNNING", True), ("CANCELLED", False)])
def test_orphan_sensor_waits_while_workers_await_a_new_head(tmp_path, state, healthy):
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    worker = session.add_worker_allocation()
    pool.states.update({700: "NODE_FAIL", worker.slurm_job_id: state})
    with dg.DagsterInstance.ephemeral() as instance:
        instance.add_run(
            dg.DagsterRun(
                job_name="relay",
                run_id="relay",
                status=dg.DagsterRunStatus.STARTED,
                tags={
                    "dagster_slurm/job_id": "700",
                    "dagster_slurm/run_dir": str(tmp_path),
                    "dagster_slurm/session_allocation_dir": worker.session_dir,
                },
            )
        )
        context_pool = MagicMock()
        context_pool.__enter__.return_value = pool
        with patch(
            "dagster_slurm.sensors.SSHConnectionPool", return_value=context_pool
        ):
            requests = reconcile_orphaned_slurm_runs(instance, session.slurm)
    assert (requests == []) == healthy


def test_nodes_with_the_same_temp_dir_resolve_to_their_own_ids(tmp_path, monkeypatch):
    """With hash_jobid ports, all nodes of an allocation share the temp dir path."""
    temp_dir = tmp_path / "r701-10000"
    sockets = temp_dir / "session_1" / "sockets"
    sockets.mkdir(parents=True)
    (sockets / "raylet").touch()
    socket_name = str(sockets / "raylet")
    nodes = [
        {
            "Alive": True,
            "NodeID": "a" * 56,
            "RayletSocketName": socket_name,
            "NodeManagerAddress": "10.0.0.1",
            "NodeManagerHostname": "gpu-01",
        },
        {
            "Alive": True,
            "NodeID": "b" * 56,
            "RayletSocketName": socket_name,
            "NodeManagerAddress": "10.0.0.2",
            "NodeManagerHostname": "gpu-02",
        },
    ]
    connections = []
    fake_ray = SimpleNamespace(
        init=lambda **kwargs: connections.append(kwargs),
        shutdown=lambda: None,
        nodes=lambda: nodes,
    )
    monkeypatch.setitem(sys.modules, "ray", fake_ray)
    monkeypatch.setattr(ray_worker_monitor.socket, "gethostname", lambda: "gpu-02")

    def resolve(node_ip: str) -> str | None:
        return ray_worker_monitor.resolve_node_id(
            "head:1", str(temp_dir), node_ip, os.getpid(), timeout=5
        )

    assert resolve("") == "b" * 56  # Matched by hostname.
    assert resolve("10.0.0.1") == "a" * 56  # Matched by the configured address.
    assert len(connections) == 2  # One driver connection per lookup.
    # If two nodes still match, publishing either ID would drain the wrong one.
    nodes[0]["NodeManagerHostname"] = "gpu-02"
    assert resolve("") is None


def test_resolve_node_id_waits_for_the_raylet_socket(tmp_path, monkeypatch):
    connections = []
    fake_ray = SimpleNamespace(
        init=lambda **kwargs: connections.append(kwargs),
        shutdown=lambda: None,
        nodes=lambda: [],
    )
    monkeypatch.setitem(sys.modules, "ray", fake_ray)
    monkeypatch.setattr(ray_worker_monitor, "POLL_SECONDS", 0.01)
    node_id = ray_worker_monitor.resolve_node_id(
        "head:1", str(tmp_path), "", os.getpid(), timeout=0.2
    )
    assert node_id is None
    assert connections == []  # No driver before the local raylet exists.


def test_a_drain_request_cuts_the_registration_backoff_short(tmp_path, monkeypatch):
    sockets = tmp_path / "session_1" / "sockets"
    sockets.mkdir(parents=True)
    (sockets / "raylet").touch()
    connected = []
    fake_ray = SimpleNamespace(
        init=lambda **kwargs: connected.append(True),
        shutdown=lambda: None,
        nodes=lambda: [],
    )
    monkeypatch.setitem(sys.modules, "ray", fake_ray)
    # The first backoff after a failed lookup would last five seconds.
    monkeypatch.setattr(ray_worker_monitor, "POLL_SECONDS", 5.0)
    started = time.monotonic()
    node_id = ray_worker_monitor.resolve_node_id(
        "head:1",
        str(tmp_path),
        "",
        os.getpid(),
        stop=lambda: bool(connected),
        timeout=60,
    )
    assert node_id is None and connected == [True]
    assert time.monotonic() - started < 2


@needs_procfs
@pytest.mark.skipif(shutil.which("flock") is None, reason="needs util-linux flock")
def test_worker_batch_restarts_only_the_node_whose_ray_worker_died(
    tmp_path, fake_cluster
):
    pool = RelayPool()
    session = make_session(tmp_path, pool)
    # Both fake nodes run on this host, so give them separate port blocks.
    _prepare_head(
        session.allocation,
        fake_cluster.activation,
        launcher=RayLauncher(
            port_strategy="random",
            port_config=RayPortConfig(lock_dir=str(tmp_path / "port-locks")),
            grace_period=2,
        ),
    )
    worker = session.add_worker_allocation()
    batch_file = tmp_path / "worker_batch.sh"
    batch_file.write_text(_last_batch_script(pool))
    batch = subprocess.Popen(
        ["bash", str(batch_file)],
        env={**fake_cluster.env, "FAKE_NODES": "node-a node-b"},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    nodes_dir = Path(worker.working_dir) / "ray_nodes"

    def ray_pid(node: str) -> int:
        return int((fake_cluster.states / f"ray-{node}.pid").read_text())

    def starts(node: str) -> int:
        lines = fake_cluster.ray_log.read_text().splitlines()
        return sum(line.startswith(f"{node}: start ") for line in lines)

    try:
        pool.states[worker.slurm_job_id] = "RUNNING"
        assert (
            session.wait_for_worker_allocation(worker, timeout=20, poll_interval=0.2)
            == NODE_IDS
        )
        node_b = ray_pid("node-b")

        # Ray dies on node-a only; node-b keeps serving and node-a comes back.
        os.kill(ray_pid("node-a"), signal.SIGKILL)
        _wait_for(lambda: starts("node-a") == 2)
        _wait_for(lambda: (nodes_dir / "node-a.id").exists())
        assert starts("node-b") == 1 and ray_pid("node-b") == node_b
        assert ray_worker_monitor.process_alive(node_b)

        (Path(worker.working_dir) / "drain_timeout").write_text("5\n")
        os.kill(batch.pid, signal.SIGTERM)
        output, _ = batch.communicate(timeout=30)
    finally:
        try:
            # Also stops the raylet orphaned by the killed `ray start`.
            os.killpg(batch.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        if batch.returncode is None:
            batch.communicate(timeout=5)
    assert batch.returncode == 0, output
    assert "Ray worker on node-a ended early" in output
    drains = {
        line.split(":")[0]: line.split("--node-id=")[1].split()[0]
        for line in fake_cluster.ray_log.read_text().splitlines()
        if ": drain-node " in line
    }
    assert drains == NODE_IDS  # Each node drained itself, not its neighbour.


def test_separate_head_does_not_take_the_compute_queue_shape(monkeypatch):
    monkeypatch.setattr(SlurmSessionResource, "setup_for_execution", lambda *a: None)
    gpu_queue = SlurmResource(
        ssh=_mock_slurm_resource().ssh,
        queue=SlurmQueueConfig(
            partition="GPU-a100", cpus=64, mem="400G", gpus_per_node=4
        ),
        remote_base="/tmp/dagster_test",
    )
    compute = ComputeResource(
        mode="slurm",
        slurm=gpu_queue,
        allocation_scope="run",
        default_launcher=RayLauncher(num_gpus_per_node=4),
        ray_head_allocation=SlurmRunAllocationConfig(
            partition="cpu", time_limit="7-00:00:00"
        ),
    )
    session = compute.get_run_allocation_session(dg.build_init_resource_context())
    assert session.time_limit == "7-00:00:00"
    assert (
        session.partition,
        session.num_nodes,
        session.cpus_per_task,
        session.mem,
        session.gpus_per_node,
    ) == ("cpu", 1, 2, "8G", 0)
    worker = session.worker_allocation
    assert worker is not None
    assert (worker.partition, worker.cpus_per_task, worker.mem) == (
        "GPU-a100",
        64,
        "400G",
    )
    # The queue's walltime is for compute, so the head needs its own.
    for head in (
        SlurmRunAllocationConfig(time_limit="7-00:00:00"),
        SlurmRunAllocationConfig(partition="cpu"),
    ):
        with pytest.raises(ValueError, match="explicit partition and time_limit"):
            ComputeResource(
                mode="slurm",
                slurm=gpu_queue,
                allocation_scope="run",
                default_launcher=RayLauncher(),
                ray_head_allocation=head,
            )


_DRIVER = """
import json, socket, sys, time
import ray

address, phase, marker = sys.argv[1:4]
ray.init(address=address, logging_level="ERROR", log_to_driver=False)


@ray.remote(num_cpus=0, resources={"elastic_worker": 1})
def on_worker(seconds):
    open(marker + ".started", "w").close()
    time.sleep(seconds)
    open(marker + ".finished", "w").close()
    return socket.gethostname()


@ray.remote(num_cpus=0)
def anywhere():
    return socket.gethostname()


if phase == "joined":
    result = {"host": ray.get(on_worker.remote(0), timeout=60)}
elif phase == "busy":
    result = {"host": ray.get(on_worker.remote(20), timeout=300)}
else:
    result = {"host": ray.get(anywhere.remote(), timeout=60)}
result["cpus"] = ray.cluster_resources().get("CPU", 0)
result["nodes"] = [
    {"id": n["NodeID"], "alive": n["Alive"], "reason": n.get("DeathReasonMessage")}
    for n in ray.nodes()
]
print("DRIVER_RESULT " + json.dumps(result), flush=True)
"""


def _run_driver(pool, head, activation, address, driver, phase, marker):
    command = (
        f"source {shlex.quote(activation)} && python {shlex.quote(driver)} "
        f"{shlex.quote(address)} {phase} {shlex.quote(marker)}"
    )
    output = pool.run(
        f"srun --overlap --jobid={head.slurm_job_id} -N1 -n1 "
        f"bash -c {shlex.quote(command)}",
        timeout=360,
    )
    line = next(line for line in output.splitlines() if "DRIVER_RESULT " in line)
    return json.loads(line.split("DRIVER_RESULT ", 1)[1])


def _wait_for_state(session, job_id, states, timeout):
    deadline = time.monotonic() + timeout
    while (state := session._get_job_state(job_id)) not in states:
        if time.monotonic() > deadline:
            raise TimeoutError(f"job {job_id} is still {state}")
        time.sleep(3)
    return state


@pytest.mark.needs_slurm_docker
def test_slurm_worker_allocations_join_drain_and_release(
    slurm_resource_for_testing, slurm_cluster_ready, request
):
    """Dummy CPU jobs on the Docker cluster; memory stays within CI limits."""
    session = SlurmSessionResource(
        slurm=slurm_resource_for_testing,
        num_nodes=1,
        partition="normal",
        time_limit="00:20:00",
        cpus_per_task=1,
        mem="3G",
        gpus_per_node=0,
        enable_health_checks=False,
    )
    context = SimpleNamespace(
        run=SimpleNamespace(run_id=f"elastic_{uuid.uuid4().hex}", tags={}),
        instance=MagicMock(),
    )
    session.setup_for_execution(cast(Any, context))
    try:
        head = session.allocation
        pool = session._require_ssh_pool()
        activation = os.getenv("DAGSTER_SLURM_RELAY_TEST_ACTIVATE")
        if activation is None:
            deployment = request.getfixturevalue("deployment_metadata")
            activation = f"{deployment['deployment_path']}/activate.sh"
        address = head.ensure_ray_cluster(
            ssh_pool=pool,
            launcher=RayLauncher(
                num_gpus_per_node=0,
                object_store_memory_gb=1,
                ray_start_args=["--num-cpus=1"],
            ),
            activation_script=activation,
            startup_timeout=180,
        )
        run_dir = f"{head.working_dir}/elastic"
        driver = f"{run_dir}/driver.py"
        pool.run(f"mkdir -p {shlex.quote(run_dir)}")
        pool.write_file(_DRIVER, driver)
        # One small worker at a time, on the node the head does not use.
        worker_shape = dict(
            num_nodes=1,
            cpus_per_task=1,
            mem="2G",
            exclude=head.nodes[0],
            ray_resources={"elastic_worker": 1},
            ray_start_args=["--num-cpus=1", "--object-store-memory=200000000"],
        )

        # On-demand removal lets running work finish before the job ends.
        worker = session.add_worker_allocation(
            SlurmWorkerAllocationConfig(**worker_shape)
        )
        [(worker_node, worker_ray_id)] = session.wait_for_worker_allocation(
            worker, timeout=300
        ).items()
        assert worker_node != head.nodes[0]
        joined = _run_driver(
            pool, head, activation, address, driver, "joined", f"{run_dir}/joined"
        )
        assert joined["host"] == worker_node
        assert sum(node["alive"] for node in joined["nodes"]) == 2
        assert [found.slurm_job_id for found in session.worker_allocations] == [
            worker.slurm_job_id
        ]

        busy_marker = f"{run_dir}/busy"
        # The pool serializes commands; the busy driver needs its own connection.
        with (
            SSHConnectionPool(session.slurm.ssh) as driver_pool,
            ThreadPoolExecutor(max_workers=1) as executor,
        ):
            busy = executor.submit(
                _run_driver,
                driver_pool,
                head,
                activation,
                address,
                driver,
                "busy",
                busy_marker,
            )
            _wait_for(
                lambda: (
                    pool.run(
                        f"test -f {shlex.quote(busy_marker + '.started')} && echo yes || true"
                    ).strip()
                    == "yes"
                ),
                timeout=120,
            )
            state = session.remove_worker_allocation(worker, drain_timeout=120)
            assert busy.result(timeout=120)["host"] == worker_node
        assert state == "COMPLETED"
        assert pool.run(f"test -f {shlex.quote(busy_marker + '.finished')} && echo ok")
        after = _run_driver(
            pool, head, activation, address, driver, "head", f"{run_dir}/head"
        )
        assert after["host"] == head.nodes[0]
        [left] = [node for node in after["nodes"] if node["id"] == worker_ray_id]
        assert not left["alive"] and "is draining" in (left["reason"] or "")
        assert session._get_job_state(head.slurm_job_id) == "RUNNING"
        assert session.worker_allocations == []

        # The pre-walltime signal drains an idle worker and releases it early.
        short = session.add_worker_allocation(
            SlurmWorkerAllocationConfig(
                **worker_shape, time_limit="00:03:00", signal_before_timeout="TERM@60"
            )
        )
        session.wait_for_worker_allocation(short, timeout=110)
        assert (
            _wait_for_state(session, short.slurm_job_id, TERMINAL_STATES, 240)
            == "COMPLETED"
        )
        assert pool.run(f"cat {shlex.quote(short.working_dir + '/drain_signal')}")
        after = _run_driver(
            pool, head, activation, address, driver, "head", f"{run_dir}/head2"
        )
        assert sum(node["alive"] for node in after["nodes"]) == 1
    finally:
        session.teardown_after_execution(cast(Any, context))


@pytest.mark.needs_slurm_docker
def test_slurm_leaders_are_elected_as_allocations_leave(
    slurm_resource_for_testing, slurm_cluster_ready, request, monkeypatch
):
    """The first allocation dies, then the next leader phases out; Ray goes on.

    Two sessions stand for two step processes of one run. The worker spans
    both Docker nodes with hash_jobid ports, so its nodes share a Ray temp dir
    path and must still find their own IDs.
    """
    monkeypatch.setattr(session_module, "_HEAD_WATCH_SECONDS", 5)
    settings = dict(
        slurm=slurm_resource_for_testing,
        num_nodes=1,
        partition="normal",
        time_limit="00:20:00",
        cpus_per_task=1,
        mem="3G",
        gpus_per_node=0,
        ray_head_only=True,
        enable_health_checks=False,
        worker_allocation=SlurmWorkerAllocationConfig(
            num_nodes=2,
            cpus_per_task=1,
            mem="3G",
            time_limit="00:20:00",
            ray_resources={"elastic_worker": 1},
            # No --num-cpus: each node offers the one CPU Slurm granted.
            ray_start_args=["--object-store-memory=200000000"],
            rejoin_timeout=600,
        ),
    )
    session = SlurmSessionResource(**settings)
    sibling = SlurmSessionResource(**settings)
    context = SimpleNamespace(
        run=SimpleNamespace(run_id=f"leaders_{uuid.uuid4().hex}", tags={}),
        instance=MagicMock(),
    )
    session.setup_for_execution(cast(Any, context))
    sibling.setup_for_execution(cast(Any, context))
    try:
        head = session.allocation
        pool = session._require_ssh_pool()
        activation = os.getenv("DAGSTER_SLURM_RELAY_TEST_ACTIVATE")
        if activation is None:
            deployment = request.getfixturevalue("deployment_metadata")
            activation = f"{deployment['deployment_path']}/activate.sh"
        launcher = RayLauncher(
            num_gpus_per_node=0, object_store_memory_gb=1, port_strategy="hash_jobid"
        )

        def address_of(allocation: SlurmAllocation) -> str:
            return allocation.ensure_ray_cluster(
                ssh_pool=pool,
                launcher=launcher,
                activation_script=activation,
                startup_timeout=180,
            )

        address = address_of(head)
        run_dir = f"{head.session_dir}/elastic"
        driver = f"{run_dir}/driver.py"
        pool.run(f"mkdir -p {shlex.quote(run_dir)}")
        pool.write_file(_DRIVER, driver)
        # setup_for_execution submitted the compute as the first worker.
        [first] = session.worker_allocations
        first_ids = session.wait_for_worker_allocation(first, timeout=300)
        assert len(first_ids) == 2 and len(set(first_ids.values())) == 2
        joined = _run_driver(
            pool, head, activation, address, driver, "joined", f"{run_dir}/1"
        )
        assert joined["host"] in first_ids
        assert joined["cpus"] == 2  # One granted CPU per worker node; none on the head.

        # The first allocation goes down. Both steps ask for a head at once.
        pool.run(f"scancel {head.slurm_job_id}")
        with ThreadPoolExecutor(max_workers=2) as executor:
            leaders = [
                future.result().slurm_job_id
                for future in [
                    executor.submit(member.replace_head, timeout=300)
                    for member in (session, sibling)
                ]
            ]
        assert leaders == [first.slurm_job_id, first.slurm_job_id]
        leader = session.allocation
        node_ids = session.wait_for_worker_allocation(first, timeout=300)
        assert sorted(node_ids) == sorted(first_ids)
        assert not set(node_ids.values()) & set(first_ids.values())
        rejoined = _run_driver(
            pool,
            leader,
            activation,
            address_of(leader),
            driver,
            "joined",
            f"{run_dir}/2",
        )
        assert rejoined["host"] in node_ids and rejoined["cpus"] == 2

        # Another worker joins, then the leader phases out as at its walltime.
        # Ray settings are per allocation; the Slurm shape comes from the first.
        second = session.add_worker_allocation(
            SlurmWorkerAllocationConfig(
                num_nodes=1,
                ray_resources={"elastic_worker": 1},
                ray_start_args=["--object-store-memory=200000000"],
            )
        )
        [second_node] = session.wait_for_worker_allocation(second, timeout=300)
        leader.request_drain()
        drain_marker = shlex.quote(f"{leader.working_dir}/drain_signal")
        _wait_for(
            lambda: (
                pool.run(f"test -f {drain_marker} && echo yes || true").strip() == "yes"
            ),
            timeout=60,
        )
        new_leader = sibling.replace_head(timeout=300)
        assert new_leader.slurm_job_id == second.slurm_job_id
        assert session.wait_for_worker_allocation(second, timeout=300)
        final = _run_driver(
            pool,
            new_leader,
            activation,
            address_of(new_leader),
            driver,
            "joined",
            f"{run_dir}/3",
        )
        assert final["host"] == second_node and final["cpus"] == 1
        # The phased-out leader is released once the head has moved.
        _wait_for_state(session, first.slurm_job_id, TERMINAL_STATES, 180)
        assert session._get_job_state(second.slurm_job_id) == "RUNNING"
    finally:
        sibling.teardown_after_execution(cast(Any, context))
        session.teardown_after_execution(cast(Any, context))
