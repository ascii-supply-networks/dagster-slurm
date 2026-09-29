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


def test_a_retried_run_adopts_workers_after_its_head_ended(tmp_path):
    pool = StartingPool()
    session = _head_only_session(tmp_path, pool)
    assert session.worker_allocation is not None
    session._ensure_worker_allocation(session.worker_allocation)
    pool.states.update({700: "NODE_FAIL", 701: "RUNNING"})

    retried = _head_only_session(tmp_path, pool)
    assert retried.allocation.slurm_job_id == 702
    assert retried.worker_allocation is not None
    retried._ensure_worker_allocation(retried.worker_allocation)
    assert pool.submitted_jobs == [700, 701, 702]
    assert [worker.slurm_job_id for worker in retried.worker_allocations] == [701]
    session_dir = Path(retried.allocation.session_dir)
    assert (session_dir / "ray_head").read_text() == "702\n"
    metadata = json.loads((session_dir / "allocation.json").read_text())
    assert metadata["worker_template"]["partition"] == "GPU-rtx6000"


def test_replace_head_fails_over_and_keeps_workers(tmp_path, monkeypatch):
    pool = StartingPool()
    session = _head_only_session(tmp_path, pool)
    assert session.worker_allocation is not None
    session._ensure_worker_allocation(session.worker_allocation)
    started_ray = []
    monkeypatch.setattr(
        SlurmAllocation,
        "_read_ray_start_options",
        lambda self, ssh_pool: {"launcher": RayLauncher(), "activation_script": "a"},
    )
    monkeypatch.setattr(
        SlurmAllocation,
        "ensure_ray_cluster",
        lambda self, **kwargs: started_ray.append(self.slurm_job_id),
    )
    pool.states[700] = "NODE_FAIL"

    head = session.replace_head(timeout=10)

    assert head.slurm_job_id == 702 and started_ray == [702]
    assert head.config.ray_head_only and head.config.partition == "cpu"
    assert session.allocation.slurm_job_id == 702
    assert pool.states[701] == "RUNNING"
    assert (Path(head.session_dir) / "ray_head").read_text() == "702\n"
    assert [worker.slurm_job_id for worker in session.worker_allocations] == [701]


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
        ray_head_allocation=SlurmRunAllocationConfig(partition="cpu"),
    )
    session = compute.get_run_allocation_session(dg.build_init_resource_context())
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
    with pytest.raises(ValueError, match="explicit partition"):
        ComputeResource(
            mode="slurm",
            slurm=gpu_queue,
            allocation_scope="run",
            default_launcher=RayLauncher(),
            ray_head_allocation=SlurmRunAllocationConfig(),
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
def test_slurm_killed_head_fails_over_and_keeps_its_worker_allocation(
    slurm_resource_for_testing, slurm_cluster_ready, request
):
    """A separate head allocation dies; the same worker job joins its replacement.

    The worker spans both Docker nodes with hash_jobid ports, so its nodes share
    one Ray temp dir path and must still publish and drain their own IDs.
    """
    session = SlurmSessionResource(
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
            mem="2G",
            time_limit="00:20:00",
            ray_resources={"elastic_worker": 1},
            # No --num-cpus: each node offers the one CPU Slurm granted.
            ray_start_args=["--object-store-memory=200000000"],
            rejoin_timeout=600,
        ),
    )
    context = SimpleNamespace(
        run=SimpleNamespace(run_id=f"failover_{uuid.uuid4().hex}", tags={}),
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
                port_strategy="hash_jobid",
            ),
            activation_script=activation,
            startup_timeout=180,
        )
        run_dir = f"{head.session_dir}/elastic"
        driver = f"{run_dir}/driver.py"
        pool.run(f"mkdir -p {shlex.quote(run_dir)}")
        pool.write_file(_DRIVER, driver)
        # setup_for_execution submitted the compute as the first worker.
        [worker] = session.worker_allocations
        first_ids = session.wait_for_worker_allocation(worker, timeout=300)
        assert sorted(first_ids) == sorted(worker.nodes) and len(first_ids) == 2
        assert len(set(first_ids.values())) == 2  # Each node found its own ID.
        joined = _run_driver(
            pool, head, activation, address, driver, "joined", f"{run_dir}/first"
        )
        assert joined["host"] in first_ids
        # One CPU per worker node, as granted by Slurm; the head offers none.
        assert joined["cpus"] == 2

        # Kill the head allocation, as a failing node would.
        pool.run(f"scancel {head.slurm_job_id}")
        _wait_for_state(session, head.slurm_job_id, TERMINAL_STATES, 120)
        new_head = session.replace_head(timeout=300)
        assert new_head.slurm_job_id != head.slurm_job_id
        assert new_head._ray_address is not None

        node_ids = session.wait_for_worker_allocation(worker, timeout=300)
        assert sorted(node_ids) == sorted(first_ids)
        assert len(set(node_ids.values())) == 2
        assert not set(node_ids.values()) & set(first_ids.values())
        # The same Slurm job kept its nodes through the failover.
        assert session._get_job_state(worker.slurm_job_id) == "RUNNING"
        rejoined = _run_driver(
            pool,
            new_head,
            activation,
            new_head._ray_address,
            driver,
            "joined",
            f"{run_dir}/second",
        )
        assert rejoined["host"] in node_ids
        assert sum(node["alive"] for node in rejoined["nodes"]) == 3

        # Each node drains its own Ray node, so the job ends without a deadline.
        state = session.remove_worker_allocation(worker, drain_timeout=120)
        assert state == "COMPLETED"
        for node in node_ids:
            log_path = f"{worker.working_dir}/ray_nodes/{node}.log"
            log = pool.run(f"cat {shlex.quote(log_path)}")
            assert "Ray node drained" in log
            assert "Drain deadline reached" not in log
    finally:
        session.teardown_after_execution(cast(Any, context))
