"""Tests for launchers."""

import os
import shutil
import socket
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import pytest
from dagster_slurm.helpers.ray_dashboard import (
    RayDashboardLogEmitter,
    ray_dashboard_url_from_line,
)
from dagster_slurm.launchers import (
    BashLauncher,
    RayLauncher,
    RayPortConfig,
    SparkLauncher,
)
from dagster_slurm.runners.local_runner import LocalRunner


@dataclass
class _FakeRayLaunch:
    process: subprocess.Popen[str]
    args_path: Path
    pid_path: Path
    output_path: Path

    def args(self) -> list[str]:
        return self.args_path.read_text(encoding="utf-8").splitlines()

    def output_lines(self) -> list[str]:
        return self.output_path.read_text(encoding="utf-8").splitlines()


class _FakeRayRuntime:
    def __init__(self, root: Path):
        self.root = root
        self.processes: list[subprocess.Popen[str]] = []
        fake_bin = root / "bin"
        fake_bin.mkdir()
        fake_ray = fake_bin / "ray"
        fake_ray.write_text(
            """#!/bin/bash
set -euo pipefail
command_name="$1"
shift
if [[ "$command_name" == "status" ]]; then exit 0; fi
if [[ "$command_name" != "start" ]]; then exit 2; fi
printf '%s\\n' "$@" > "$FAKE_RAY_ARGS"
printf '%s\\n' "$$" > "$FAKE_RAY_PID"
trap 'exit 0' TERM INT
while true; do sleep 0.05; done
""",
            encoding="utf-8",
        )
        fake_ray.chmod(0o755)
        self.fake_bin = fake_bin
        self.payload = root / "payload.sh"
        self.payload.write_text('sleep "$PAYLOAD_SLEEP"\n', encoding="utf-8")

    def start(
        self,
        launcher: RayLauncher,
        name: str,
        *,
        seed: int,
        payload_sleep: float,
    ) -> _FakeRayLaunch:
        working_dir = self.root / name
        working_dir.mkdir()
        plan = launcher.prepare_execution(
            payload_path=str(self.payload),
            python_executable="/bin/bash",
            working_dir=str(working_dir),
            pipes_context={},
            extra_env={
                "PAYLOAD_SLEEP": str(payload_sleep),
                "RAY_PORT_SEED": str(seed),
            },
        )
        launch_script = working_dir / "launch.sh"
        launch_script.write_text("\n".join(plan.payload), encoding="utf-8")
        args_path = working_dir / "ray-args"
        pid_path = working_dir / "ray-pid"
        output_path = working_dir / "launcher.log"
        process_env = {
            **os.environ,
            "FAKE_RAY_ARGS": str(args_path),
            "FAKE_RAY_PID": str(pid_path),
            "PATH": f"{self.fake_bin}:{os.environ['PATH']}",
        }
        with output_path.open("w", encoding="utf-8") as output:
            process = subprocess.Popen(
                ["bash", str(launch_script)],
                cwd=working_dir,
                env=process_env,
                stdout=output,
                stderr=subprocess.STDOUT,
                text=True,
            )
        self.processes.append(process)
        return _FakeRayLaunch(process, args_path, pid_path, output_path)

    def close(self) -> None:
        for process in self.processes:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=15)


@pytest.fixture
def fake_ray_runtime(tmp_path: Path) -> Iterator[_FakeRayRuntime]:
    runtime = _FakeRayRuntime(tmp_path)
    try:
        yield runtime
    finally:
        runtime.close()


def test_bash_launcher_basic():
    """Test basic bash launcher."""
    launcher = BashLauncher()

    plan = launcher.prepare_execution(
        payload_path="/path/to/script.py",
        python_executable="python3",
        working_dir="/tmp/test",
        pipes_context={"DAGSTER_PIPES_CONTEXT": "test"},
        extra_env={"FOO": "bar"},
    )

    assert plan.kind == "shell_script"
    assert isinstance(plan.payload, list)
    assert "#!/bin/bash" in plan.payload
    assert any("export DAGSTER_PIPES_CONTEXT=" in line for line in plan.payload)
    assert any("export FOO=" in line for line in plan.payload)
    assert any("python3" in line for line in plan.payload)


def test_bash_launcher_prefers_packed_native_libraries():
    """Load native libraries from the packed environment."""
    launcher = BashLauncher()

    plan = launcher.prepare_execution(
        payload_path="/remote/script.py",
        python_executable="/remote/env/bin/python",
        working_dir="/remote/run",
        activation_script="/remote/activate.sh",
        pipes_context={},
    )

    script = "\n".join(plan.payload)
    assert "source /remote/activate.sh" in script
    assert "export LD_LIBRARY_PATH=/remote/env/lib:${LD_LIBRARY_PATH:-}" in script
    assert script.index("source /remote/activate.sh") < script.index(
        "export LD_LIBRARY_PATH=/remote/env/lib:${LD_LIBRARY_PATH:-}"
    )


# TODO: Implement session mode, HET job
# def test_bash_launcher_with_allocation():
#     """Test bash launcher with allocation context."""
#     launcher = BashLauncher()

#     allocation_context = {
#         "nodes": ["node1", "node2", "node3"],
#         "num_nodes": 3,
#         "head_node": "node1",
#         "slurm_job_id": 12345,
#     }

#     plan = launcher.prepare_execution(
#         payload_path="/path/to/script.py",
#         python_executable="python3",
#         working_dir="/tmp/test",
#         pipes_context={"DAGSTER_PIPES_CONTEXT": "test"},
#         allocation_context=allocation_context,
#     )

#     script = "\n".join(plan.payload)
#     assert "SLURM_ALLOCATION_NODES=" in script
#     assert "node1,node2,node3" in script
#     assert "SLURM_ALLOCATION_NUM_NODES=" in script


def test_ray_launcher_local_mode():
    """Test Ray launcher in local mode."""
    launcher = RayLauncher(num_gpus_per_node=0, dashboard_port=8265)

    plan = launcher.prepare_execution(
        payload_path="/path/to/script.py",
        python_executable="/remote/env/bin/python",
        working_dir="/tmp/test",
        activation_script="env/activate.sh",
        pipes_context={"DAGSTER_PIPES_CONTEXT": "test"},
    )

    script = "\n".join(plan.payload)
    assert "ray start --head" in script
    assert 'dash_port="8265"' in script
    assert "--dashboard-port=$dash_port" in script
    assert "trap - EXIT SIGINT SIGTERM" in script
    assert 'exit "$exit_code"' in script
    assert "export LD_LIBRARY_PATH=/remote/env/lib:${LD_LIBRARY_PATH:-}" in script


def test_ray_launcher_existing_cluster_prefers_packed_native_libraries():
    """Load native libraries from the packed environment."""
    launcher = RayLauncher(ray_address="ray://head:10001")

    plan = launcher.prepare_execution(
        payload_path="/remote/script.py",
        python_executable="/remote/env/bin/python",
        working_dir="/remote/run",
        activation_script="/remote/activate.sh",
        pipes_context={},
    )

    script = "\n".join(plan.payload)
    assert script.index("source /remote/activate.sh") < script.index(
        "export LD_LIBRARY_PATH=/remote/env/lib:${LD_LIBRARY_PATH:-}"
    )


def test_concurrent_ray_launchers_use_disjoint_ports_and_isolated_cleanup(
    fake_ray_runtime: _FakeRayRuntime,
):
    launcher = RayLauncher(
        num_gpus_per_node=0,
        port_config=RayPortConfig(
            range_start=30000,
            range_end=31999,
            block_size=1000,
            lock_dir=str(fake_ray_runtime.root / "port-locks"),
        ),
        node_ip_address_command="printf '192.0.2.10'",
    )

    first = fake_ray_runtime.start(
        launcher,
        "first",
        seed=0,
        payload_sleep=0.1,
    )
    second = fake_ray_runtime.start(
        launcher,
        "second",
        seed=0,
        payload_sleep=8,
    )
    assert first.process.wait(timeout=15) == 0
    assert second.process.poll() is None
    os.kill(int(second.pid_path.read_text(encoding="utf-8")), 0)
    third = fake_ray_runtime.start(
        launcher,
        "third",
        seed=0,
        payload_sleep=0.1,
    )
    assert third.process.wait(timeout=15) == 0
    assert second.process.poll() is None

    first_args = first.args()
    second_args = second.args()
    third_args = third.args()
    assert "--node-ip-address=192.0.2.10" in first_args
    assert "--node-ip-address=192.0.2.10" in second_args
    first_ports = {
        int(argument.rsplit("=", 1)[1])
        for argument in first_args
        if "port=" in argument or "ports=" in argument
    }
    second_ports = {
        int(argument.rsplit("=", 1)[1])
        for argument in second_args
        if "port=" in argument or "ports=" in argument
    }
    third_ports = {
        int(argument.rsplit("=", 1)[1])
        for argument in third_args
        if "port=" in argument or "ports=" in argument
    }
    assert first_ports
    assert second_ports
    assert first_ports.isdisjoint(second_ports)
    assert {min(first_ports), min(second_ports)} == {30000, 31000}
    assert {max(first_ports), max(second_ports)} == {30999, 31999}
    assert third_ports == first_ports

    first_dashboard_url = next(
        filter(
            None,
            (ray_dashboard_url_from_line(line) for line in first.output_lines()),
        )
    )
    deadline = time.monotonic() + 10
    second_dashboard_url = None
    while time.monotonic() < deadline and second_dashboard_url is None:
        second_dashboard_url = next(
            filter(
                None,
                (ray_dashboard_url_from_line(line) for line in second.output_lines()),
            ),
            None,
        )
        if second_dashboard_url is None:
            time.sleep(0.05)
    assert second.process.poll() is None
    assert second_dashboard_url is not None
    assert int(first_dashboard_url.rsplit(":", 1)[1]) in first_ports
    assert int(second_dashboard_url.rsplit(":", 1)[1]) in second_ports
    assert second.process.wait(timeout=15) == 0


def test_random_ray_ports_skip_a_block_with_a_listener(
    fake_ray_runtime: _FakeRayRuntime,
):
    if shutil.which("ss") is None:
        pytest.skip("ss is required to probe occupied port blocks")

    listener = socket.socket()
    for occupied_base in range(20000, 30000, 1000):
        try:
            listener.bind(("0.0.0.0", occupied_base))
            listener.listen()
            break
        except OSError:
            continue
    else:
        listener.close()
        pytest.skip("no test port block is available")

    launcher = RayLauncher(
        port_config=RayPortConfig(
            range_start=20000,
            range_end=29999,
            block_size=1000,
            lock_dir=str(fake_ray_runtime.root / "port-locks"),
        ),
        node_ip_address_command="printf '192.0.2.10'",
    )
    try:
        launch = fake_ray_runtime.start(
            launcher,
            "occupied",
            seed=(occupied_base - 20000) // 1000,
            payload_sleep=0.1,
        )
        assert launch.process.wait(timeout=15) == 0
    finally:
        listener.close()

    assigned_ports = {
        int(argument.rsplit("=", 1)[1])
        for argument in launch.args()
        if "port=" in argument or "ports=" in argument
    }
    occupied_ports = range(occupied_base, occupied_base + 1000)
    assert assigned_ports.isdisjoint(occupied_ports)


def test_local_runner_logs_ray_dashboard_while_process_is_alive(tmp_path: Path):
    release_path = tmp_path / "release"
    observed_messages: list[str] = []
    dashboard_observed = threading.Event()

    def log_info(message: str) -> None:
        observed_messages.append(message)
        dashboard_observed.set()

    emitter = RayDashboardLogEmitter(log_info)
    runner = LocalRunner()
    runner_thread = threading.Thread(
        target=runner.execute_script,
        kwargs={
            "script_lines": [
                "#!/bin/bash",
                "set -euo pipefail",
                "echo DAGSTER_SLURM_RAY_DASHBOARD_URL=http://192.0.2.10:31005",
                f"while [[ ! -f {release_path} ]]; do sleep 0.05; done",
            ],
            "working_dir": str(tmp_path),
            "line_callback": emitter.process_line,
        },
    )
    runner_thread.start()
    try:
        assert dashboard_observed.wait(timeout=5)
        assert runner_thread.is_alive()
        assert observed_messages == ["Ray head node web UI: http://192.0.2.10:31005"]
    finally:
        release_path.touch()
        runner_thread.join(timeout=5)
    assert not runner_thread.is_alive()


def test_ray_launcher_cluster_standalone_mode():
    """
    Tests the RayLauncher's ability to generate a script for a standalone,
    multi-node sbatch job (i.e., NON-session mode).
    """
    launcher = RayLauncher(num_gpus_per_node=2)

    plan = launcher.prepare_execution(
        payload_path="/path/to/script.py",
        python_executable="/remote/env/bin/python",
        working_dir="/tmp/test",
        pipes_context={"DAGSTER_PIPES_CONTEXT": "test"},
        activation_script="env/activate.sh",
    )

    # --- Main Script Assertions ---
    main_script = "\n".join(plan.payload)
    assert (
        'if [[ -n "${SLURM_JOB_ID:-}" && "${SLURM_JOB_NUM_NODES:-1}" -gt 1 ]]; then'
        in main_script
    )
    assert "Detected multi-node Slurm allocation" in main_script
    assert 'srun --cpu-bind=none --nodes=1 --ntasks=1 -w "$head_node"' in main_script
    assert "else" in main_script
    assert "Single-node mode detected" in main_script

    # --- Auxiliary Script Assertions ---
    assert "ray_driver.sh" in plan.auxiliary_scripts
    assert "ray_worker.sh" in plan.auxiliary_scripts

    driver_script = plan.auxiliary_scripts["ray_driver.sh"]
    worker_script = plan.auxiliary_scripts["ray_worker.sh"]

    # 1. Assertions for the Driver Script
    assert (
        'head_node_name=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -n1)'
        in driver_script
    )
    assert "ray start --head" in driver_script
    assert "--node-ip-address=$head_bind_addr" in driver_script
    assert 'srun --cpu-bind=none --nodes=1 --ntasks=1 -w "$node_i"' in driver_script
    assert "/remote/env/bin/python /path/to/script.py" in driver_script
    assert "trap - EXIT SIGINT SIGTERM" in driver_script
    assert 'exit "$exit_code"' in driver_script
    assert (
        "export LD_LIBRARY_PATH=/remote/env/lib:${LD_LIBRARY_PATH:-}" in driver_script
    )

    assert "--address=$ip_head" in worker_script
    assert "--num-gpus=2" in worker_script
    assert "--node-ip-address" not in worker_script
    assert "trap - EXIT INT TERM" in worker_script
    assert 'exit "$exit_code"' in worker_script
    assert (
        "export LD_LIBRARY_PATH=/remote/env/lib:${LD_LIBRARY_PATH:-}" in worker_script
    )


# TODO: Implement session mode, HET job
# def test_ray_launcher_cluster_mode():
#     """Test Ray launcher in cluster mode."""
#     launcher = RayLauncher(num_gpus_per_node=1)

#     allocation_context = {
#         "nodes": ["node1", "node2"],
#         "num_nodes": 2,
#         "head_node": "node1",
#         "slurm_job_id": 12345,
#     }

#     plan = launcher.prepare_execution(
#         payload_path="/path/to/script.py",
#         python_executable="python3",
#         working_dir="/tmp/test",
#         pipes_context={"DAGSTER_PIPES_CONTEXT": "test"},
#         activation_script="env/activate.sh",
#         allocation_context=allocation_context,
#     )

#     script = "\n".join(plan.payload)
#     assert "HEAD_NODE=" in script
#     assert "ray start --head" in script
#     assert "ray start --address=" in script
#     assert "--num-gpus=1" in script


def test_spark_launcher_local_mode():
    """Test Spark launcher in local mode."""
    launcher = SparkLauncher(
        spark_home="/opt/spark",
        executor_memory="2g",
        executor_cores=2,
    )

    plan = launcher.prepare_execution(
        payload_path="/path/to/script.py",
        python_executable="python3",
        working_dir="/tmp/test",
        pipes_context={"DAGSTER_PIPES_CONTEXT": "test"},
    )

    script = "\n".join(plan.payload)
    assert 'SPARK_MASTER_URL="local[*]"' in script
    assert "spark-submit" in script
    assert "--executor-memory 2g" in script
    assert "--executor-cores 2" in script


# TODO: Implement once spark cluster mode launcher is completed
# def test_spark_launcher_cluster_standalone_mode():
#     """Tests the SparkLauncher's script for a standalone, multi-node sbatch job."""
#     launcher = SparkLauncher(spark_home="/opt/spark", executor_memory="8g")

#     plan = launcher.prepare_execution(
#         payload_path="/path/to/script.py",
#         python_executable="python3",
#         working_dir="/tmp/test",
#         pipes_context={"DAGSTER_PIPES_CONTEXT": "test"},
#         activation_script="env/activate.sh",
#     )

#     main_script = "\n".join(plan.payload)

#     assert 'if [[ -n "${SLURM_JOB_ID:-}" && "${SLURM_JOB_NUM_NODES:-1}" -gt 1 ]]; then' in main_script
#     assert "else" in main_script

#     assert "start-master.sh" in main_script
#     assert "start-worker.sh" in main_script

#     assert "spark-submit --master spark://$HEAD_NODE_IP:7077" in main_script
#     assert "--executor-memory 8g" in main_script

#     assert '--master "local[*]"' in main_script

# TODO: Implement session mode, HET job
# def test_spark_launcher_cluster_mode():
#     """Test Spark launcher in cluster mode."""
#     launcher = SparkLauncher(spark_home="/opt/spark")

#     allocation_context = {
#         "nodes": ["node1", "node2", "node3"],
#         "num_nodes": 3,
#         "head_node": "node1",
#         "slurm_job_id": 12345,
#     }

#     plan = launcher.prepare_execution(
#         payload_path="/path/to/script.py",
#         python_executable="python3",
#         working_dir="/tmp/test",
#         pipes_context={"DAGSTER_PIPES_CONTEXT": "test"},
#         allocation_context=allocation_context,
#     )

#     script = "\n".join(plan.payload)
#     assert "start-master.sh" in script
#     assert "start-worker.sh" in script
