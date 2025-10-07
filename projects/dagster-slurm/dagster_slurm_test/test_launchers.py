"""Tests for launchers."""

from dagster_slurm.launchers import (
    BashLauncher,
    RayLauncher,
    SparkLauncher,
)


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


def test_bash_launcher_with_allocation():
    """Test bash launcher with allocation context."""
    launcher = BashLauncher()

    allocation_context = {
        "nodes": ["node1", "node2", "node3"],
        "num_nodes": 3,
        "head_node": "node1",
        "slurm_job_id": 12345,
    }

    plan = launcher.prepare_execution(
        payload_path="/path/to/script.py",
        python_executable="python3",
        working_dir="/tmp/test",
        pipes_context={"DAGSTER_PIPES_CONTEXT": "test"},
        allocation_context=allocation_context,
    )

    script = "\n".join(plan.payload)
    assert "SLURM_ALLOCATION_NODES=" in script
    assert "node1,node2,node3" in script
    assert "SLURM_ALLOCATION_NUM_NODES=" in script


def test_ray_launcher_local_mode():
    """Test Ray launcher in local mode."""
    launcher = RayLauncher(num_gpus_per_node=0, dashboard_port=8265)

    plan = launcher.prepare_execution(
        payload_path="/path/to/script.py",
        python_executable="python3",
        working_dir="/tmp/test",
        activation_script="env/activate.sh",
        pipes_context={"DAGSTER_PIPES_CONTEXT": "test"},
    )

    script = "\n".join(plan.payload)
    assert "ray start --head" in script
    assert "dashboard-port=8265" in script
    assert 'RAY_ADDRESS="auto"' in script


def test_ray_launcher_cluster_mode():
    """Test Ray launcher in cluster mode."""
    launcher = RayLauncher(num_gpus_per_node=1)

    allocation_context = {
        "nodes": ["node1", "node2"],
        "num_nodes": 2,
        "head_node": "node1",
        "slurm_job_id": 12345,
    }

    plan = launcher.prepare_execution(
        payload_path="/path/to/script.py",
        python_executable="python3",
        working_dir="/tmp/test",
        pipes_context={"DAGSTER_PIPES_CONTEXT": "test"},
        activation_script="env/activate.sh",
        allocation_context=allocation_context,
    )

    script = "\n".join(plan.payload)
    assert "HEAD_NODE=" in script
    assert "ray start --head" in script
    assert "ray start --address=" in script
    assert "--num-gpus=1" in script


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


def test_spark_launcher_cluster_mode():
    """Test Spark launcher in cluster mode."""
    launcher = SparkLauncher(spark_home="/opt/spark")

    allocation_context = {
        "nodes": ["node1", "node2", "node3"],
        "num_nodes": 3,
        "head_node": "node1",
        "slurm_job_id": 12345,
    }

    plan = launcher.prepare_execution(
        payload_path="/path/to/script.py",
        python_executable="python3",
        working_dir="/tmp/test",
        pipes_context={"DAGSTER_PIPES_CONTEXT": "test"},
        allocation_context=allocation_context,
    )

    script = "\n".join(plan.payload)
    assert "start-master.sh" in script
    assert "start-worker.sh" in script
