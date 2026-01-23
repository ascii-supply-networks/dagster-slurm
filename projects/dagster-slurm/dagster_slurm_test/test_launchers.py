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
        python_executable="python3",
        working_dir="/tmp/test",
        activation_script="env/activate.sh",
        pipes_context={"DAGSTER_PIPES_CONTEXT": "test"},
    )

    script = "\n".join(plan.payload)
    assert "ray start --head" in script
    assert 'dash_port="8265"' in script
    assert "--dashboard-port=$dash_port" in script


def test_ray_launcher_cluster_standalone_mode():
    """
    Tests the RayLauncher's ability to generate a script for a standalone,
    multi-node sbatch job (i.e., NON-session mode).
    """
    launcher = RayLauncher(num_gpus_per_node=2)

    plan = launcher.prepare_execution(
        payload_path="/path/to/script.py",
        python_executable="python3",
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
    assert "python3 /path/to/script.py" in driver_script

    assert "--address=$ip_head" in worker_script
    assert "--num-gpus=2" in worker_script
    assert "--node-ip-address" not in worker_script


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
