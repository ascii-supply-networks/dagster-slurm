import pytest
from pathlib import Path

from dagster import asset, materialize, AssetExecutionContext

from dagster_slurm import ComputeResource

pytestmark = pytest.mark.needs_slurm_docker

# =========================================================================
# BASH LAUNCHER INTEGRATION TESTS
# =========================================================================


def test_slurm_bash_single_node(
    slurm_bash_compute_resource: ComputeResource, temp_dir: Path
):
    """Tests a simple asset using the BashLauncher on a single SLURM node."""
    payload_path = temp_dir / "hostname_payload.py"
    payload_path.write_text(
        """
from dagster_pipes import open_dagster_pipes
import socket
with open_dagster_pipes() as context:
    hostname = socket.gethostname()
    context.log.info(f"Executing on {hostname}")
    context.report_asset_materialization(metadata={"hostname": hostname})
"""
    )

    @asset
    def slurm_bash_asset(context: AssetExecutionContext, compute: ComputeResource):
        yield from compute.run(
            context=context,
            payload_path=str(payload_path),
            extra_slurm_opts={"nodes": 1, "cpus-per-task": 1, "time": "00:02:00"},
        )

    result = materialize(
        [slurm_bash_asset],
        resources={"compute": slurm_bash_compute_resource},
    )

    assert result.success

    # --- CORRECTED ASSERTION ---
    materialization_events = result.get_asset_materialization_events()
    assert len(materialization_events) == 1
    mat_event = materialization_events[0]
    assert mat_event.asset_key is not None
    assert mat_event.asset_key.to_user_string() == "slurm_bash_asset"
    metadata = mat_event.materialization.metadata
    assert "hostname" in metadata
    assert metadata["hostname"].value in ["c1", "c2"]


# =========================================================================
# RAY LAUNCHER INTEGRATION TESTS
# =========================================================================


def test_slurm_ray_cluster(slurm_ray_compute_resource: ComputeResource, temp_dir: Path):
    """Tests an asset using the RayLauncher on a multi-node SLURM cluster."""
    payload_path = temp_dir / "ray_payload.py"
    payload_path.write_text(
        """
from dagster_pipes import open_dagster_pipes
import ray
import time
with open_dagster_pipes() as context:
    # Adding a retry loop for ray.init() as cluster startup can have delays
    for _ in range(3):
        try:
            ray.init()
            break
        except ConnectionError:
            time.sleep(5)
    else:
        raise RuntimeError("Failed to connect to Ray cluster")
        
    num_nodes = len(ray.nodes())
    cpu_resources = ray.cluster_resources().get("CPU", 0)
    context.log.info(f"Ray cluster started with {num_nodes} nodes.")
    context.report_asset_materialization(
        metadata={"num_ray_nodes": num_nodes, "total_cpus": cpu_resources}
    )
"""
    )

    @asset
    def slurm_ray_asset(context: AssetExecutionContext, compute: ComputeResource):
        yield from compute.run(
            context=context,
            payload_path=str(payload_path),
            extra_slurm_opts={"nodes": 2, "cpus-per-task": 2, "time": "00:05:00"},
        )

    result = materialize(
        [slurm_ray_asset],
        resources={"compute": slurm_ray_compute_resource},
    )

    assert result.success

    materialization_events = result.get_asset_materialization_events()
    assert len(materialization_events) == 1
    mat_event = materialization_events[0]
    assert mat_event.asset_key is not None
    assert mat_event.asset_key.to_user_string() == "slurm_ray_asset"
    metadata = mat_event.materialization.metadata

    assert metadata["num_ray_nodes"].value == 2
    assert metadata["total_cpus"].value >= 4


# TODO: Add a similar test for SparkLauncher (`test_slurm_spark_cluster`)
# TODO: Add integration tests for session mode and HET job execution
