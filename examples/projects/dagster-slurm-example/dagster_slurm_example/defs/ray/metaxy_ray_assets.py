"""Example 2: metaxy + dagster + Ray integration with DeltaLake store.

Demonstrates metaxy incremental processing with Ray Data on HPC:
- `input_texts`: Root asset that registers text samples in metaxy (runs dagster-side).
- `compute_embeddings`: Delegates embedding computation to HPC via Ray,
  using MetaxyDatasource/MetaxyDatasink for incremental reads/writes.

In development mode, Ray runs locally. In production, it spawns a
multi-node Ray cluster on Slurm. The HPC workload uses a deployment-
specific metaxy store (dev vs prod) for distributed access.
"""

import os

import dagster as dg
import metaxy.ext.dagster as mxd
from dagster_slurm import BashLauncher, ComputeResource, RayLauncher, SlurmRunConfig


def _ray_store_name_for_deployment(deployment: str) -> str:
    deployment_lc = deployment.lower()
    return (
        "ray_embeddings_prod"
        if (
            "supercomputer" in deployment_lc
            or "staging" in deployment_lc
            or "production" in deployment_lc
            or "prod" in deployment_lc
        )
        else "ray_embeddings_dev"
    )


@mxd.metaxify
@dg.asset(
    metadata={"metaxy/feature": "ray_example/input_texts"},
    group_name="metaxy_ray",
)
def input_texts(
    context: dg.AssetExecutionContext,
    compute_ray: ComputeResource,
    config: SlurmRunConfig,
):
    """Register input_texts in the same runtime/store context as HPC workloads.

    Avoids host-local metaxy writes that may not be visible to remote Slurm jobs.
    """
    script_path = dg.file_relative_path(
        __file__,
        "../../../../dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/ray/register_input_texts_metaxy.py",
    )
    metaxy_config = dg.file_relative_path(__file__, "../../../../../metaxy.toml")
    deployment = os.getenv("DAGSTER_DEPLOYMENT", "development")
    ray_store_name = _ray_store_name_for_deployment(deployment)

    context.log.info(
        f"Registering ray_example/input_texts via launched workload.\n"
        f"  Deployment: {deployment}\n"
        f"  Store: {ray_store_name}"
    )

    completed_run = compute_ray.run(
        context=context,
        payload_path=script_path,
        config=config,
        launcher=BashLauncher(),
        extra_files=[metaxy_config],
        extra_env={
            "METAXY_STORE": ray_store_name,
            "DAGSTER_DEPLOYMENT": deployment,
            "SLURM_SUPERCOMPUTER_SITE": os.getenv("SLURM_SUPERCOMPUTER_SITE", ""),
        },
        extra_slurm_opts={
            "nodes": 1,
            "cpus_per_task": 1,
            "mem": "2G",
        },
    )
    yield from completed_run.get_results()


@mxd.metaxify
@dg.asset(
    metadata={"metaxy/feature": "ray_example/embeddings"},
    deps=[dg.AssetKey(["ray_example", "input_texts"])],
    group_name="metaxy_ray",
)
def compute_embeddings(
    context: dg.AssetExecutionContext,
    compute_ray: ComputeResource,
    config: SlurmRunConfig,
):
    """Compute text embeddings using Ray Data with metaxy incremental processing.

    Delegates to the HPC workload script which:
    1. Reads only new/stale samples via MetaxyDatasource(incremental=True)
    2. Computes embeddings with Ray map_batches()
    3. Writes results via MetaxyDatasink

    The HPC workload uses the deployment-selected ray_embeddings store for
    distributed access across Ray workers.
    """
    script_path = dg.file_relative_path(
        __file__,
        "../../../../dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/ray/compute_embeddings_metaxy.py",
    )
    metaxy_config = dg.file_relative_path(__file__, "../../../../../metaxy.toml")

    ray_launcher = RayLauncher(
        num_gpus_per_node=0,
    )

    deployment = os.getenv("DAGSTER_DEPLOYMENT", "development")
    ray_store_name = _ray_store_name_for_deployment(deployment)

    context.log.info(
        f"Starting metaxy-enabled embedding computation...\n"
        f"  Deployment: {deployment}\n"
        f"  Feature: ray_example/embeddings\n"
        f"  Store: {ray_store_name}"
    )

    completed_run = compute_ray.run(
        context=context,
        payload_path=script_path,
        config=config,
        launcher=ray_launcher,
        extra_files=[metaxy_config],
        extra_env={
            "METAXY_STORE": ray_store_name,
            "DAGSTER_DEPLOYMENT": deployment,
            "SLURM_SUPERCOMPUTER_SITE": os.getenv("SLURM_SUPERCOMPUTER_SITE", ""),
        },
        extra_slurm_opts={
            "nodes": 1,
            "cpus_per_task": 2,
            "mem": "4G",
        },
    )

    yield from completed_run.get_results()
