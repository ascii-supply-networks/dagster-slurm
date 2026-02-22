"""Example 2: metaxy + dagster + Ray integration with DeltaLake store.

Demonstrates metaxy incremental processing with Ray Data on HPC:
- `input_texts`: Root asset that registers text samples in metaxy (runs dagster-side).
- `compute_embeddings`: Delegates embedding computation to HPC via Ray,
  using MetaxyDatasource/MetaxyDatasink for incremental reads/writes.

In development mode, Ray runs locally. In production, it spawns a
multi-node Ray cluster on Slurm. The HPC workload uses metaxy's
DeltaLake store (METAXY_STORE=ray_embeddings) for distributed access.
"""

import os

import dagster as dg
import metaxy as mx
import metaxy.ext.dagster as mxd
import polars as pl
from dagster_slurm import ComputeResource, RayLauncher, SlurmRunConfig


@mxd.metaxify
@dg.asset(
    metadata={"metaxy/feature": "ray_example/input_texts"},
    group_name="metaxy_ray",
)
def input_texts(store_ray: dg.ResourceParam[mx.MetadataStore]) -> pl.DataFrame:
    """Register text samples in metaxy as a root feature.

    This lightweight asset runs on the dagster host (not HPC) and
    registers the input data. The heavy embedding computation is
    delegated to the downstream `compute_embeddings` asset.
    """
    samples = [
        {"sample_uid": f"text_{i:03d}", "text": text}
        for i, text in enumerate(
            [
                "The quick brown fox jumps over the lazy dog",
                "Machine learning enables pattern recognition",
                "High performance computing accelerates science",
                "Distributed systems process data in parallel",
                "Natural language processing understands text",
                "Data pipelines transform raw inputs to features",
                "Incremental processing saves compute resources",
                "Metadata tracking ensures reproducibility",
            ]
        )
    ]
    df = pl.DataFrame(samples)

    samples = df.with_columns(
        pl.struct(
            pl.col("text").cast(pl.String).alias("text"),
        ).alias("metaxy_provenance_by_field")
    )

    with store_ray:
        increment = store_ray.resolve_update("ray_example/input_texts", samples=samples)
        if len(increment.new) > 0:
            store_ray.write("ray_example/input_texts", increment.new)
        if len(increment.stale) > 0:
            store_ray.write("ray_example/input_texts", increment.stale)

    return samples


@mxd.metaxify
@dg.asset(
    metadata={"metaxy/feature": "ray_example/embeddings"},
    deps=[input_texts],
    group_name="metaxy_ray",
)
def compute_embeddings(
    context: dg.AssetExecutionContext,
    compute_ray: ComputeResource,
    config: SlurmRunConfig,
    store_ray: dg.ResourceParam[mx.MetadataStore],
):
    """Compute text embeddings using Ray Data with metaxy incremental processing.

    Delegates to the HPC workload script which:
    1. Reads only new/stale samples via MetaxyDatasource(incremental=True)
    2. Computes embeddings with Ray map_batches()
    3. Writes results via MetaxyDatasink

    The HPC workload uses the ray_embeddings store (DeltaLake) for
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

    context.log.info(
        f"Starting metaxy-enabled embedding computation...\n"
        f"  Deployment: {deployment}\n"
        f"  Feature: ray_example/embeddings"
    )

    with store_ray:
        increment = store_ray.resolve_update("ray_example/embeddings")
        new_count = len(increment.new)
        stale_count = len(increment.stale)
    total_count = new_count + stale_count

    if total_count == 0:
        context.log.info(
            "No new/stale records for ray_example/embeddings; skipping Ray launcher."
        )
        yield dg.MaterializeResult(
            metadata={
                "status": "up_to_date",
                "new_samples": new_count,
                "stale_samples": stale_count,
                "total_samples": total_count,
                "execution_mode": "skipped_external",
            }
        )
        return

    completed_run = compute_ray.run(
        context=context,
        payload_path=script_path,
        config=config,
        launcher=ray_launcher,
        extra_files=[metaxy_config],
        extra_env={
            "METAXY_STORE": "ray_embeddings",
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
