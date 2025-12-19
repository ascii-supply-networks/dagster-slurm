"""Assets using Ray launcher for distributed compute."""

import dagster as dg
from dagster_slurm import ComputeResource, RayLauncher, SlurmRunConfig


@dg.asset
def distributed_training(
    context: dg.AssetExecutionContext,
    compute_ray: ComputeResource,
    config: SlurmRunConfig,
):
    """Train model using Ray for distributed compute.
    In dev: starts local Ray
    In prod session: starts Ray cluster across allocated nodes.
    """
    script_path = dg.file_relative_path(
        __file__,
        "../../../../dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/ray/train_ray.py",
    )

    # Use Ray launcher - don't pass activation_script here
    ray_launcher = RayLauncher(
        num_gpus_per_node=0,  # Set to >0 if using GPUs
    )

    completed_run = compute_ray.run(
        context=context,
        payload_path=script_path,
        config=config,
        launcher=ray_launcher,
        extra_env={
            "MODEL_CONFIG": "config.yaml",
            "CHECKPOINT_DIR": "/path/to/checkpoints",
        },
        extra_slurm_opts={
            "nodes": 1,  # Single node = local Ray mode
            "cpus_per_task": 2,
            "mem": "4G",
        },
    )
    yield from completed_run.get_results()


@dg.asset(deps=[distributed_training])
def distributed_inference(
    context: dg.AssetExecutionContext,
    compute_ray: ComputeResource,
    config: SlurmRunConfig,
):
    """Run inference using Ray.
    In session mode, this reuses the same Ray cluster from training!
    """
    script_path = dg.file_relative_path(
        __file__,
        "../../../../dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/ray/infer_ray.py",
    )

    event_record = context.instance.get_latest_materialization_event(
        asset_key=distributed_training.key
    )

    # 2. Check that the materialization and metadata exist.
    if (
        not event_record
        or not event_record.asset_materialization
        or not event_record.asset_materialization.metadata
        or "model_path" not in event_record.asset_materialization.metadata
    ):
        # If any part of the chain is missing, raise a clear error.
        raise dg.DagsterError(
            f"Could not find `model_path` in the metadata of the latest "
            f"materialization of upstream asset '{distributed_training.key}'. "
            "Please ensure the upstream asset has been materialized and reports this metadata."
        )

    # 3. If we get here, we know all objects exist. Extract the path.
    model_path = event_record.asset_materialization.metadata["model_path"].value
    context.log.info(f"Found upstream model path from metadata: {model_path}")

    ray_launcher = RayLauncher(
        num_gpus_per_node=0,
    )

    completed_run = compute_ray.run(
        context=context,
        payload_path=script_path,
        config=config,
        launcher=ray_launcher,
        extra_env={
            "MODEL_PATH": model_path,
            "INPUT_DATA": "/path/to/input",
        },
    )

    yield from completed_run.get_results()
