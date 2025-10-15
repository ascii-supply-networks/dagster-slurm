"""Assets using Ray launcher for distributed compute."""

import dagster as dg
from dagster_slurm import ComputeResource, RayLauncher


@dg.asset
def distributed_training(
    context: dg.AssetExecutionContext,
    compute_ray: ComputeResource,
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


@dg.asset
def distributed_inference(
    context: dg.AssetExecutionContext,
    compute_ray: ComputeResource,
    distributed_training,  # Uses trained model
):
    """Run inference using Ray.
    In session mode, this reuses the same Ray cluster from training!
    """
    script_path = dg.file_relative_path(
        __file__,
        "../../../../dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/ray/infer_ray.py",
    )

    ray_launcher = RayLauncher(
        num_gpus_per_node=0,
    )

    completed_run = compute_ray.run(
        context=context,
        payload_path=script_path,
        launcher=ray_launcher,
        extra_env={
            "MODEL_PATH": distributed_training["model_path"],
            "INPUT_DATA": "/path/to/input",
        },
    )

    yield from completed_run.get_results()
