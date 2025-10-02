"""Assets using Ray launcher for distributed compute."""

import dagster as dg
from dagster_slurm import ComputeResource, RayLauncher


@dg.asset
def distributed_training(
    context: dg.AssetExecutionContext,
    compute: ComputeResource,
) -> dg.Output:
    """
    Train model using Ray for distributed compute.
    In dev: starts local Ray
    In prod session: starts Ray cluster across allocated nodes
    """
    script_path = dg.file_relative_path(
        __file__,
        "../../../../dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/ray/train_ray.py",
    )
    # Use Ray launcher instead of default Bash
    ray_launcher = RayLauncher(
        activate_sh=compute.slurm.activate_sh if compute.slurm else None,
        num_gpus_per_node=0,  # Set to >0 if using GPUs
    )
    _ = list(
        compute.run(
            context=context,
            payload_path=script_path,
            launcher=ray_launcher,
            extra_env={
                "MODEL_CONFIG": "config.yaml",
                "CHECKPOINT_DIR": "/path/to/checkpoints",
            },
        )
    )
    return dg.Output(
        value={"model_path": "/path/to/model"},
        metadata={"framework": "ray"},
    )


@dg.asset
def distributed_inference(
    context: dg.AssetExecutionContext,
    compute: ComputeResource,
    distributed_training,  # Uses trained model
) -> dg.Output:
    """
    Run inference using Ray.
    In session mode, this reuses the same Ray cluster from training!
    """
    script_path = dg.file_relative_path(
        __file__,
        "../../../../dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/ray/infer_ray.py",
    )
    ray_launcher = RayLauncher(
        activate_sh=compute.slurm.activate_sh if compute.slurm else None,
        num_gpus_per_node=0,
    )
    _ = list(
        compute.run(
            context=context,
            payload_path=script_path,
            launcher=ray_launcher,
            extra_env={
                "MODEL_PATH": distributed_training["model_path"],
                "INPUT_DATA": "/path/to/input",
            },
        )
    )
    return dg.Output(value={"predictions_path": "/path/to/predictions"})
