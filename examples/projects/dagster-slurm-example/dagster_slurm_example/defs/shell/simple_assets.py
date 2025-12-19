"""Simple assets using Bash launcher."""

import dagster as dg
from dagster_slurm import ComputeResource, SlurmRunConfig


@dg.asset
# see an example for per asset slurm resource overrides
# (
#         metadata={
#         "slurm": {
#             "cpus": 16,
#             "mem": "32G",
#             "gpus_per_node": 2,
#             "time_limit": "02:00:00",
#         }
#     }
# )
def process_data(
    context: dg.AssetExecutionContext,
    compute: ComputeResource,
    config: SlurmRunConfig,
):
    """Process data using bash script.
    Works in all modes (dev/staging/prod) without code changes.
    """
    # Path to your processing script
    script_path = dg.file_relative_path(
        __file__,
        "../../../../dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/shell/process.py",
    )
    # Run via compute resource
    completed_run = compute.run(
        context=context,
        payload_path=script_path,
        config=config,
        extra_env={
            "INPUT_DATA": "/path/to/input",
            "OUTPUT_DATA": "/path/to/output",
        },
        extras={
            "foo": "bar",
            "config": {"batch_size": 100},
        },
    )
    yield from completed_run.get_results()
    yield dg.AssetObservation(
        asset_key=context.asset_key,
        metadata={
            "deployment_mode": str(compute.mode),
            "custom_messages_count": len(completed_run.get_custom_messages()),
        },
    )


@dg.asset(deps=[process_data])
def aggregate_results(
    context: dg.AssetExecutionContext,
    compute: ComputeResource,
    config: SlurmRunConfig,
):
    """Aggregate results from processing."""
    script_path = dg.file_relative_path(
        __file__,
        "../../../../dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/shell/aggregate.py",
    )

    return compute.run(
        context=context,
        payload_path=script_path,
        config=config,
        extra_env={
            "PROCESSED_DATA": "/path/to/processed",
        },
    ).get_results()


@dg.multi_asset(
    specs=[
        dg.AssetSpec(key=["myprefix", "orders"]),
        dg.AssetSpec("users"),
    ],
)
def subprocess_asset(
    context: dg.AssetExecutionContext,
    compute: ComputeResource,
    config: SlurmRunConfig,
):
    """Multi asset example with tests.

    Uses SlurmRunConfig for launchpad-configurable options instead of hardcoded metadata.
    """
    script_path = dg.file_relative_path(
        __file__,
        "../../../../dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/shell/multi_asset_example.py",
    )
    return compute.run(
        context=context,
        payload_path=script_path,
        config=config,
        extras={"foo": "bar"},
        extra_env={
            "MY_ENV_VAR_IN_SUBPROCESS": "my_value",
        },
    ).get_results()


@dg.asset_check(
    asset=dg.AssetKey(["myprefix", "orders"]),
    blocking=True,
)
def no_empty_order_check(
    context: dg.AssetCheckExecutionContext,
    compute: ComputeResource,
) -> dg.AssetCheckResult:
    """asset check example"""
    script_path = dg.file_relative_path(
        __file__,
        "../../../../dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/shell/multi_asset_example_checking.py",
    )
    return compute.run(
        context=context,
        payload_path=script_path,
        extras={"foo": "bar"},
        extra_env={
            "MY_ENV_VAR_IN_SUBPROCESS": "my_value",
        },
    ).get_asset_check_result()


# # Define a job that runs assets as heterogeneous job
# from dagster import job, op, OpExecutionContext

# @op
# def run_ml_pipeline(context: OpExecutionContext, compute: ComputeResource):
#     """Run entire ML pipeline as one heterogeneous Slurm job."""

#     # Collect all assets with their resource requirements
#     assets = [
#         (
#             "data_prep",
#             "/path/to/data_prep.py",
#             {"nodes": 1, "cpus_per_task": 8, "mem": "32G", "time_limit": "00:30:00"}
#         ),
#         (
#             "training",
#             "/path/to/training.py",
#             {"nodes": 4, "cpus_per_task": 32, "mem": "128G", "gpus_per_node": 2, "time_limit": "02:00:00"}
#         ),
#         (
#             "inference",
#             "/path/to/inference.py",
#             {"nodes": 8, "cpus_per_task": 16, "mem": "64G", "gpus_per_node": 1, "time_limit": "01:00:00"}
#         ),
#     ]

#     # Submit as heterogeneous job
#     compute.run_job(context, assets=assets)

# @job
# def ml_pipeline_job():
#     run_ml_pipeline()
