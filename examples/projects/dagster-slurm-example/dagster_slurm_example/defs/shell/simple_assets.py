"""Simple assets using Bash launcher."""

import dagster as dg
from dagster_slurm import ComputeResource


@dg.asset
def process_data(
    context: dg.AssetExecutionContext,
    compute: ComputeResource,
) -> dg.Output:
    """
    Process data using bash script.
    Works in all modes (dev/staging/prod) without code changes.
    """
    # Path to your processing script
    script_path = dg.file_relative_path(
        __file__,
        "../../../../dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/shell/process.py",
    )
    # Run via compute resource
    results = list(
        compute.run(
            context=context,
            payload_path=script_path,
            extra_env={
                "INPUT_DATA": "/path/to/input",
                "OUTPUT_DATA": "/path/to/output",
            },
        )
    )
    return dg.Output(
        value={"status": "completed"},
        metadata={
            "deployment": compute.mode,
            "events_count": len(results),
        },
    )


@dg.asset
def aggregate_results(
    context: dg.AssetExecutionContext,
    compute: dg.ComputeResource,
    process_data,  # Dependency
) -> dg.Output:
    """Aggregate results from processing."""
    script_path = dg.file_relative_path(
        __file__,
        "../../../../dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/shell/aggregate.py",
    )
    _ = list(
        compute.run(
            context=context,
            payload_path=script_path,
            extra_env={
                "PROCESSED_DATA": "/path/to/processed",
            },
        )
    )
    return dg.Output(value={"status": "aggregated"})
