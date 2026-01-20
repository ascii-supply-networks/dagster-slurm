"""Assets for distributed document processing using docling and Ray.

This example demonstrates how to use dagster-slurm with Ray for large-scale
document processing workflows. In development, it runs locally; in production
on Slurm clusters, it spawns a multi-node Ray cluster for parallel processing.
"""

import dagster as dg
from dagster_slurm import ComputeResource, RayLauncher, SlurmRunConfig


@dg.asset(
    description="Process PDF documents using docling with distributed Ray workers",
)
def process_documents_with_docling(
    context: dg.AssetExecutionContext,
    compute_ray: ComputeResource,
    config: SlurmRunConfig,
):
    """Process PDF documents using docling and Ray for parallel conversion.

    This asset demonstrates:
    - Distributed document processing with Ray Data
    - Converting PDFs to markdown using docling
    - Seamless local dev vs Slurm production execution
    - Multi-node Ray cluster orchestration on HPC

    In local mode: Starts a local Ray instance
    In Slurm mode: Spawns Ray cluster across allocated nodes

    Args:
        context: Dagster execution context
        compute_ray: ComputeResource configured with RayLauncher
        config: Slurm run configuration
    """
    script_path = dg.file_relative_path(
        __file__,
        "../../../../dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/ray/process_documents_docling.py",
    )

    # Configure Ray launcher for document processing
    # Adjust num_nodes and resources based on your workload
    ray_launcher = RayLauncher(
        num_gpus_per_node=0,  # Set to >0 if using GPU-accelerated OCR
    )

    context.log.info("Starting distributed document processing with docling...")

    completed_run = compute_ray.run(
        context=context,
        payload_path=script_path,
        config=config,
        launcher=ray_launcher,
        extra_env={
            # Document processing configuration
            "INPUT_GLOB": "data/**/*.pdf",  # Adjust to your data location
            "OUTPUT_DIR": "/tmp/docling_output",  # Output directory for processed docs
            "NUM_WORKERS": "2",  # Number of parallel workers
            "BATCH_SIZE": "4",  # Documents per batch
        },
        extra_slurm_opts={
            # Resource allocation for document processing
            # Single node = local Ray mode
            # Multi-node = distributed Ray cluster
            "nodes": 1,  # Increase for larger workloads
            "cpus_per_task": 4,  # CPUs per Ray worker
            "mem": "8G",  # Memory per node
            # Uncomment for GPU-accelerated OCR:
            # "gres": "gpu:1",
        },
    )

    yield from completed_run.get_results()


@dg.asset(
    deps=[process_documents_with_docling],
    description="Aggregate and analyze docling processing results",
)
def analyze_docling_results(  # noqa: C901
    context: dg.AssetExecutionContext,
):
    """Analyze results from document processing and generate summary statistics.

    This asset demonstrates downstream processing of docling results,
    showing how to chain multiple assets in a document processing pipeline.

    Args:
        context: Dagster execution context
    """
    # Get metadata from upstream asset
    event_record = context.instance.get_latest_materialization_event(
        asset_key=process_documents_with_docling.key
    )

    if (
        not event_record
        or not event_record.asset_materialization
        or not event_record.asset_materialization.metadata
    ):
        raise dg.DagsterError(
            f"Could not find metadata from upstream asset '{process_documents_with_docling.key}'. "
            "Please ensure the upstream asset has been materialized."
        )

    metadata = event_record.asset_materialization.metadata

    # Extract metadata values properly
    def get_int_value(key: str, default: int = 0) -> int:
        val = metadata.get(key)
        if val is None:
            return default
        return int(val.value) if hasattr(val, "value") else default

    def get_str_value(key: str, default: str = "") -> str:
        val = metadata.get(key)
        if val is None:
            return default
        return str(val.value) if hasattr(val, "value") else default

    total_docs = get_int_value("total_documents", 0)
    successful = get_int_value("successful", 0)
    failed = get_int_value("failed", 0)
    output_dir = get_str_value("output_directory", "")

    context.log.info(
        f"Analyzing results: {successful}/{total_docs} documents processed successfully, "
        f"{failed} failed. Output: {output_dir}"
    )

    # Calculate success rate
    success_rate = (successful / total_docs * 100) if total_docs > 0 else 0.0

    context.log.info(f"Analysis complete: {success_rate:.2f}% success rate")

    # Return materialization with analysis results
    yield dg.Output(
        value=None,
        metadata={
            "success_rate_percent": round(success_rate, 2),
            "total_documents": total_docs,
            "successful": successful,
            "failed": failed,
            "output_directory": output_dir,
            "status": "healthy" if success_rate > 95 else "needs_attention",
        },
    )


@dg.asset(
    description="Multi-node docling processing for large document collections",
)
def process_large_document_collection(
    context: dg.AssetExecutionContext,
    compute_ray: ComputeResource,
    config: SlurmRunConfig,
):
    """Process a large collection of documents using multi-node Ray cluster.

    This asset demonstrates scaling up document processing to multiple nodes
    for handling large document collections that benefit from HPC resources.

    Args:
        context: Dagster execution context
        compute_ray: ComputeResource configured with RayLauncher
        config: Slurm run configuration
    """
    script_path = dg.file_relative_path(
        __file__,
        "../../../../dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/ray/process_documents_docling.py",
    )

    # Configure multi-node Ray cluster
    ray_launcher = RayLauncher(
        num_gpus_per_node=0,
    )

    context.log.info(
        "Starting large-scale document processing with multi-node Ray cluster..."
    )

    completed_run = compute_ray.run(
        context=context,
        payload_path=script_path,
        config=config,
        launcher=ray_launcher,
        extra_env={
            "INPUT_GLOB": "data/large_collection/**/*.pdf",
            "OUTPUT_DIR": "/tmp/docling_large_output",
            "NUM_WORKERS": "8",  # More workers for multi-node
            "BATCH_SIZE": "8",  # Larger batches
        },
        extra_slurm_opts={
            # Multi-node configuration
            "nodes": 4,  # Multiple nodes for distributed processing
            "cpus_per_task": 4,
            "mem": "16G",
            "time": "02:00:00",  # 2-hour time limit
        },
    )

    yield from completed_run.get_results()
