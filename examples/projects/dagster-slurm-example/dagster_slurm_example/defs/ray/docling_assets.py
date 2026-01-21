"""Assets for distributed document processing using docling and Ray.

This example demonstrates how to use dagster-slurm with Ray for large-scale
document processing workflows. In development, it runs locally; in production
on Slurm clusters, it spawns a multi-node Ray cluster for parallel processing.
"""

import dagster as dg
from dagster_slurm import ComputeResource, RayLauncher, SlurmRunConfig
from pydantic import Field


class DoclingProcessingConfig(dg.Config):
    """Configuration for docling document processing."""

    input_glob: str = Field(
        default="/home/submitter/dagster-slurm-data/**/*.pdf",
        description="Glob pattern for input PDF files to process",
    )
    output_dir: str = Field(
        default="/tmp/docling_output",
        description="Directory where processed documents will be saved",
    )
    num_workers: int = Field(
        default=2,
        gt=0,
        le=64,
        description="Number of parallel Ray workers for processing",
    )
    batch_size: int = Field(
        default=4,
        gt=0,
        le=100,
        description="Number of documents to process per batch",
    )


class DoclingAssetConfig(dg.Config):
    """Combined config for docling processing and Slurm run settings."""

    slurm: SlurmRunConfig = Field(
        default_factory=SlurmRunConfig,
        description="Slurm run configuration",
    )
    docling: DoclingProcessingConfig = Field(
        default_factory=DoclingProcessingConfig,
        description="Document processing configuration",
    )


class LargeScaleDoclingConfig(dg.Config):
    """Configuration for large-scale multi-node docling processing."""

    input_glob: str = Field(
        default="/home/submitter/dagster-slurm-data/large_collection/**/*.pdf",
        description="Glob pattern for input PDF files to process",
    )
    output_dir: str = Field(
        default="/tmp/docling_large_output",
        description="Directory where processed documents will be saved",
    )
    num_workers: int = Field(
        default=8,
        gt=0,
        le=128,
        description="Number of parallel Ray workers for processing",
    )
    batch_size: int = Field(
        default=8,
        gt=0,
        le=200,
        description="Number of documents to process per batch",
    )


class LargeScaleDoclingAssetConfig(dg.Config):
    """Combined config for large-scale docling processing and Slurm run settings."""

    slurm: SlurmRunConfig = Field(
        default_factory=SlurmRunConfig,
        description="Slurm run configuration",
    )
    docling: LargeScaleDoclingConfig = Field(
        default_factory=LargeScaleDoclingConfig,
        description="Large-scale document processing configuration",
    )


@dg.asset(
    description="Process PDF documents using docling with distributed Ray workers",
)
def process_documents_with_docling(
    context: dg.AssetExecutionContext,
    compute_ray: ComputeResource,
    config: DoclingAssetConfig,
):
    """Process PDF documents using docling and Ray for parallel conversion.

    This asset demonstrates:
    - Distributed document processing with Ray Data
    - Converting PDFs to markdown using docling
    - Seamless local dev vs Slurm production execution
    - Multi-node Ray cluster orchestration on HPC
    - Configurable document processing parameters via Dagster config

    In local mode: Starts a local Ray instance
    In Slurm mode: Spawns Ray cluster across allocated nodes

    Args:
        context: Dagster execution context
        compute_ray: ComputeResource configured with RayLauncher
        config: Combined configuration (slurm + document processing)
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

    context.log.info(
        f"Starting distributed document processing with docling...\n"
        f"  Input: {config.docling.input_glob}\n"
        f"  Output: {config.docling.output_dir}\n"
        f"  Workers: {config.docling.num_workers}\n"
        f"  Batch size: {config.docling.batch_size}"
    )

    completed_run = compute_ray.run(
        context=context,
        payload_path=script_path,
        config=config.slurm,
        launcher=ray_launcher,
        extra_env={
            # Document processing configuration from Dagster config
            "INPUT_GLOB": config.docling.input_glob,
            "OUTPUT_DIR": config.docling.output_dir,
            "NUM_WORKERS": str(config.docling.num_workers),
            "BATCH_SIZE": str(config.docling.batch_size),
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
    config: LargeScaleDoclingAssetConfig,
):
    """Process a large collection of documents using multi-node Ray cluster.

    This asset demonstrates scaling up document processing to multiple nodes
    for handling large document collections that benefit from HPC resources.

    Args:
        context: Dagster execution context
        compute_ray: ComputeResource configured with RayLauncher
        config: Combined configuration (slurm + document processing)
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
        f"Starting large-scale document processing with multi-node Ray cluster...\n"
        f"  Input: {config.docling.input_glob}\n"
        f"  Output: {config.docling.output_dir}\n"
        f"  Workers: {config.docling.num_workers}\n"
        f"  Batch size: {config.docling.batch_size}"
    )

    completed_run = compute_ray.run(
        context=context,
        payload_path=script_path,
        config=config.slurm,
        launcher=ray_launcher,
        extra_env={
            "INPUT_GLOB": config.docling.input_glob,
            "OUTPUT_DIR": config.docling.output_dir,
            "NUM_WORKERS": str(config.docling.num_workers),
            "BATCH_SIZE": str(config.docling.batch_size),
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
