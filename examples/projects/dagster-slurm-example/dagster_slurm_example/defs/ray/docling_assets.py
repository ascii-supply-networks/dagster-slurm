"""Assets for distributed document processing using docling and Ray.

This example demonstrates how to use dagster-slurm with Ray for large-scale
document processing workflows. In development, it runs locally; in production
on Slurm clusters, it spawns a multi-node Ray cluster for parallel processing.
"""

from pathlib import Path

import dagster as dg
from dagster_slurm import ComputeResource, RayLauncher, SlurmRunConfig
from pydantic import Field


def _default_docling_glob() -> str:
    """Prefer local example data when present, otherwise use HPC default."""
    examples_dir = Path(__file__).resolve().parents[5]
    data_dir = examples_dir / "data"
    if data_dir.exists() and any(data_dir.rglob("*.pdf")):
        return str(data_dir / "**" / "*.pdf")
    return "/home/submitter/dagster-slurm-data/**/*.pdf"


def _default_large_docling_glob() -> str:
    """Prefer local example data when present, otherwise use HPC large-collection path."""
    examples_dir = Path(__file__).resolve().parents[5]
    data_dir = examples_dir / "data"
    if data_dir.exists() and any(data_dir.rglob("*.pdf")):
        return str(data_dir / "**" / "*.pdf")
    return "/home/submitter/dagster-slurm-data/large_collection/**/*.pdf"


class DoclingRunConfig(SlurmRunConfig):
    """Configuration for docling document processing."""

    input_glob: str = Field(
        default_factory=_default_docling_glob,
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


class LargeScaleDoclingRunConfig(SlurmRunConfig):
    """Configuration for large-scale multi-node docling processing."""

    input_glob: str = Field(
        default_factory=_default_large_docling_glob,
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


@dg.asset(
    description="Process PDF documents using docling with distributed Ray workers",
    group_name="document_processing",
    metadata={
        "slurm_pack_cmd": [
            "pixi",
            "run",
            "-e",
            "opstooling",
            "--frozen",
            "python",
            "scripts/pack_environment.py",
            "--env",
            "workload-document-processing",
        ]
    },
)
def process_documents_with_docling(
    context: dg.AssetExecutionContext,
    compute_ray: ComputeResource,
    config: DoclingRunConfig,
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
        config: Slurm + docling configuration
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
        f"  Input: {config.input_glob}\n"
        f"  Output: {config.output_dir}\n"
        f"  Workers: {config.num_workers}\n"
        f"  Batch size: {config.batch_size}"
    )

    completed_run = compute_ray.run(
        context=context,
        payload_path=script_path,
        config=config,
        launcher=ray_launcher,
        extra_env={
            # Document processing configuration from Dagster config
            "INPUT_GLOB": config.input_glob,
            "OUTPUT_DIR": config.output_dir,
            "NUM_WORKERS": str(config.num_workers),
            "BATCH_SIZE": str(config.batch_size),
        },
        extra_slurm_opts={
            # Resource allocation for document processing
            # Single node = local Ray mode
            # Multi-node = distributed Ray cluster
            "nodes": 1,  # Increase for larger workloads
            "cpus_per_task": 2,  # CPUs per Ray worker
            "mem": "4G",  # Memory per node
            # Uncomment for GPU-accelerated OCR:
            # "gres": "gpu:1",
        },
    )

    yield from completed_run.get_results()


@dg.asset(
    description="Multi-node docling processing for large document collections",
    group_name="document_processing",
    metadata={
        "slurm_pack_cmd": [
            "pixi",
            "run",
            "-e",
            "opstooling",
            "--frozen",
            "python",
            "scripts/pack_environment.py",
            "--env",
            "workload-document-processing",
        ]
    },
)
def process_large_document_collection(
    context: dg.AssetExecutionContext,
    compute_ray: ComputeResource,
    config: LargeScaleDoclingRunConfig,
):
    """Process a large collection of documents using multi-node Ray cluster.

    This asset demonstrates scaling up document processing to multiple nodes
    for handling large document collections that benefit from HPC resources.

    Args:
        context: Dagster execution context
        compute_ray: ComputeResource configured with RayLauncher
        config: Slurm + docling configuration
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
        f"  Input: {config.input_glob}\n"
        f"  Output: {config.output_dir}\n"
        f"  Workers: {config.num_workers}\n"
        f"  Batch size: {config.batch_size}"
    )

    completed_run = compute_ray.run(
        context=context,
        payload_path=script_path,
        config=config,
        launcher=ray_launcher,
        extra_env={
            "INPUT_GLOB": config.input_glob,
            "OUTPUT_DIR": config.output_dir,
            "NUM_WORKERS": str(config.num_workers),
            "BATCH_SIZE": str(config.batch_size),
        },
        extra_slurm_opts={
            # Multi-node configuration
            "nodes": 2,  # Multiple nodes for distributed processing
            "cpus_per_task": 2,
            "mem": "4G",
            "time": "01:00:00",  # 1-hour time limit
        },
    )

    yield from completed_run.get_results()
