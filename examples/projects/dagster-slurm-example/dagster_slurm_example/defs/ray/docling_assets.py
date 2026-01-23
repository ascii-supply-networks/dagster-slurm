"""Assets for distributed document processing using docling and Ray.

This example demonstrates how to use dagster-slurm with Ray for large-scale
document processing workflows. In development, it runs locally; in production
on Slurm clusters, it spawns a multi-node Ray cluster for parallel processing.
"""

import os
from pathlib import Path

import dagster as dg
from dagster_slurm import ComputeResource, RayLauncher, SlurmRunConfig
from dagster_slurm_example_shared import get_base_output_path
from pydantic import Field


def _default_docling_glob() -> str | None:
    """Return default input glob based on deployment environment.

    - Local/development: Use local example data directory
    - Docker SLURM: Use Docker container data path
    - Supercomputer: None (must be configured via launchpad)
    """
    deployment = os.getenv("DAGSTER_DEPLOYMENT", "development")

    # Docker SLURM deployments use container path
    # Use *.pdf to match files in the directory root (not just subdirectories)
    if "docker" in deployment:
        return "/home/submitter/dagster-slurm-data/*.pdf"

    # Supercomputer: no default path (user must configure via launchpad)
    if "supercomputer" in deployment:
        return None

    # For local development, use local example data
    examples_dir = Path(__file__).resolve().parents[5]
    data_dir = examples_dir / "data"
    if data_dir.exists() and any(data_dir.rglob("*.pdf")):
        return str(data_dir / "**" / "*.pdf")

    # Unknown deployment: no default
    return None


def _default_docling_output_dir() -> str:
    """Return default output directory based on deployment environment.

    Uses shared path configuration from dagster_slurm_example_shared.
    - Local/development: /tmp/dagster_output/docling
    - Docker: $DATA/output/docling
    - musica/vsc5: $DATA/output/docling
    - datalab: $HOME/output/docling
    """
    base_output = get_base_output_path()
    return f"{base_output}/docling"


class DoclingRunConfig(SlurmRunConfig):
    """Configuration for docling document processing."""

    input_glob: str | None = Field(
        default_factory=_default_docling_glob,
        description="Glob pattern for input PDF files to process (relative to project root or absolute path). "
        "Required for supercomputer deployments - configure via launchpad.",
    )
    output_dir: str = Field(
        default_factory=_default_docling_output_dir,
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

    input_glob: str | None = Field(
        default_factory=_default_docling_glob,
        description="Glob pattern for input PDF files to process (relative to project root or absolute path). "
        "Required for supercomputer deployments - configure via launchpad.",
    )
    output_dir: str = Field(
        default_factory=_default_docling_output_dir,
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
    # Validate required configuration
    if config.input_glob is None:
        raise ValueError(
            "input_glob is required but not configured. "
            "Please provide input_glob in the Dagster launchpad or run configuration."
        )

    script_path = dg.file_relative_path(
        __file__,
        "../../../../dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/ray/process_documents_docling.py",
    )

    # Configure Ray launcher for document processing
    # Adjust num_nodes and resources based on your workload
    ray_launcher = RayLauncher(
        num_gpus_per_node=0,  # Set to >0 if using GPU-accelerated OCR
    )

    # Adjust resources based on deployment target
    # Docker/local: 2 CPUs, 4GB (safe for Docker environments)
    # Datalab/Musica: 8 CPUs, 24GB (docling + Ray + models need more)
    deployment = os.getenv("DAGSTER_DEPLOYMENT", "development")
    is_supercomputer = "supercomputer" in deployment
    cpus_per_task = 8 if is_supercomputer else 2
    memory_per_node = "24G" if is_supercomputer else "4G"

    context.log.info(
        f"Starting distributed document processing with docling...\n"
        f"  Deployment: {deployment}\n"
        f"  CPUs per task: {cpus_per_task}\n"
        f"  Memory per node: {memory_per_node}\n"
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
            "cpus_per_task": cpus_per_task,  # 2 CPUs for Docker, 8 for supercomputers
            "mem": memory_per_node,  # 4GB for Docker, 24GB for supercomputers
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
    # Validate required configuration
    if config.input_glob is None:
        raise ValueError(
            "input_glob is required but not configured. "
            "Please provide input_glob in the Dagster launchpad or run configuration."
        )

    script_path = dg.file_relative_path(
        __file__,
        "../../../../dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/ray/process_documents_docling.py",
    )

    # Configure multi-node Ray cluster
    ray_launcher = RayLauncher(
        num_gpus_per_node=0,
    )

    # Adjust resources based on deployment target
    deployment = os.getenv("DAGSTER_DEPLOYMENT", "development")
    is_supercomputer = "supercomputer" in deployment
    cpus_per_task = 8 if is_supercomputer else 2
    memory_per_node = "24G" if is_supercomputer else "4G"

    context.log.info(
        f"Starting large-scale document processing with multi-node Ray cluster...\n"
        f"  Deployment: {deployment}\n"
        f"  CPUs per task: {cpus_per_task}\n"
        f"  Memory per node: {memory_per_node}\n"
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
            "cpus_per_task": cpus_per_task,  # 2 CPUs for Docker, 8 for supercomputers
            "mem": memory_per_node,  # 4GB for Docker, 24GB for supercomputers
            "time": "01:00:00",  # 1-hour time limit
        },
    )

    yield from completed_run.get_results()
