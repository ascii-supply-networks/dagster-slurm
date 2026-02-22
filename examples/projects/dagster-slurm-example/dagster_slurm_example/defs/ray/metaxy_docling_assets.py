"""Example 3: metaxy-enabled docling pipeline with environment switching.

Demonstrates metaxy incremental tracking for document processing with
per-environment store selection:
- Development: uses `docling_dev` store (DuckDB, lightweight)
- Production/Supercomputer: uses `docling_prod` store (DeltaLake, distributed)

Documents are registered as metaxy samples with provenance hashes.
Only new/changed documents are reprocessed on subsequent runs.
"""

import os

import dagster as dg
import metaxy.ext.dagster as mxd
from dagster_slurm import ComputeResource, RayLauncher, SlurmRunConfig
from pydantic import Field, model_validator


class MetaxyDoclingRunConfig(SlurmRunConfig):
    """Configuration for metaxy-tracked docling document processing."""

    input_glob: str = Field(
        description="Glob pattern for input PDF files to process (absolute path). "
        "Examples:\n"
        "  - Docker: /home/submitter/dagster-slurm-data/*.pdf\n"
        "  - Supercomputer: /scratch/username/data/*.pdf\n"
        "  - Development: /path/to/your/data/**/*.pdf",
    )
    output_dir: str = Field(
        default="$HOME/output/docling_metaxy",
        description="Directory where processed documents will be saved. "
        "Default: $HOME/output/docling_metaxy (expanded on compute node)",
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

    @model_validator(mode="after")
    def validate_input_glob_not_empty(self):
        """Validate that input_glob is not empty."""
        if not self.input_glob or not self.input_glob.strip():
            raise ValueError(
                "input_glob is required and cannot be empty. "
                "Please provide a glob pattern in the Dagster launchpad, "
                "e.g., '/scratch/username/data/*.pdf' or '/home/user/documents/**/*.pdf'"
            )
        return self


@mxd.metaxify
@dg.asset(
    metadata={"metaxy/feature": "docling/converted_documents"},
    description="Process PDF documents with docling + metaxy incremental tracking",
    group_name="metaxy_document_processing",
)
def process_documents_metaxy(
    context: dg.AssetExecutionContext,
    compute_ray: ComputeResource,
    config: MetaxyDoclingRunConfig,
):
    """Process PDF documents using docling with metaxy for incremental tracking.

    On each run, metaxy determines which documents are new or changed
    (based on file size provenance) and only those are reprocessed.
    Results are tracked in the metadata store selected by the deployment
    environment: DuckDB (docling_dev) locally, DeltaLake (docling_prod)
    on Slurm.

    In local mode: runs Ray locally with docling_dev store.
    In Slurm mode: spawns Ray cluster with docling_prod store.
    """
    script_path = dg.file_relative_path(
        __file__,
        "../../../../dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/ray/process_documents_docling_metaxy.py",
    )
    metaxy_config = dg.file_relative_path(__file__, "../../../../../metaxy.toml")

    ray_launcher = RayLauncher(
        num_gpus_per_node=0,
    )

    deployment = os.getenv("DAGSTER_DEPLOYMENT", "development")
    is_supercomputer = "supercomputer" in deployment
    is_production = "production" in deployment
    cpus_per_task = 8 if is_supercomputer else 2
    memory_per_node = "24G" if is_supercomputer else "4G"

    # Environment switching: DeltaLake for prod/supercomputer, DuckDB for dev
    metaxy_store = (
        "docling_prod" if (is_supercomputer or is_production) else "docling_dev"
    )

    context.log.info(
        f"Starting metaxy-tracked document processing with docling...\n"
        f"  Deployment: {deployment}\n"
        f"  Metaxy store: {metaxy_store}\n"
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
        extra_files=[metaxy_config],
        extra_env={
            "METAXY_STORE": metaxy_store,
            "INPUT_GLOB": config.input_glob,
            "OUTPUT_DIR": config.output_dir,
            "NUM_WORKERS": str(config.num_workers),
            "BATCH_SIZE": str(config.batch_size),
            "RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION": "0.5",
            "DAGSTER_DEPLOYMENT": deployment,
            "SLURM_SUPERCOMPUTER_SITE": os.getenv("SLURM_SUPERCOMPUTER_SITE", ""),
        },
        extra_slurm_opts={
            "nodes": 1,
            "cpus_per_task": cpus_per_task,
            "mem": memory_per_node,
        },
    )

    yield from completed_run.get_results()
