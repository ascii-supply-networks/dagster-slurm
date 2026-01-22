---
sidebar_position: 1
title: Document Preprocessing with Docling
---

# Document Preprocessing with Docling

Process PDF documents at scale using [docling](https://github.com/DS4SD/docling) and [Ray](https://ray.io/). This example demonstrates distributed document processing on HPC clusters with automatic Ray cluster management.

:::info Ray Integration
For a complete guide on using Ray with dagster-slurm, including cluster management and distributed computing patterns, see [Ray on Slurm](../integration-ray/ray.md).
:::

## Overview

This application shows how to:

- Process large collections of PDF documents in parallel using docling
- Use Ray Data for distributed batch processing with real docling converters
- Run the same code locally (development) and on Slurm HPC clusters (production)
- Execute the payload script standalone or via Dagster Pipes
- Chain multiple assets in a document processing pipeline
- Scale from single-node to multi-node processing
- Choose between BasicDocumentConverter or RapidOCRDocumentConverter

## Architecture

The application follows a layered architecture:

```
┌─────────────────────────────────────────────────────────────┐
│ Dagster Assets (orchestration layer)                        │
│  - process_documents_with_docling                           │
│  - analyze_docling_results                                  │
│  - process_large_document_collection                        │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
        ┌────────────────────────┐
        │   ComputeResource      │
        │   + RayLauncher        │
        └────────┬───────────────┘
                 │
      ┌──────────┴──────────┐
      │                     │
      ▼                     ▼
┌──────────┐        ┌──────────────┐
│  Local   │        │    Slurm     │
│  Mode    │        │    Cluster   │
└──────────┘        └──────────────┘
      │                     │
      ▼                     ▼
┌──────────┐        ┌──────────────┐
│ Ray      │        │ Multi-node   │
│ Local    │        │ Ray Cluster  │
└──────────┘        └──────────────┘
      │                     │
      └─────────┬───────────┘
                ▼
    ┌──────────────────────┐
    │  Payload Script      │
    │  (process_documents_ │
    │   docling.py)        │
    └──────────┬───────────┘
               │
               ▼
    ┌──────────────────────┐
    │  Ray Data Pipeline   │
    │  - Docling actors    │
    │  - Batch processing  │
    │  - PDF → Markdown    │
    └──────────────────────┘
```

## Files

### Assets (Dagster definitions)

Located in: `examples/projects/dagster-slurm-example/dagster_slurm_example/defs/ray/docling_assets.py`

**`process_documents_with_docling`** - Main document processing asset

- Single-node document processing with configurable resources
- Uses `ComputeResource` with `RayLauncher`
- Converts PDFs to markdown in parallel

**`analyze_docling_results`** - Downstream analysis asset

- Reads metadata from upstream processing
- Calculates success rates and health metrics
- Demonstrates asset chaining and metadata propagation

**`process_large_document_collection`** - Multi-node scaling example

- Demonstrates scaling to 4+ nodes for large workloads
- Higher parallelism configuration for batch processing

### Payload Scripts (Execution code)

Located in: `examples/projects/dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/ray/process_documents_docling.py`

Key components:

- **`BasicDocumentConverter`** - Simple out-of-the-box PDF converter
- **`RapidOCRDocumentConverter`** - Advanced OCR converter (optional, commented)
- **`DoclingActor`** - Ray actor for parallel document processing
- **`run_processing()`** - Core processing function (works with/without Pipes context)
- **`main()`** - Entry point for Dagster Pipes mode

## Usage

The application supports three execution modes with increasing complexity. **Start simple without Dagster, then add orchestration, then scale to HPC.**

### Test Data

Sample PDFs are included in `examples/data/` for testing all three modes.

For HPC testing, transfer data to your cluster:

```bash
# Transfer via SCP
scp -r examples/data/ user@hpc-cluster:/path/on/hpc/

# Or via rsync for larger datasets
rsync -avz --progress examples/data/ user@hpc-cluster:/path/on/hpc/

# For docker Slurm testing
docker cp examples/data/. slurmctld:/home/submitter/dagster-slurm-data/
```

:::warning AI Model Downloads
Docling and other AI workloads require downloading models on first run. **Network connectivity requirements are cluster-dependent:**

- **Local development**: Models download automatically
- **HPC clusters with internet access**: Models download to compute nodes (may be slow)
- **Air-gapped/restricted clusters**: Pre-download required via edge node or proxy

See [Pre-Downloading Models](#pre-downloading-models-for-restricted-clusters) below for setup instructions.
:::

### 1. Local Python Script

**Simplest:** Run the processing script directly as a standalone Python script. No orchestration, no slurm —just Python, Ray, and docling.

**Requirements:** Python environment with Ray and docling.

```bash
# Navigate to the payload script directory
cd examples/
pixi shell -e dev
cd projects/dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/ray/

# Run with default settings
python process_documents_docling.py

# Or customize parameters
python process_documents_docling.py \
  --input-glob "data/**/*.pdf" \
  --output-dir "out/my_docs" \
  --num-workers 4 \
  --batch-size 8
```

**What happens:**

1. Script runs as a normal Python program on your local machine
2. Initializes a local Ray instance automatically
3. Processes documents in parallel using available CPU cores
4. Converts PDFs to markdown using docling
5. Prints results to console (no Dagster, no metadata tracking)

**Best for:**

- Quick testing and experimentation
- Debugging conversion issues
- Learning how the processing works
- One-off document conversions without orchestration

### 2. Local with Dagster Orchestration

**Medium:** Same local execution, but now integrated with Dagster for orchestration, metadata tracking, and pipeline capabilities.

**Requirements:** Dagster dev environment.

```bash
# Start Dagster dev server
pixi run -e build --frozen start

# In Dagster UI, materialize the asset:
# - process_documents_with_docling

# Or use the dg CLI:
dg launch --assets process_documents_with_docling
```

**What happens:**

1. Dagster launches the payload script via **Pipes** (inter-process communication)
2. Script runs on your local machine with local Ray
3. Processes documents in parallel using available CPU cores
4. Reports progress and results **back to Dagster via Pipes**
5. Metadata appears in Dagster UI (success rate, document count, processing time)

**Best for:**

- Development with full Dagster features (UI, metadata, lineage)
- Building multi-asset pipelines with dependencies
- Tracking processing history and observability
- Iterating on document processing workflows
- Combining processing before the HPC, with HPC workloads and postprocessing

### 3. HPC Slurm Deployment

Deploy to HPC clusters for large-scale processing. Choose staging for quick iteration or production for optimized, repeatable deployments.

#### 3a. Staging (Direct Submission)

**Medium-Complex:** Submit jobs directly to Slurm with on-demand environment deployment. Dependencies are resolved and deployed each time.

**Requirements:** Access to Slurm cluster with configured SSH authentication.

**Configure Slurm resources:**

```python
from dagster_slurm import SlurmResource, ComputeResource, RayLauncher

slurm = SlurmResource(
    host="your-hpc-cluster.com",
    username="your-username",
    # ... other Slurm config
)

compute_ray = ComputeResource.for_slurm(
    slurm_resource=slurm,
    launcher=RayLauncher(num_gpus_per_node=0),
)
```

**Run:**

```bash
# Set deployment mode to staging
export DAGSTER_DEPLOYMENT=staging_supercomputer

# Launch via Dagster UI or CLI
dg launch --assets process_documents_with_docling
```

**What happens:**

1. Dagster submits a Slurm job with requested resources
2. Environment is built/deployed on-demand (slower first run, cached thereafter)
3. RayLauncher spawns Ray head and worker nodes across allocated Slurm nodes
4. Payload script distributes document processing across the Ray cluster
5. Logs stream back to Dagster in real-time via SSH
6. Results and metadata reported when complete

**Best for:**

- Development and testing on real HPC infrastructure
- Iterating on processing logic before production deployment
- Quick experimentation with different configurations
- When environment changes frequently

#### 3b. Production (Pre-Packaged)

**Most Complex:** Deploy with pre-packaged, optimized environments for fast startup and repeatability.

**Requirements:** Same as staging + ability to pre-deploy packaged environments.

**Step 1: Package environment**

```bash
# Package environment for HPC
cd examples
pixi run -e opstooling --frozen python scripts/pack_environment.py \
  --env packaged-cluster \
  --platform linux-64 \
  --build-missing
```

**Step 2: Deploy to cluster**

```bash
# Deploy packaged environment to cluster
pixi run -e dev --frozen deploy-prod-docker
```

**Step 3: Run**

```bash
# Set deployment mode to production
export DAGSTER_DEPLOYMENT=production_supercomputer

# Launch via Dagster UI or CLI
dg launch --assets process_documents_with_docling
```

**What happens:**

1. Dagster submits a Slurm job using pre-deployed environment
2. Job starts immediately (no environment build/deployment overhead)
3. RayLauncher spawns Ray head and worker nodes across allocated Slurm nodes
4. Payload script distributes document processing across the Ray cluster
5. Logs stream back to Dagster in real-time via SSH
6. Results and metadata reported when complete

**Best for:**

- Production workloads with large document collections (> 1000 documents)
- Scheduled or triggered processing pipelines
- Multi-node parallel processing at scale
- GPU-accelerated OCR processing
- Environments that change infrequently
- Fast, repeatable job startup times

## Configuration

Assets are configured via `extra_env` (processing settings like input paths, worker count) and `extra_slurm_opts` (resource allocation like nodes, CPUs, memory). See the [Example Assets](#example-assets) below for concrete single-node and multi-node configurations.

For the complete implementation: [`docling_assets.py`](https://github.com/ascillato/dagster-slurm/blob/main/examples/projects/dagster-slurm-example/dagster_slurm_example/defs/ray/docling_assets.py)

## Example Assets

These examples show concrete configurations for different workload scales. All parameters (`extra_env`, `extra_slurm_opts`) can be adjusted based on your requirements.

### Basic Document Processing (Single Node)

Small to medium collections with single-node Ray:

```python
@dg.asset
def process_documents_with_docling(
    context: dg.AssetExecutionContext,
    compute_ray: ComputeResource,
    config: SlurmRunConfig,
):
    """Process PDF documents using docling and Ray."""
    script_path = dg.file_relative_path(__file__, "../../path/to/process_documents_docling.py")

    completed_run = compute_ray.run(
        context=context,
        payload_path=script_path,
        launcher=RayLauncher(num_gpus_per_node=0),
        extra_env={
            "INPUT_GLOB": "data/**/*.pdf",
            "NUM_WORKERS": "2",
        },
        extra_slurm_opts={
            "nodes": 1,
            "cpus_per_task": 4,
            "mem": "8G",
        },
    )
    yield from completed_run.get_results()
```

### Large-Scale Processing (Multi-Node)

Large collections with multi-node Ray cluster:

```python
@dg.asset
def process_large_document_collection(
    context: dg.AssetExecutionContext,
    compute_ray: ComputeResource,
    config: SlurmRunConfig,
):
    """Process large document collection with multi-node Ray cluster."""
    completed_run = compute_ray.run(
        context=context,
        payload_path=script_path,
        launcher=RayLauncher(num_gpus_per_node=0),
        extra_env={
            "INPUT_GLOB": "data/large_collection/**/*.pdf",
            "NUM_WORKERS": "8",  # More workers
            "BATCH_SIZE": "8",
        },
        extra_slurm_opts={
            "nodes": 4,          # Multi-node
            "cpus_per_task": 4,
            "mem": "16G",
            "time": "02:00:00",
        },
    )
    yield from completed_run.get_results()
```

## Pre-Downloading Models for Restricted Clusters

For air-gapped or restricted HPC clusters, pre-download docling models on an edge node or via proxy before running jobs.

### Download Script

A ready-to-use script is available: [`download_docling_models.py`](../../../examples/scripts/download_docling_models.py)

The script:

- Downloads all docling models using the official `docling.utils.model_downloader`
- Supports custom cache directories via `MODEL_CACHE_DIR` and `HF_HOME` environment variables
- Excludes unnecessary components (EasyOCR by default)
- Provides clear instructions for configuring worker nodes

### Setup Instructions

**1. On edge node or login node with internet access:**

```bash
# Create download directory on shared filesystem
export MODEL_CACHE_DIR=/shared/models
export HF_HOME=/shared/models/huggingface

# Run download script
cd examples/scripts
python download_docling_models.py
```

**2. Configure worker nodes to use pre-downloaded models:**

Update your asset to set `HF_HOME` environment variable:

```python
completed_run = compute_ray.run(
    context=context,
    payload_path=script_path,
    launcher=RayLauncher(num_gpus_per_node=0),
    extra_env={
        "INPUT_GLOB": "data/**/*.pdf",
        "NUM_WORKERS": "2",
        "HF_HOME": "/shared/models/huggingface",  # Point to pre-downloaded models
    },
    extra_slurm_opts={
        "nodes": 1,
        "cpus_per_task": 4,
        "mem": "8G",
    },
)
```

**Important:** Adapt paths (`/shared/models/huggingface`) to match your cluster's shared filesystem structure. Consult your HPC administrator for recommended locations.

## Monitoring

The application provides rich metadata for monitoring:

- `success_rate_percent` - Conversion success rate
- `status` - Health status (healthy/needs_attention)
- `total_documents` - Total from upstream
- `successful` - Successful from upstream
- `failed` - Failed from upstream

View these metrics in the Dagster UI under the materialization event for each asset.

## Next Steps

- **Ray Integration**: Combine with [other Ray workloads](../integration-ray/dagster-slurm-ray.md) in pipelines
- **API Reference**: Explore [RayLauncher parameters](../api/api_core.md#class-dagster_slurm-raylauncher)
- **Original Example**: Review [duckpond's docling implementation](https://github.com/l-mds/duckpond/tree/main/projects/100_combined/ai_example/ai_example/defs/document_ai)

## Source Code

Full implementation available in the dagster-slurm examples:

- **Assets**: `examples/projects/dagster-slurm-example/dagster_slurm_example/defs/ray/docling_assets.py`
- **Payload**: `examples/projects/dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/ray/process_documents_docling.py`
