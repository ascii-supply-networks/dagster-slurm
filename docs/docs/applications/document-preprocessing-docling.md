---
sidebar_position: 1
title: Document Preprocessing with Docling
---

# Document Preprocessing with Docling

Process PDF documents at scale using [docling](https://github.com/DS4SD/docling) and Ray. This example demonstrates distributed document processing on HPC clusters with automatic Ray cluster management.

:::info Reference Implementation
Adapted from [duckpond's docling+Ray example](https://github.com/l-mds/duckpond/tree/main/projects/100_combined/ai_example/ai_example/defs/document_ai) for HPC/Slurm environments.
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

The payload script supports two execution modes:

### Test Data

Sample PDFs are included in `examples/data/` for testing:

- **WO2021041671-eval-small.pdf** - 4-page patent document (596KB)
- **test-document-{1-5}.pdf** - 5 copies for batch processing tests

For HPC testing, transfer data to your cluster:

```bash
# Transfer via SCP
scp -r examples/data/ user@hpc-cluster:/path/on/hpc/

# Or via rsync for larger datasets
rsync -avz --progress examples/data/ user@hpc-cluster:/path/on/hpc/

# For docker Slurm testing
docker cp examples/data/. slurmctld:/home/submitter/dagster-slurm-data/
```

### Mode 1: Via Dagster (Pipes Mode)

This is the primary integration mode for production workflows.

#### Local Development

```bash
# Start Dagster dev server
pixi run -e build --frozen start

# In Dagster UI, materialize the asset:
# - process_documents_with_docling
```

The asset will:

1. Start a local Ray instance automatically
2. Process documents in parallel using available CPU cores
3. Convert PDFs to markdown using real docling
4. Report progress and results back to Dagster

#### Production (Slurm)

Configure resources in your Dagster definitions:

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

The asset will:

1. Submit a Slurm job with requested resources
2. Start a Ray cluster across allocated nodes
3. Distribute document processing across the cluster
4. Stream logs back to Dagster in real-time
5. Report results when complete

### Mode 2: Standalone Python

For testing or one-off processing without Dagster:

```bash
# Navigate to the payload script directory
cd examples/projects/dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/ray/

# Run standalone with default settings
python process_documents_docling.py --mode standalone

# Or customize parameters
python process_documents_docling.py \
  --mode standalone \
  --input-glob "data/**/*.pdf" \
  --output-dir "out/my_docs" \
  --num-workers 4 \
  --batch-size 8

# Run in Pipes mode (if called from Dagster)
python process_documents_docling.py --mode pipes
```

**Standalone mode features:**

- Initializes Ray automatically
- No Dagster dependency required
- Returns results as dictionary
- Useful for debugging and testing

## Configuration

### Document Processing Settings

Configure via environment variables passed to `extra_env`:

```python
extra_env={
    "INPUT_GLOB": "data/**/*.pdf",      # Input file pattern
    "OUTPUT_DIR": "/tmp/docling_output", # Output directory
    "NUM_WORKERS": "2",                  # Parallel workers
    "BATCH_SIZE": "4",                   # Documents per batch
}
```

### Slurm Resource Allocation

Configure via `extra_slurm_opts`:

```python
extra_slurm_opts={
    "nodes": 1,            # Number of compute nodes
    "cpus_per_task": 4,    # CPUs per node
    "mem": "8G",           # Memory per node
    "time": "01:00:00",    # Time limit (1 hour)
    # For GPU-accelerated OCR:
    # "gres": "gpu:1",
}
```

### Scaling Configurations

#### Single Node (Local Ray)

```python
extra_slurm_opts={
    "nodes": 1,
    "cpus_per_task": 4,
    "mem": "8G",
}
```

**Best for:** Small to medium document collections (< 1000 documents)

#### Multi-Node (Distributed Ray)

```python
extra_slurm_opts={
    "nodes": 4,           # 4 nodes
    "cpus_per_task": 8,   # 8 CPUs per node
    "mem": "16G",         # 16GB per node
}
```

**Best for:** Large document collections (> 1000 documents)

## Example Assets

### 1. Basic Document Processing

Processes documents with single-node Ray:

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

### 2. Result Analysis

Analyzes results from document processing:

```python
@dg.asset(deps=[process_documents_with_docling])
def analyze_docling_results(context: dg.AssetExecutionContext):
    """Analyze processing results and generate summary statistics."""
    # Get metadata from upstream asset
    event_record = context.instance.get_latest_materialization_event(
        asset_key=process_documents_with_docling.key
    )
    metadata = event_record.asset_materialization.metadata

    # Extract and analyze results
    total_docs = get_int_value(metadata, "total_documents", 0)
    successful = get_int_value(metadata, "successful", 0)
    success_rate = (successful / total_docs * 100) if total_docs > 0 else 0

    yield dg.Output(
        value=None,
        metadata={"success_rate_percent": round(success_rate, 2)}
    )
```

### 3. Large-Scale Processing

Multi-node processing for large collections:

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

## Docling Converters

### BasicDocumentConverter (Default)

Simple converter that works out-of-the-box:

```python
class BasicDocumentConverter:
    """Basic docling converter for PDF documents."""
    def __init__(self, document_format: InputFormat = InputFormat.PDF):
        self.converter = DocumentConverter()
        self.converter.initialize_pipeline(document_format)

    def convert_one(self, uri: str) -> ConversionResult:
        return self.converter.convert(uri)
```

**Best for:** General-purpose document conversion with standard PDFs.

**Usage:** Enabled by default in `DoclingActor.__init__()`.

### RapidOCRDocumentConverter (Advanced)

High-quality OCR for scanned documents. To enable:

1. Uncomment the RapidOCR imports and class in `process_documents_docling.py`
2. Download RapidOCR model files from [PaddleOCR](https://github.com/PaddlePaddle/PaddleOCR/blob/release/2.6/doc/doc_en/models_list_en.md)
3. Set `RAPIDOCR_MODELS_ROOT` environment variable
4. Update `DoclingActor` to use `RapidOCRDocumentConverter()`

```python
# In DoclingActor.__init__()
self.converter = RapidOCRDocumentConverter(
    models_root="/path/to/rapidocr/models"
)
```

**Best for:** Scanned documents, images in PDFs, higher OCR accuracy.

**Requirements:**

- RapidOCR ONNX models (PP-OCRv5, PP-OCRv4)
- Additional 2-3GB for model files
- Recommended for GPU-enabled nodes

**Implementation reference:** [duckpond RapidOCR converter](https://github.com/l-mds/duckpond/blob/main/projects/100_combined/ai_example/ai_example/defs/document_ai/document_plain_rapidocr.py)

## Performance Tuning

### Batch Size

Adjust based on document size:

- **Small PDFs (< 5MB)**: `batch_size=8-16`
- **Large PDFs (> 20MB)**: `batch_size=2-4`

### Worker Count

Match to available resources:

- **Local**: `cpu_count() - 1`
- **Slurm**: `nodes × cpus_per_task`

### Memory

Allocate based on document size:

- **Small PDFs**: 4-8GB per node
- **Large PDFs**: 16-32GB per node

### Multi-Node Decision

- **Single node**: < 1000 documents
- **Multi-node**: > 1000 documents

## GPU-Accelerated OCR

For GPU-accelerated document processing:

```python
ray_launcher = RayLauncher(
    num_gpus_per_node=1,  # 1 GPU per node
)

extra_slurm_opts={
    "nodes": 2,
    "gres": "gpu:1",      # Request 1 GPU per node
    "cpus_per_task": 4,
    "mem": "16G",
}
```

## Monitoring

The application provides rich metadata for monitoring:

### Document Processing Metrics

- `total_documents` - Total documents processed
- `successful` - Successfully converted documents
- `failed` - Failed conversions
- `duration_seconds` - Total processing time
- `output_directory` - Location of processed files
- `num_workers` - Number of parallel workers used
- `batch_size` - Batch size configuration

### Analysis Metrics

- `success_rate_percent` - Conversion success rate
- `status` - Health status (healthy/needs_attention)
- `total_documents` - Total from upstream
- `successful` - Successful from upstream
- `failed` - Failed from upstream

View these metrics in the Dagster UI under the materialization event for each asset.

## Implementation Comparison

### Architecture Differences

| Aspect             | Duckpond Original                  | dagster-slurm Adaptation               |
| ------------------ | ---------------------------------- | -------------------------------------- |
| **Integration**    | Uses `dagster-ray` + `RayResource` | Uses `ComputeResource` + `RayLauncher` |
| **Execution**      | Direct Ray calls in asset          | Pipes-based payload script             |
| **Environment**    | Cloud/local Ray clusters           | HPC/Slurm clusters + local dev         |
| **Ray Management** | Manual Ray cluster connection      | Automatic cluster startup/shutdown     |
| **Code Location**  | Ray code embedded in asset         | Separate payload script                |
| **Packaging**      | Python environment assumed         | pixi-pack for environment portability  |
| **Modes**          | Single execution mode              | Dual-mode: Pipes + standalone          |

### Key Adaptations

1. **ComputeResource Pattern**: Using dagster-slurm's `ComputeResource` instead of `dagster-ray`'s `RayResource` for better HPC integration

2. **Pipes Architecture**: Payload script runs in isolation via Dagster Pipes for:
   - Better error handling and logging
   - Environment isolation
   - Reusable standalone execution

3. **RayLauncher**: Automatic multi-node Ray cluster management:
   - Spawns Ray head/worker nodes on Slurm
   - Handles cluster startup/shutdown
   - Manages SSH connections

4. **Dual-Mode Execution**:
   - **Pipes mode**: Full Dagster integration with metadata reporting
   - **Standalone mode**: Direct Python execution for testing/debugging

5. **Environment Packaging**: Uses `pixi-pack` to package entire environment for remote execution on HPC nodes

### Why These Changes?

The adaptations optimize for **HPC/Slurm environments** where:

- Compute nodes may not have internet access
- Python environments need to be self-contained
- Job submission is via sbatch (not direct Ray cluster connection)
- Multi-node coordination requires SSH and shared filesystems
- Resources are allocated via Slurm's queueing system

For **cloud or persistent Ray clusters**, the original duckpond approach with `dagster-ray` may be more appropriate.

## Troubleshooting

### No files found

**Issue**: "No files found for glob: data/**/*.pdf"

**Solution**:

- Verify the input glob pattern matches your file structure
- Use absolute paths if working directory is uncertain
- Check file permissions on the cluster

### Ray cluster fails to start

**Issue**: Ray workers cannot connect to head node

**Solution**:

- Ensure `ray` is installed in the pixi environment
- Check Slurm partition allows requested resources
- Verify intra-node networking is allowed
- Review job logs: `${SLURM_DEPLOYMENT_BASE_PATH}/.../run.log`

### Out of memory errors

**Issue**: Workers crash with OOM errors

**Solution**:

- Reduce `batch_size` for large documents
- Increase memory allocation per node (`mem` in `extra_slurm_opts`)
- Reduce `num_workers` to allow more memory per worker

### Slow processing

**Issue**: Document conversion is slower than expected

**Solution**:

- Increase `num_workers` to match available CPUs
- Adjust `batch_size` based on document size
- Consider multi-node processing for large collections
- Enable GPU acceleration for RapidOCR converter

## Next Steps

- **Ray Integration**: Combine with [other Ray workloads](../integration-ray/dagster-slurm-ray.md) in pipelines
- **Advanced OCR**: Configure [RapidOCRDocumentConverter](#rapidocrdocumentconverter-advanced) for scanned documents
- **API Reference**: Explore [RayLauncher parameters](../api/api_core.md#class-dagster_slurm-raylauncher)
- **Original Example**: Review [duckpond's docling implementation](https://github.com/l-mds/duckpond/tree/main/projects/100_combined/ai_example/ai_example/defs/document_ai)

## Source Code

Full implementation available in the dagster-slurm examples:

- **Assets**: `examples/projects/dagster-slurm-example/dagster_slurm_example/defs/ray/docling_assets.py`
- **Payload**: `examples/projects/dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/ray/process_documents_docling.py`
