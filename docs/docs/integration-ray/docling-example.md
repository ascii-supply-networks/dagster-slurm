---
sidebar_position: 3
title: Document processing with docling
---

Process PDF documents at scale using [docling](https://github.com/DS4SD/docling) and Ray. This example converts PDFs to markdown across HPC clusters with automatic Ray cluster management.

:::info
Adapted from [duckpond's docling example](https://github.com/l-mds/duckpond/tree/main/projects/100_combined/ai_example/ai_example/defs/document_ai) for HPC environments.

📚 **[Full documentation and examples →](../applications/document-preprocessing-docling.md)**
:::

## Quick start

### 1. Create the processing script

```python title="dagster_slurm_example_hpc_workload/ray/process_documents_docling.py"
import ray.data as rd
from dagster_pipes import PipesContext, open_dagster_pipes
from docling.document_converter import DocumentConverter

class BasicDocumentConverter:
    def __init__(self):
        self.converter = DocumentConverter()
        self.converter.initialize_pipeline()

    def convert_one(self, uri: str):
        return self.converter.convert(uri)

class DoclingActor:
    def __init__(self):
        self.converter = BasicDocumentConverter()

    def convert_one(self, src_path: str, base_out_dir: str):
        result = self.converter.convert_one(src_path)
        document = result.document

        # Export to markdown
        out_dir = Path(base_out_dir) / Path(src_path).stem
        out_dir.mkdir(parents=True, exist_ok=True)
        document.save_as_markdown(out_dir / f"{Path(src_path).stem}.md")

        return {"ok": result.status == "SUCCESS", "pages": document.num_pages()}

def main():
    context = PipesContext.get()

    # Configuration from environment
    input_glob = context.get_extra("INPUT_GLOB") or "data/**/*.pdf"
    output_dir = context.get_extra("OUTPUT_DIR") or "out/docling"
    num_workers = int(context.get_extra("NUM_WORKERS") or "2")

    # Find and process files
    files = [str(p) for p in Path().glob(input_glob)]
    ds = rd.from_items([{"path": p} for p in files])

    # Ray Data processing
    result = ds.map_batches(
        build_converter(output_dir),
        batch_size=4,
        concurrency=(num_workers, num_workers),
    ).materialize()

    # Report results
    total = result.count()
    ok = result.filter(lambda r: r["ok"]).count()

    context.report_asset_materialization(metadata={
        "total_documents": total,
        "successful": ok,
        "failed": total - ok,
    })

if __name__ == "__main__":
    with open_dagster_pipes():
        main()
```

### 2. Define the Dagster asset

```python title="dagster_slurm_example/defs/ray/docling_assets.py"
import dagster as dg
from dagster_slurm import ComputeResource, RayLauncher, SlurmRunConfig

@dg.asset
def process_documents_with_docling(
    context: dg.AssetExecutionContext,
    compute_ray: ComputeResource,
    config: SlurmRunConfig,
):
    """Convert PDFs to markdown using docling and Ray."""

    script_path = dg.file_relative_path(__file__, "../../path/to/process_documents_docling.py")

    completed_run = compute_ray.run(
        context=context,
        payload_path=script_path,
        config=config,
        launcher=RayLauncher(num_gpus_per_node=0),
        extra_env={
            "INPUT_GLOB": "data/**/*.pdf",
            "OUTPUT_DIR": "/tmp/docling_output",
            "NUM_WORKERS": "2",
        },
        extra_slurm_opts={
            "nodes": 1,        # Single node = local Ray
            "cpus_per_task": 4,
            "mem": "8G",
        },
    )

    yield from completed_run.get_results()
```

### 3. Run locally

```bash
pixi run -e build --frozen start
# Materialize: process_documents_with_docling in Dagster UI
```

Documents are converted in parallel using local Ray. Same code deploys to Slurm clusters.

## Execution modes

| Mode | Ray Cluster | Use Case |
|------|-------------|----------|
| Local dev | Single-node local | Fast iteration, testing |
| Slurm single-node | Local Ray in job | < 1000 documents |
| Slurm multi-node | Distributed Ray cluster | > 1000 documents |

### Scale to multi-node

```python
extra_slurm_opts={
    "nodes": 4,         # 4-node Ray cluster
    "cpus_per_task": 8,
    "mem": "16G",
}
```

RayLauncher automatically spawns Ray head/worker nodes across Slurm allocation.

## Standalone execution

Test without Dagster:

```bash
cd dagster_slurm_example_hpc_workload/ray/

# Direct Python execution
python process_documents_docling.py \
  --mode standalone \
  --input-glob "data/**/*.pdf" \
  --output-dir "out/my_docs" \
  --num-workers 4
```

The script initializes Ray and runs independently—useful for debugging.

## Converter options

### BasicDocumentConverter (default)

Out-of-the-box PDF conversion:

```python
class BasicDocumentConverter:
    def __init__(self):
        self.converter = DocumentConverter()
        self.converter.initialize_pipeline()
```

Works for most PDFs with standard text.

### RapidOCRDocumentConverter (advanced)

High-quality OCR for scanned documents:

```python
from docling.datamodel.pipeline_options import RapidOcrOptions

class RapidOCRDocumentConverter:
    def __init__(self, models_root: str):
        pipeline_options = ThreadedPdfPipelineOptions()
        pipeline_options.do_ocr = True
        pipeline_options.ocr_options = RapidOcrOptions(
            det_model_path=f"{models_root}/PP-OCRv5/det/...",
            rec_model_path=f"{models_root}/PP-OCRv5/rec/...",
        )
        self.converter = DocumentConverter(
            format_options={InputFormat.PDF: PdfFormatOption(pipeline_options)}
        )
```

Requires RapidOCR model files (~2-3GB). See [duckpond implementation](https://github.com/l-mds/duckpond/blob/main/projects/100_combined/ai_example/ai_example/defs/document_ai/document_plain_rapidocr.py).

## Performance tuning

**Batch size**: Documents per worker
- Small PDFs (< 5MB): `batch_size=8-16`
- Large PDFs (> 20MB): `batch_size=2-4`

**Worker count**: Match available resources
- Local: `cpu_count() - 1`
- Slurm: `nodes × cpus_per_task`

**Memory**: Based on document size
- Small PDFs: 4-8GB/node
- Large PDFs: 16-32GB/node

## Key differences from duckpond

| Aspect | Duckpond | dagster-slurm |
|--------|----------|---------------|
| Integration | `dagster-ray` | `ComputeResource` + `RayLauncher` |
| Execution | Direct Ray cluster | Pipes-based payload script |
| Environment | Cloud/local | HPC/Slurm + environment packaging |
| Ray management | Manual connection | Automatic cluster startup/shutdown |

dagster-slurm optimizes for HPC environments with:
- Slurm job submission (`sbatch`)
- Environment packaging (`pixi-pack`)
- SSH-based multi-node coordination
- No cluster pre-deployment required

## Next steps

- Combine with [other Ray workloads](./dagster-slurm-ray.md) in pipelines
- Configure [GPU-accelerated OCR](./docling-example.md#rapidocrconverter-advanced) for scanned documents
- Explore [RayLauncher API](../api/api_core.md#class-dagster_slurm-raylauncher) parameters
