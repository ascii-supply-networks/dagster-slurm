"""HPC workload: Document processing with docling + metaxy incremental tracking.

This script runs on the HPC cluster and:
1. Discovers input files via glob and registers them in metaxy
2. Uses store.resolve_update() to find new/changed documents
3. Processes only those documents with Ray Data + docling
4. Writes results via MetaxyDatasink

Reuses BasicDocumentConverter and warmup_model_cache from the existing
docling workload (process_documents_docling.py).
"""

import glob
import hashlib
import os
import time
from pathlib import Path
from typing import Any

import metaxy as mx

# metaxy.toml is uploaded as sibling of this script via extra_files
os.environ.setdefault("METAXY_CONFIG", str(Path(__file__).parent / "metaxy.toml"))
# Import shared feature definitions to register them with metaxy
import dagster_slurm_example_shared.metaxy_features  # noqa: F401
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import ray
import ray.data as rd
from dagster_pipes import DagsterPipesError, PipesContext, open_dagster_pipes
from dagster_slurm_example_shared import get_base_output_path
from metaxy.ext.ray import MetaxyDatasink

from dagster_slurm_example_hpc_workload.ray.process_documents_docling import (
    BasicDocumentConverter,
    warmup_model_cache,
)

METAXY_TIMESTAMP_COLUMNS = (
    "metaxy_created_at",
    "metaxy_updated_at",
    "metaxy_deleted_at",
)


def _coerce_metaxy_timestamps_to_utc(table: pa.Table) -> pa.Table:
    for name in METAXY_TIMESTAMP_COLUMNS:
        idx = table.schema.get_field_index(name)
        if idx < 0:
            continue
        field = table.schema.field(idx)
        if not pa.types.is_timestamp(field.type):
            continue
        if field.type.tz == "UTC":
            continue
        table = table.set_column(
            idx,
            name,
            pc.cast(table.column(idx), pa.timestamp(field.type.unit, tz="UTC")),
        )
    return table


def _upsert_column(table: pa.Table, name: str, array: pa.Array) -> pa.Table:
    idx = table.schema.get_field_index(name)
    if idx >= 0:
        return table.set_column(idx, name, array)
    return table.append_column(name, array)


class MetaxyDoclingMapper:
    """Ray map_batches callable that converts documents and produces metaxy-compatible output."""

    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self.converter = BasicDocumentConverter()

    def __call__(self, batch: pa.Table) -> pa.Table:  # noqa: C901
        from docling.datamodel.document import ConversionStatus
        from docling_core.types.doc import ImageRefMode

        source_paths = batch.column(
            batch.schema.get_field_index("source_path")
        ).to_pylist()
        markdown_paths: list[str] = []
        num_pages_list: list[int] = []
        status_list: list[str] = []
        elapsed_list: list[float] = []

        for source_path in source_paths:
            t0 = time.time()
            try:
                result = self.converter.convert_one(source_path)

                if result.status != ConversionStatus.SUCCESS:
                    elapsed = time.time() - t0
                    markdown_paths.append("")
                    num_pages_list.append(0)
                    status_list.append(f"failed: {result.status}")
                    elapsed_list.append(round(elapsed, 2))
                    continue

                document = result.document
                pages = document.num_pages()
                doc_name = Path(source_path).stem
                out_dir = Path(self.output_dir) / doc_name
                out_dir.mkdir(parents=True, exist_ok=True)

                md_path = out_dir / f"{doc_name}.md"
                document.save_as_markdown(md_path, image_mode=ImageRefMode.REFERENCED)

                elapsed = time.time() - t0
                markdown_paths.append(str(md_path))
                num_pages_list.append(pages)
                status_list.append("success")
                elapsed_list.append(round(elapsed, 2))

            except Exception as e:
                elapsed = time.time() - t0
                markdown_paths.append("")
                num_pages_list.append(0)
                status_list.append(f"error: {e}")
                elapsed_list.append(round(elapsed, 2))

        batch = _upsert_column(
            batch, "markdown_path", pa.array(markdown_paths, type=pa.string())
        )
        batch = _upsert_column(
            batch, "num_pages", pa.array(num_pages_list, type=pa.int64())
        )
        batch = _upsert_column(
            batch, "conversion_status", pa.array(status_list, type=pa.string())
        )
        return _upsert_column(
            batch, "elapsed_s", pa.array(elapsed_list, type=pa.float64())
        )


def run_processing(  # noqa: C901
    input_glob: str,
    output_dir: str,
    num_workers: int,
    batch_size: int,
    context: PipesContext | None = None,
) -> dict[str, Any]:
    """Run metaxy-tracked document processing pipeline."""
    log_func = context.log.info if context else print

    log_func("Starting metaxy-tracked docling processing with Ray...")
    log_func(
        f"Configuration: input_glob={input_glob}, output_dir={output_dir}, "
        f"num_workers={num_workers}, batch_size={batch_size}"
    )

    cfg = mx.init()
    store = cfg.get_store()

    # Step 1: Discover input files and register as source documents
    files = [str(p) for p in glob.glob(input_glob, recursive=True)]
    if not files and not os.path.isabs(input_glob):
        project_root = os.getenv("PIXI_PROJECT_ROOT")
        if project_root:
            fallback_glob = os.path.join(project_root, input_glob)
            files = [str(p) for p in glob.glob(fallback_glob, recursive=True)]

    if not files:
        log_func(f"No files found for glob: {input_glob}")
        result = {
            "total_documents": 0,
            "successful": 0,
            "failed": 0,
            "skipped_unchanged": 0,
            "duration_seconds": 0.0,
            "output_directory": output_dir,
            "warning": "No input files found",
        }
        if context:
            context.report_asset_materialization(metadata=result)
        return result

    log_func(f"Found {len(files)} documents on disk")

    # Register source documents in metaxy with per-field provenance tokens.
    sources_rows = []
    for f in files:
        p = Path(f)
        doc_uid = hashlib.sha256(f.encode()).hexdigest()[:16]
        sources_rows.append(
            {
                "doc_uid": doc_uid,
                "source_path": f,
                "file_size_bytes": p.stat().st_size,
            }
        )

    sources_samples = pl.DataFrame(sources_rows).with_columns(
        pl.struct(
            pl.col("source_path").cast(pl.String).alias("source_path"),
            pl.col("file_size_bytes").cast(pl.String).alias("file_size_bytes"),
        ).alias("metaxy_provenance_by_field")
    )

    with store:
        source_increment = store.resolve_update(
            "docling/source_documents",
            samples=sources_samples,
        )
        if len(source_increment.new) > 0:
            store.write("docling/source_documents", source_increment.new)
        if len(source_increment.stale) > 0:
            store.write("docling/source_documents", source_increment.stale)

    # Step 2: Resolve which documents need (re)processing
    with store:
        try:
            increment = store.resolve_update(
                "docling/converted_documents",
                versioning_engine="polars",
            )
        except Exception as e:
            error_text = str(e)
            if (
                "Cannot compute precedence for `float32` and `timestamp('UTC', 6)` types"
                not in error_text
            ):
                raise

            log_func(
                "Detected legacy corrupted timestamp schema in docling/converted_documents. "
                "Dropping feature metadata and retrying incremental resolution."
            )
            store.drop_feature_metadata("docling/converted_documents")
            increment = store.resolve_update(
                "docling/converted_documents",
                versioning_engine="polars",
            )

    current_doc_uids = set(sources_samples["doc_uid"].to_list())
    doc_uid_filter = pl.col("doc_uid").is_in(list(current_doc_uids))

    new_df = increment.new.to_polars()
    stale_df = increment.stale.to_polars()
    raw_to_process = pl.concat([new_df, stale_df]) if len(stale_df) > 0 else new_df
    initial_increment_count = len(raw_to_process)

    # Guard against processing stale historical rows that are no longer present
    # in the current file discovery set.
    new_df = new_df.filter(doc_uid_filter)
    stale_df = stale_df.filter(doc_uid_filter)
    to_process = pl.concat([new_df, stale_df]) if len(stale_df) > 0 else new_df
    dropped_orphans = initial_increment_count - len(to_process)
    if dropped_orphans > 0:
        log_func(
            f"Dropped {dropped_orphans} stale rows not present in current input discovery."
        )

    skipped = max(len(files) - len(to_process), 0)

    if len(to_process) == 0:
        log_func("All documents are up to date - nothing to process.")
        result = {
            "total_documents": len(files),
            "successful": 0,
            "failed": 0,
            "skipped_unchanged": skipped,
            "duration_seconds": 0.0,
            "output_directory": output_dir,
            "status": "up_to_date",
        }
        if context:
            context.report_asset_materialization(metadata=result)
        return result

    log_func(
        f"Processing {len(new_df)} new + {len(stale_df)} stale = "
        f"{len(to_process)} documents ({skipped} skipped as unchanged)"
    )

    # Step 3: Pre-download docling models
    warmup_model_cache(context=context)

    total_start_time = time.time()

    # Step 4: Create Ray dataset from samples to process
    arrow_input = _coerce_metaxy_timestamps_to_utc(to_process.to_arrow())
    ds = rd.from_arrow(arrow_input)

    target_blocks = max(min(len(to_process), num_workers * 8), num_workers)
    if ds.num_blocks() < target_blocks:
        ds = ds.repartition(target_blocks)

    # Step 5: Process with docling via map_batches
    log_func("Starting parallel document processing...")
    result_ds = ds.map_batches(
        MetaxyDoclingMapper,
        fn_constructor_kwargs={"output_dir": output_dir},
        batch_size=batch_size,
        concurrency=(num_workers, num_workers),
        num_cpus=0,
        num_gpus=0,
        batch_format="pyarrow",
    )

    # Step 6: Write results via MetaxyDatasink
    log_func("Writing results via MetaxyDatasink...")
    datasink = MetaxyDatasink(
        feature="docling/converted_documents",
        store=store,
        config=cfg,
    )
    result_ds.write_datasink(datasink)
    write_stats = datasink.result

    total_duration = time.time() - total_start_time

    # Materialize to compute stats
    materialized = result_ds.materialize()
    total = materialized.count()
    ok = materialized.filter(lambda r: r["conversion_status"] == "success").count()
    failed = total - ok

    results = {
        "total_documents": len(files),
        "processed": total,
        "successful": ok,
        "failed": failed,
        "skipped_unchanged": skipped,
        "duration_seconds": round(total_duration, 2),
        "output_directory": output_dir,
        "num_workers": num_workers,
        "batch_size": batch_size,
        "rows_written": write_stats.rows_written,
        "rows_failed": write_stats.rows_failed,
    }

    log_func(
        f"Processing complete: processed={total}, ok={ok}, failed={failed}, "
        f"skipped={skipped}, duration={total_duration:.2f}s"
    )

    if context:
        context.report_asset_materialization(metadata=results)

    return results


def main():
    """Entry point for Dagster Pipes mode."""
    context = PipesContext.get()

    def _get_extra(key: str) -> str | None:
        try:
            return context.get_extra(key)
        except DagsterPipesError:
            return None

    # Configure metaxy store
    store_name = _get_extra("METAXY_STORE") or os.getenv("METAXY_STORE", "docling_dev")
    os.environ["METAXY_STORE"] = store_name

    input_glob = _get_extra("INPUT_GLOB") or os.getenv("INPUT_GLOB") or "data/**/*.pdf"
    output_dir_raw = _get_extra("OUTPUT_DIR") or os.getenv("OUTPUT_DIR")

    if output_dir_raw == "$HOME/output/docling_metaxy":
        base_output = get_base_output_path()
        output_dir = f"{base_output}/docling_metaxy"
    elif output_dir_raw:
        output_dir = os.path.expandvars(output_dir_raw)
    else:
        output_dir = "out/docling_metaxy"

    num_workers = int(_get_extra("NUM_WORKERS") or os.getenv("NUM_WORKERS") or "2")
    batch_size = int(_get_extra("BATCH_SIZE") or os.getenv("BATCH_SIZE") or "4")

    run_processing(
        input_glob=input_glob,
        output_dir=output_dir,
        num_workers=num_workers,
        batch_size=batch_size,
        context=context,
    )


if __name__ == "__main__":
    if os.getenv("DAGSTER_PIPES_CONTEXT") or os.getenv("DAGSTER_PIPES_MESSAGES"):
        with open_dagster_pipes() as context:
            main()
    else:
        import argparse

        parser = argparse.ArgumentParser(
            description="Process PDF documents using docling + metaxy (incremental)"
        )
        parser.add_argument(
            "--input-glob",
            default="data/**/*.pdf",
            help="Glob pattern for input files",
        )
        parser.add_argument(
            "--output-dir",
            default="out/docling_metaxy",
            help="Output directory for processed files",
        )
        parser.add_argument(
            "--num-workers",
            type=int,
            default=2,
            help="Number of parallel workers",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=4,
            help="Documents per batch",
        )
        args = parser.parse_args()

        if not ray.is_initialized():
            print("Initializing local Ray cluster...")
            ray.init()

        try:
            result = run_processing(
                input_glob=args.input_glob,
                output_dir=args.output_dir,
                num_workers=args.num_workers,
                batch_size=args.batch_size,
                context=None,
            )
            print(f"\nResults: {result}")
        finally:
            if ray.is_initialized():
                ray.shutdown()
