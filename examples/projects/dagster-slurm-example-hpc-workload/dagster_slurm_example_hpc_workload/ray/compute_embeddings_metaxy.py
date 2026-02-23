"""HPC workload: Compute text embeddings with Ray Data + metaxy incremental processing.

This script runs on the HPC cluster (or locally in dev mode) and:
1. Reads only new/stale samples via MetaxyDatasource(incremental=True)
2. Computes mock embeddings via Ray map_batches()
3. Writes results via MetaxyDatasink

Supports both Dagster Pipes mode (launched by dagster asset) and standalone mode.
"""

import os
import time
from pathlib import Path
from typing import Any

import dagster_slurm_example_shared.metaxy_features  # noqa: F401
import metaxy as mx
import polars as pl
import pyarrow as pa
import ray
import ray.data as rd
from dagster_pipes import DagsterPipesError, PipesContext, open_dagster_pipes
from metaxy.ext.ray import MetaxyDatasink
from ray.data import ActorPoolStrategy

# metaxy.toml is uploaded via extra_files. Prefer sibling path, fallback to CWD.
_script_cfg = Path(__file__).parent / "metaxy.toml"
_cwd_cfg = Path.cwd() / "metaxy.toml"
if "METAXY_CONFIG" not in os.environ:
    if _script_cfg.exists():
        os.environ["METAXY_CONFIG"] = str(_script_cfg)
    elif _cwd_cfg.exists():
        os.environ["METAXY_CONFIG"] = str(_cwd_cfg)

EMBEDDING_DIM = 64
METAXY_TIMESTAMP_COLUMNS = (
    "metaxy_created_at",
    "metaxy_updated_at",
    "metaxy_deleted_at",
)


class EmbeddingMapper:
    """Ray map_batches callable that computes mock embeddings."""

    def __call__(self, batch: pa.Table) -> pa.Table:
        import hashlib
        import struct

        text_idx = batch.schema.get_field_index("text")
        if text_idx < 0:
            raise ValueError("Expected 'text' column in batch")

        embeddings: list[list[float]] = []
        for text_value in batch.column(text_idx).to_pylist():
            text = "" if text_value is None else str(text_value)
            # Deterministic mock embedding from text hash
            h = hashlib.sha256(text.encode()).digest()
            emb = [
                struct.unpack("f", h[i : i + 4])[0] % 1.0
                for i in range(0, min(len(h), EMBEDDING_DIM * 4), 4)
            ]
            # Pad to EMBEDDING_DIM if needed
            emb.extend([0.0] * (EMBEDDING_DIM - len(emb)))
            embeddings.append(emb[:EMBEDDING_DIM])

        return batch.append_column(
            "embedding", pa.array(embeddings, type=pa.list_(pa.float32()))
        )


def run_embedding_pipeline(
    context: PipesContext | None = None,
) -> dict[str, Any]:
    """Run the embedding pipeline with metaxy incremental processing."""
    log_func = context.log.info if context else print

    cfg = mx.init()
    store = cfg.get_store()

    with store:
        increment = store.resolve_update("ray_example/embeddings")
        new_df = increment.new.to_polars()
        stale_df = increment.stale.to_polars()

    to_process = pl.concat([new_df, stale_df]) if len(stale_df) > 0 else new_df
    if isinstance(to_process, pl.Series):
        # Defensive: Ray expects tabular Arrow input, not a single Series.
        to_process = to_process.to_frame()
    count = len(to_process)
    log_func(
        f"Found {len(new_df)} new + {len(stale_df)} stale = {count} samples to process"
    )

    if count == 0:
        log_func("No samples to process - everything is up to date.")
        result = {
            "total_samples": 0,
            "embedding_dim": EMBEDDING_DIM,
            "status": "up_to_date",
        }
        if context:
            context.report_asset_materialization(metadata=result)
        return result

    t0 = time.time()

    arrow_table = to_process.to_arrow()
    log_func(f"Input schema for Ray: {arrow_table.schema}")
    ds = rd.from_arrow(arrow_table)
    log_func("Computing embeddings with Ray map_batches...")
    result_ds = ds.map_batches(
        EmbeddingMapper,
        batch_size=32,
        compute=ActorPoolStrategy(min_size=1, max_size=4),
        num_cpus=0,
        batch_format="pyarrow",
    )

    # Write results via MetaxyDatasink
    log_func("Writing results via MetaxyDatasink...")
    datasink = MetaxyDatasink(
        feature="ray_example/embeddings",
        store=store,
        config=cfg,
    )
    result_ds.write_datasink(datasink)
    write_stats = datasink.result

    elapsed = time.time() - t0

    result = {
        "total_samples": count,
        "embedding_dim": EMBEDDING_DIM,
        "duration_seconds": round(elapsed, 2),
        "rows_written": write_stats.rows_written,
        "rows_failed": write_stats.rows_failed,
        "status": "completed",
    }

    log_func(f"Embedding computation complete: {count} samples in {elapsed:.2f}s")

    if context:
        context.report_asset_materialization(metadata=result)

    return result


def main():
    """Entry point for Dagster Pipes mode."""
    context = PipesContext.get()

    def _get_extra(key: str) -> str | None:
        try:
            return context.get_extra(key)
        except DagsterPipesError:
            return None

    # Allow overriding the metaxy store via env or extras
    store_name = _get_extra("METAXY_STORE") or os.getenv(
        "METAXY_STORE", "ray_embeddings_dev"
    )
    os.environ["METAXY_STORE"] = store_name

    run_embedding_pipeline(context=context)


if __name__ == "__main__":
    if os.getenv("DAGSTER_PIPES_CONTEXT") or os.getenv("DAGSTER_PIPES_MESSAGES"):
        with open_dagster_pipes() as context:
            main()
    else:
        # Standalone mode
        if not ray.is_initialized():
            print("Initializing local Ray cluster...")
            ray.init()

        try:
            result = run_embedding_pipeline(context=None)
            print(f"\nResults: {result}")
        finally:
            if ray.is_initialized():
                ray.shutdown()
