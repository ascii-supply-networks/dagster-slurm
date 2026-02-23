"""Minimal Metaxy incremental demo (no Dagster, Polars + DuckDB :memory:).

Shows how many rows are:
- new
- stale
- orphaned (removed upstream)
- processed

Run:
    pixi run -e dev -- python scripts/metaxy_incremental_minidemo.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

# Reuse existing feature specs:
# - example/raw_numbers
# - example/processed_numbers
import dagster_slurm_example_shared.metaxy_features  # noqa: F401
import metaxy as mx
import narwhals as nw
import polars as pl


def _print_preview(label: str, df: pl.DataFrame, columns: list[str] | None = None) -> None:
    print(f"\n[{label}]")
    if columns is not None:
        print(df.select(columns))
    else:
        print(df)


def _build_config_file() -> Path:
    cfg_text = """
project = "metaxy_minidemo"
entrypoints = ["dagster_slurm_example_shared.metaxy_features"]
auto_create_tables = true

[stores.demo]
type = "metaxy.ext.metadata_stores.duckdb.DuckDBMetadataStore"
[stores.demo.config]
database = ":memory:"
"""
    tmp = tempfile.NamedTemporaryFile(
        prefix="metaxy_minidemo_", suffix=".toml", delete=False
    )
    path = Path(tmp.name)
    path.write_text(cfg_text)
    tmp.close()
    return path


def _as_root_samples(rows: list[dict[str, object]]) -> pl.DataFrame:
    raw = pl.DataFrame(rows)
    return raw.with_columns(
        pl.struct(
            pl.col("value").cast(pl.String).alias("value"),
            pl.col("category").cast(pl.String).alias("category"),
        ).alias("metaxy_provenance_by_field")
    )


def _upsert_root(
    store: mx.MetadataStore, rows: list[dict[str, object]]
) -> dict[str, int]:
    samples = _as_root_samples(rows)
    _print_preview("root input", samples, ["sample_uid", "file_uri", "value", "category"])

    inc = store.resolve_update("example/raw_numbers", samples=samples)
    new_df = inc.new.to_polars()
    stale_df = inc.stale.to_polars()
    orphaned_df = inc.orphaned.to_polars()

    if len(new_df) > 0:
        store.write("example/raw_numbers", inc.new)
    if len(stale_df) > 0:
        store.write("example/raw_numbers", inc.stale)

    return {
        "new": len(new_df),
        "stale": len(stale_df),
        "orphaned": len(orphaned_df),
    }


def _compute_result(df: pl.DataFrame) -> pl.DataFrame:
    if len(df) == 0:
        return df
    return df.with_columns(
        (pl.col("value") ** 2).alias("result"),
        pl.when(pl.col("value") < 50)
        .then(pl.lit("lt_50"))
        .otherwise(pl.lit("gte_50"))
        .alias("value_bucket"),
    )


def _process_downstream(store: mx.MetadataStore) -> dict[str, int]:
    inc = store.resolve_update("example/processed_numbers")
    new_df = inc.new.to_polars()
    stale_df = inc.stale.to_polars()
    orphaned_df = inc.orphaned.to_polars()

    new_out = _compute_result(new_df)
    stale_out = _compute_result(stale_df)

    if len(new_out) > 0:
        store.write("example/processed_numbers", new_out)
    if len(stale_out) > 0:
        store.write("example/processed_numbers", stale_out)

    # Optional cleanup to reflect upstream removals in downstream table.
    if len(orphaned_df) > 0 and "sample_uid" in orphaned_df.columns:
        removed_ids = orphaned_df["sample_uid"].to_list()
        store.delete(
            "example/processed_numbers",
            filters=nw.col("sample_uid").is_in(removed_ids),
            soft=False,
            with_feature_history=True,
            with_sample_history=True,
        )

    latest = store.read("example/processed_numbers").collect().to_polars()

    _print_preview(
        "processed latest rows",
        latest.select(["sample_uid", "result", "value_bucket"]).sort("sample_uid"),
    )

    return {
        "new": len(new_df),
        "stale": len(stale_df),
        "orphaned": len(orphaned_df),
        "processed": len(new_out) + len(stale_out),
    }


def _run_round(
    round_name: str,
    rows: list[dict[str, object]],
    store: mx.MetadataStore,
    previous_uids: set[str],
) -> set[str]:
    print("\n" + "=" * 72)
    print(f"{round_name}")
    print("=" * 72)

    current_uids = {str(row["sample_uid"]) for row in rows}
    removed_from_input = previous_uids - current_uids

    root_stats = _upsert_root(store, rows)
    print(
        f"[root increment] new={root_stats['new']} stale={root_stats['stale']} "
        f"removed_from_input={len(removed_from_input)}"
    )

    proc_stats = _process_downstream(store)
    print(
        f"[processed increment] new={proc_stats['new']} stale={proc_stats['stale']} "
        f"orphaned={proc_stats['orphaned']} processed={proc_stats['processed']}"
    )
    return current_uids


def main() -> None:
    config_path = _build_config_file()
    cfg = mx.init(config=config_path)
    store = cfg.get_store("demo")

    round1 = [
        {
            "sample_uid": "avatar_001",
            "file_uri": "s3://anam/raw/avatar_001.mp4",
            "value": 10.0,
            "category": "video/full",
        },
        {
            "sample_uid": "avatar_002",
            "file_uri": "s3://anam/raw/avatar_002.mp4",
            "value": 20.0,
            "category": "transcript/whisper",
        },
        {
            "sample_uid": "avatar_003",
            "file_uri": "s3://anam/raw/avatar_003.mp4",
            "value": 30.0,
            "category": "video/face_crop",
        },
    ]
    # avatar_002 changes value/category, avatar_003 removed, avatar_004 added
    round2 = [
        {
            "sample_uid": "avatar_001",
            "file_uri": "s3://anam/raw/avatar_001.mp4",
            "value": 10.0,
            "category": "video/full",
        },
        {
            "sample_uid": "avatar_002",
            "file_uri": "s3://anam/raw/avatar_002.mp4",
            "value": 25.0,
            "category": "transcript/whisper_v2",
        },
        {
            "sample_uid": "avatar_004",
            "file_uri": "s3://anam/raw/avatar_004.mp4",
            "value": 60.0,
            "category": "video/face_crop",
        },
    ]
    # identical to round2 -> should be no-op
    round3 = list(round2)

    with store:
        prev_uids: set[str] = set()
        prev_uids = _run_round("ROUND 1: initial load", round1, store, prev_uids)
        prev_uids = _run_round(
            "ROUND 2: one changed + one added + one removed",
            round2,
            store,
            prev_uids,
        )
        _run_round("ROUND 3: no changes", round3, store, prev_uids)

    print("\nDone.")


if __name__ == "__main__":
    main()
