"""Example 1: Simple metaxy + dagster integration with DuckDB store.

Demonstrates sample-level incremental processing with metaxy:
- `raw_numbers`: Root asset that registers sample data in metaxy.
- `processed_numbers`: Downstream asset that uses `store.resolve_update()`
  to only process new/changed samples (incremental).

Run these assets in the Dagster UI under the `metaxy_simple` group.
Re-materializing `raw_numbers` with different data and then materializing
`processed_numbers` will show only the changed samples being reprocessed.
"""

import dagster as dg
import metaxy as mx
import metaxy.ext.dagster as mxd
import polars as pl


@mxd.metaxify
@dg.asset(
    metadata={"metaxy/feature": "example/raw_numbers"},
    group_name="metaxy_simple",
)
def raw_numbers(store_simple: dg.ResourceParam[mx.MetadataStore]) -> pl.DataFrame:
    """Generate sample numerical data and register it in metaxy.

    This is a root feature - it creates the initial samples that
    downstream features depend on.
    """
    rows = [
        {"sample_uid": f"sample_{i:03d}", "value": float(i * 1.5), "category": cat}
        for i, cat in enumerate(
            [
                "alpha",
                "beta",
                "gamma",
                "alpha",
                "beta",
                "gamma",
                "alpha",
                "beta",
                "gamma",
                "delta",
            ],
        )
    ]
    df = pl.DataFrame(rows)

    # Root features provide per-field provenance tokens via samples.
    samples = df.with_columns(
        pl.struct(
            pl.col("value").cast(pl.String).alias("value"),
            pl.col("category").cast(pl.String).alias("category"),
        ).alias("metaxy_provenance_by_field")
    )

    with store_simple:
        increment = store_simple.resolve_update("example/raw_numbers", samples=samples)
        if len(increment.new) > 0:
            store_simple.write("example/raw_numbers", increment.new)
        if len(increment.stale) > 0:
            store_simple.write("example/raw_numbers", increment.stale)

    return samples


@mxd.metaxify
@dg.asset(
    metadata={"metaxy/feature": "example/processed_numbers"},
    deps=[raw_numbers],
    group_name="metaxy_simple",
)
def processed_numbers(  # noqa: C901
    store_simple: dg.ResourceParam[mx.MetadataStore],
) -> pl.DataFrame:
    """Process only new/changed samples using metaxy incremental resolution.

    Calls `store.resolve_update()` which returns an increment containing:
    - `increment.new`: samples that are new since last processing
    - `increment.stale`: samples whose upstream data changed

    Only these samples are processed, skipping already-processed unchanged ones.
    """
    with store_simple:
        increment = store_simple.resolve_update("example/processed_numbers")

    new_df = increment.new.to_polars()
    stale_df = increment.stale.to_polars()

    def _compute_results(df: pl.DataFrame) -> pl.DataFrame:
        if len(df) == 0:
            return df
        return df.with_columns((pl.col("value") ** 2).alias("result"))

    new_with_results = _compute_results(new_df)
    stale_with_results = _compute_results(stale_df)

    # Combine new and stale samples that need processing
    to_process = (
        pl.concat([new_with_results, stale_with_results])
        if len(stale_with_results) > 0
        else new_with_results
    )

    if len(to_process) == 0:
        dg.get_dagster_logger().info(
            "No new or stale samples to process - everything is up to date."
        )
        return pl.DataFrame(schema={"sample_uid": pl.String, "result": pl.Float64})

    dg.get_dagster_logger().info(
        f"Processing {len(new_df)} new + {len(stale_df)} stale = "
        f"{len(to_process)} samples (incremental)"
    )

    with store_simple:
        if len(new_with_results) > 0:
            store_simple.write("example/processed_numbers", new_with_results)
        if len(stale_with_results) > 0:
            store_simple.write("example/processed_numbers", stale_with_results)

    return to_process.select(pl.col("sample_uid"), pl.col("result"))
