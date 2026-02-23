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

# Register shared metaxy feature definitions used by @metaxify.
import dagster_slurm_example_shared.metaxy_features  # noqa: F401
import metaxy as mx
import metaxy.ext.dagster as mxd
import narwhals as nw
import polars as pl

NUMBER_PARTITIONS = dg.StaticPartitionsDefinition(["lt_50", "gte_50"])


def _partition_pl_filter(partition_key: str) -> pl.Expr:
    if partition_key == "lt_50":
        return pl.col("value") < 50
    if partition_key == "gte_50":
        return pl.col("value") >= 50
    raise ValueError(f"Unknown partition key: {partition_key}")


def _partition_nw_filter(partition_key: str) -> nw.Expr:
    if partition_key == "lt_50":
        return nw.col("value") < 50
    if partition_key == "gte_50":
        return nw.col("value") >= 50
    raise ValueError(f"Unknown partition key: {partition_key}")


def _compute_results(df: pl.DataFrame) -> pl.DataFrame:
    if len(df) == 0:
        return df
    return df.with_columns(
        (pl.col("value") ** 2).alias("result"),
        pl.when(pl.col("value") < 50)
        .then(pl.lit("lt_50"))
        .otherwise(pl.lit("gte_50"))
        .alias("value_bucket"),
    )


def _is_schema_mismatch_error(exc: Exception) -> bool:
    error_text = str(exc)
    return (
        "Column name/value mismatch for insert" in error_text
        or "expected" in error_text
        and "columns but" in error_text
        and "values were supplied" in error_text
    )


def _ensure_current_feature_schema(
    store: mx.MetadataStore,
    feature_key: str,
    required_output_columns: set[str],
    context_logger: dg.DagsterLogManager | None = None,
) -> None:
    """One-time migration helper for legacy feature tables.

    Older local duckdb files can contain feature tables created with a previous
    schema. If required output columns are missing, drop feature metadata and let
    Metaxy recreate it on next write.
    """
    try:
        with store:
            schema_names = set(store.read(feature_key).collect_schema().names())
    except Exception:
        # Feature not materialized yet (or no readable metadata) - nothing to migrate.
        return

    if required_output_columns.issubset(schema_names):
        return

    logger = context_logger or dg.get_dagster_logger()
    logger.info(
        f"Detected legacy schema for {feature_key}; applying one-time metadata reset."
    )
    with store:
        store.drop_feature_metadata(feature_key)


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
    categories = ["alpha", "beta", "gamma", "delta"]
    rows = [
        {
            "sample_uid": f"sample_{i:03d}",
            "value": float(i * 2.5),
            "category": categories[i % len(categories)],
        }
        for i in range(40)
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
    deps=[dg.AssetKey(["example", "raw_numbers"])],
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
    _ensure_current_feature_schema(
        store_simple,
        "example/processed_numbers",
        {"result", "value_bucket"},
    )

    with store_simple:
        increment = store_simple.resolve_update("example/processed_numbers")

    new_df = increment.new.to_polars()
    stale_df = increment.stale.to_polars()

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
        return pl.DataFrame(
            schema={
                "sample_uid": pl.String,
                "result": pl.Float64,
                "value_bucket": pl.String,
            }
        )

    dg.get_dagster_logger().info(
        f"Processing {len(new_df)} new + {len(stale_df)} stale = "
        f"{len(to_process)} samples (incremental)"
    )

    try:
        with store_simple:
            if len(new_with_results) > 0:
                store_simple.write("example/processed_numbers", new_with_results)
            if len(stale_with_results) > 0:
                store_simple.write("example/processed_numbers", stale_with_results)
    except Exception as e:
        if not _is_schema_mismatch_error(e):
            raise

        logger = dg.get_dagster_logger()
        logger.warning(
            "Detected legacy schema mismatch in example/processed_numbers. "
            "Dropping feature metadata and retrying with current schema."
        )
        with store_simple:
            store_simple.drop_feature_metadata("example/processed_numbers")
            retry_increment = store_simple.resolve_update("example/processed_numbers")
            retry_new = _compute_results(retry_increment.new.to_polars())
            retry_stale = _compute_results(retry_increment.stale.to_polars())
            if len(retry_new) > 0:
                store_simple.write("example/processed_numbers", retry_new)
            if len(retry_stale) > 0:
                store_simple.write("example/processed_numbers", retry_stale)

    return to_process.select(
        pl.col("sample_uid"), pl.col("result"), pl.col("value_bucket")
    )


@mxd.metaxify
@dg.asset(
    metadata={"metaxy/feature": "example/partitioned_processed_numbers"},
    deps=[dg.AssetKey(["example", "raw_numbers"])],
    group_name="metaxy_simple",
    partitions_def=NUMBER_PARTITIONS,
)
def partitioned_processed_numbers(
    context: dg.AssetExecutionContext,
    store_simple: dg.ResourceParam[mx.MetadataStore],
) -> pl.DataFrame:
    """Partitioned incremental processing split by value bucket."""
    partition_key = context.partition_key
    if partition_key is None:
        raise ValueError("partitioned_processed_numbers requires a partition key")

    partition_filter = _partition_pl_filter(partition_key)
    target_filter = _partition_nw_filter(partition_key)

    _ensure_current_feature_schema(
        store_simple,
        "example/partitioned_processed_numbers",
        {"result", "value_bucket"},
        context_logger=context.log,
    )

    with store_simple:
        increment = store_simple.resolve_update(
            "example/partitioned_processed_numbers",
            target_filters=[target_filter],
        )

    new_df = increment.new.to_polars().filter(partition_filter)
    stale_df = increment.stale.to_polars().filter(partition_filter)

    new_with_results = _compute_results(new_df)
    stale_with_results = _compute_results(stale_df)
    to_process = (
        pl.concat([new_with_results, stale_with_results])
        if len(stale_with_results) > 0
        else new_with_results
    )

    if len(to_process) == 0:
        context.log.info(
            f"[{partition_key}] No new/stale samples to process in this partition."
        )
        return pl.DataFrame(
            schema={
                "sample_uid": pl.String,
                "result": pl.Float64,
                "value_bucket": pl.String,
            }
        )

    context.log.info(
        f"[{partition_key}] Processing {len(new_df)} new + {len(stale_df)} stale = "
        f"{len(to_process)} samples"
    )

    with store_simple:
        if len(new_with_results) > 0:
            store_simple.write(
                "example/partitioned_processed_numbers", new_with_results
            )
        if len(stale_with_results) > 0:
            store_simple.write(
                "example/partitioned_processed_numbers", stale_with_results
            )

    return to_process.select(
        pl.col("sample_uid"), pl.col("result"), pl.col("value_bucket")
    )
