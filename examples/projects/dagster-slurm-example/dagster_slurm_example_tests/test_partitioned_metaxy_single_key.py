from typing import Any, cast

import dagster as dg
import polars as pl
from dagster_slurm_example.defs.metaxy import simple_metaxy_assets as assets


class _FrameHandle:
    def __init__(self, df: pl.DataFrame):
        self._df = df

    def to_polars(self) -> pl.DataFrame:
        return self._df

    def collect(self):
        return self

    def collect_schema(self):
        return self._df.collect_schema()


class _Increment:
    def __init__(self, new_df: pl.DataFrame, stale_df: pl.DataFrame):
        self.new = _FrameHandle(new_df)
        self.stale = _FrameHandle(stale_df)


class _FakeStore:
    def __init__(
        self,
        increment: _Increment,
        raw_numbers_df: pl.DataFrame,
        processed_schema_df: pl.DataFrame,
    ):
        self._increment = increment
        self._raw_numbers_df = raw_numbers_df
        self._processed_schema_df = processed_schema_df
        self.resolve_update_calls: list[dict[str, Any]] = []
        self.writes: list[tuple[str, pl.DataFrame]] = []
        self.deletes: list[dict[str, object]] = []

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        return False

    def resolve_update(
        self, feature_key: str, target_filters: list[Any] | None = None
    ) -> _Increment:
        self.resolve_update_calls.append(
            {"feature_key": feature_key, "target_filters": target_filters}
        )
        return self._increment

    def read(self, feature_key: str):
        if feature_key == "example/partitioned_processed_numbers":
            return _FrameHandle(self._processed_schema_df)
        if feature_key == "example/raw_numbers":
            return _FrameHandle(self._raw_numbers_df)
        raise KeyError(feature_key)

    def write(self, feature_key: str, frame: pl.DataFrame):
        self.writes.append((feature_key, frame))

    def delete(
        self,
        feature_key: str,
        *,
        filters,
        soft: bool,
        with_feature_history: bool,
        with_sample_history: bool,
    ):
        self.deletes.append(
            {
                "feature_key": feature_key,
                "filters": filters,
                "soft": soft,
                "with_feature_history": with_feature_history,
                "with_sample_history": with_sample_history,
            }
        )


def _call_partitioned_asset(
    partition_key: str,
    sample_uid: str | None,
    subsample_pct: int | None,
    store: _FakeStore,
) -> pl.DataFrame:
    partitioned_asset = cast(Any, assets.partitioned_processed_numbers)
    fn = partitioned_asset.node_def.compute_fn.decorated_fn
    context = dg.build_asset_context(partition_key=partition_key)
    config = assets.PartitionedProcessedNumbersConfig(
        sample_uid=sample_uid,
        subsample_pct=subsample_pct,
    )
    return fn(context, store, config)


def test_partitioned_single_key_forces_reprocess():
    increment = _Increment(
        new_df=pl.DataFrame(
            {
                "sample_uid": ["sample_001", "sample_999"],
                "value": [10.0, 30.0],
                "category": ["alpha", "beta"],
            }
        ),
        stale_df=pl.DataFrame(schema={"sample_uid": pl.String, "value": pl.Float64}),
    )
    raw_numbers_df = pl.DataFrame(
        {
            "sample_uid": ["sample_001", "sample_999"],
            "value": [10.0, 30.0],
            "category": ["alpha", "beta"],
        }
    )
    processed_schema_df = pl.DataFrame(
        schema={
            "sample_uid": pl.String,
            "result": pl.Float64,
            "value_bucket": pl.String,
        }
    )
    store = _FakeStore(increment, raw_numbers_df, processed_schema_df)

    result = _call_partitioned_asset("lt_50", "sample_001", None, store)

    assert result["sample_uid"].to_list() == ["sample_001"]
    assert result["result"].to_list() == [100.0]
    assert result["value_bucket"].to_list() == ["lt_50"]
    assert len(store.writes) == 1
    assert store.writes[0][0] == "example/partitioned_processed_numbers"
    assert store.writes[0][1]["sample_uid"].to_list() == ["sample_001"]
    assert len(store.deletes) == 1
    assert store.deletes[0]["feature_key"] == "example/partitioned_processed_numbers"
    assert len(store.resolve_update_calls) == 1
    assert (
        store.resolve_update_calls[0]["feature_key"]
        == "example/partitioned_processed_numbers"
    )
    assert len(store.resolve_update_calls[0]["target_filters"]) == 2


def test_partitioned_single_key_raises_when_key_not_in_selected_partition():
    increment = _Increment(
        new_df=pl.DataFrame(schema={"sample_uid": pl.String, "value": pl.Float64}),
        stale_df=pl.DataFrame(schema={"sample_uid": pl.String, "value": pl.Float64}),
    )
    raw_numbers_df = pl.DataFrame(
        {"sample_uid": ["sample_090"], "value": [90.0], "category": ["gamma"]}
    )
    processed_schema_df = pl.DataFrame(
        schema={
            "sample_uid": pl.String,
            "result": pl.Float64,
            "value_bucket": pl.String,
        }
    )
    store = _FakeStore(increment, raw_numbers_df, processed_schema_df)

    try:
        _call_partitioned_asset("lt_50", "sample_090", None, store)
        raised = False
    except ValueError as exc:
        raised = True
        assert "sample_uid='sample_090' not found in this partition" in str(exc)

    assert raised
    assert len(store.writes) == 0


def test_partitioned_single_key_rejects_blank_sample_uid():
    increment = _Increment(
        new_df=pl.DataFrame(schema={"sample_uid": pl.String, "value": pl.Float64}),
        stale_df=pl.DataFrame(schema={"sample_uid": pl.String, "value": pl.Float64}),
    )
    store = _FakeStore(
        increment=increment,
        raw_numbers_df=pl.DataFrame(
            schema={"sample_uid": pl.String, "value": pl.Float64}
        ),
        processed_schema_df=pl.DataFrame(
            schema={
                "sample_uid": pl.String,
                "result": pl.Float64,
                "value_bucket": pl.String,
            }
        ),
    )

    partitioned_asset = cast(Any, assets.partitioned_processed_numbers)
    fn = partitioned_asset.node_def.compute_fn.decorated_fn
    context = dg.build_asset_context(partition_key="lt_50")

    try:
        fn(
            context,
            store,
            assets.PartitionedProcessedNumbersConfig(sample_uid="   "),
        )
        raised = False
    except ValueError as exc:
        raised = True
        assert "sample_uid must be non-empty" in str(exc)

    assert raised


def test_partitioned_subsample_processes_random_subset():
    increment = _Increment(
        new_df=pl.DataFrame(
            {
                "sample_uid": [f"sample_{i:03d}" for i in range(10)],
                "value": [float(i) for i in range(10)],
                "category": ["alpha"] * 10,
            }
        ),
        stale_df=pl.DataFrame(schema={"sample_uid": pl.String, "value": pl.Float64}),
    )
    store = _FakeStore(
        increment=increment,
        raw_numbers_df=pl.DataFrame(
            {
                "sample_uid": [f"sample_{i:03d}" for i in range(10)],
                "value": [float(i) for i in range(10)],
                "category": ["alpha"] * 10,
            }
        ),
        processed_schema_df=pl.DataFrame(
            schema={
                "sample_uid": pl.String,
                "result": pl.Float64,
                "value_bucket": pl.String,
            }
        ),
    )

    result = _call_partitioned_asset("lt_50", None, 50, store)

    assert len(result) == 5
    assert len(store.writes) == 1
    assert len(store.writes[0][1]) == 5


def test_partitioned_rejects_sample_uid_with_subsample_pct():
    increment = _Increment(
        new_df=pl.DataFrame(schema={"sample_uid": pl.String, "value": pl.Float64}),
        stale_df=pl.DataFrame(schema={"sample_uid": pl.String, "value": pl.Float64}),
    )
    store = _FakeStore(
        increment=increment,
        raw_numbers_df=pl.DataFrame(
            schema={"sample_uid": pl.String, "value": pl.Float64}
        ),
        processed_schema_df=pl.DataFrame(
            schema={
                "sample_uid": pl.String,
                "result": pl.Float64,
                "value_bucket": pl.String,
            }
        ),
    )

    try:
        _call_partitioned_asset("lt_50", "sample_001", 50, store)
        raised = False
    except ValueError as exc:
        raised = True
        assert "mutually exclusive" in str(exc)

    assert raised
