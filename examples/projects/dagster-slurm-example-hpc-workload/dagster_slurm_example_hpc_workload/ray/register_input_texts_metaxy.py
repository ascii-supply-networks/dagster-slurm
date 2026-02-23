"""Workload script: register ray_example/input_texts in metaxy store."""

import os
from pathlib import Path

import dagster_slurm_example_shared.metaxy_features  # noqa: F401
import metaxy as mx
import polars as pl
from dagster_pipes import DagsterPipesError, PipesContext, open_dagster_pipes

# metaxy.toml is uploaded via extra_files. Prefer sibling path, fallback to CWD.
_script_cfg = Path(__file__).parent / "metaxy.toml"
_cwd_cfg = Path.cwd() / "metaxy.toml"
if "METAXY_CONFIG" not in os.environ:
    if _script_cfg.exists():
        os.environ["METAXY_CONFIG"] = str(_script_cfg)
    elif _cwd_cfg.exists():
        os.environ["METAXY_CONFIG"] = str(_cwd_cfg)


def _build_input_text_samples() -> pl.DataFrame:
    texts = [
        "The quick brown fox jumps over the lazy dog",
        "Machine learning enables pattern recognition",
        "High performance computing accelerates science",
        "Distributed systems process data in parallel",
        "Natural language processing understands text",
        "Data pipelines transform raw inputs to features",
        "Incremental processing saves compute resources",
        "Metadata tracking ensures reproducibility",
    ]
    base = pl.DataFrame(
        [{"sample_uid": f"text_{i:03d}", "text": text} for i, text in enumerate(texts)]
    )
    return base.with_columns(
        pl.struct(
            pl.col("text").cast(pl.String).alias("text"),
        ).alias("metaxy_provenance_by_field")
    )


def _register_input_texts(context: PipesContext | None) -> dict[str, int | str]:
    log = context.log.info if context else print

    cfg = mx.init()
    store = cfg.get_store()
    samples = _build_input_text_samples()

    with store:
        increment = store.resolve_update("ray_example/input_texts", samples=samples)
        if len(increment.new) > 0:
            store.write("ray_example/input_texts", increment.new)
        if len(increment.stale) > 0:
            store.write("ray_example/input_texts", increment.stale)

    result: dict[str, int | str] = {
        "feature": "ray_example/input_texts",
        "new_samples": len(increment.new),
        "stale_samples": len(increment.stale),
        "status": "completed",
    }
    log(
        f"Registered ray_example/input_texts: "
        f"{result['new_samples']} new, {result['stale_samples']} stale"
    )
    if context:
        context.report_asset_materialization(metadata=result)
    return result


def main() -> None:
    context = PipesContext.get()

    def _get_extra(key: str) -> str | None:
        try:
            return context.get_extra(key)
        except DagsterPipesError:
            return None

    store_name = _get_extra("METAXY_STORE") or os.getenv(
        "METAXY_STORE", "ray_embeddings_dev"
    )
    os.environ["METAXY_STORE"] = store_name

    _register_input_texts(context)


if __name__ == "__main__":
    if os.getenv("DAGSTER_PIPES_CONTEXT") or os.getenv("DAGSTER_PIPES_MESSAGES"):
        with open_dagster_pipes():
            main()
    else:
        _register_input_texts(context=None)
