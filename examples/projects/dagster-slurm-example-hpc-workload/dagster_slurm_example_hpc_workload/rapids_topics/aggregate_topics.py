"""Stack topic-term vectors from all (month, seed) LDA models.

Reads every ``lda/month=*/seed=*/topic_terms.parquet`` written by the
partitioned training stage and concatenates them into a single table.
Skipped partitions (months below the document threshold) simply have
no file and drop out here.
Environment:
    RAPIDS_TOPICS_BASE  base dir (default: $HOME/rapids_topics)
"""

import json
import os
from pathlib import Path

from dagster_pipes import PipesContext, open_dagster_pipes


def split_current_and_stale(tables_by_file: dict, expected_dim: int) -> tuple:
    """Partition model tables into current-vocabulary and stale ones."""
    current, stale = {}, {}
    for name, table in tables_by_file.items():
        vector_dim = len(table.column("vector")[0].as_py())
        (current if vector_dim == expected_dim else stale)[name] = table
    return current, stale


def main():
    import pyarrow.parquet as pq
    from pyarrow import concat_tables

    context = PipesContext.get()
    base = Path(
        os.path.expandvars(os.environ.get("RAPIDS_TOPICS_BASE", "$HOME/rapids_topics"))
    ).expanduser()

    vocabulary = json.loads(
        (base / "corpus" / "vocabulary.json").read_text(encoding="utf-8")
    )
    expected_dim = len(vocabulary)

    files = sorted((base / "lda").glob("month=*/seed=*/topic_terms.parquet"))
    if not files:
        raise RuntimeError(f"No topic_terms.parquet found under {base / 'lda'}")

    tables = {str(f.relative_to(base / "lda")): pq.read_table(f) for f in files}
    current, stale = split_current_and_stale(tables, expected_dim)
    for name in stale:
        context.log.warning(
            f"Skipping {name}: vector length "
            f"{len(stale[name].column('vector')[0].as_py())} does not match "
            f"the current vocabulary ({expected_dim}); re-run its "
            "lda_models partition."
        )
    if not current:
        raise RuntimeError(
            f"All {len(files)} model files are stale against the current "
            f"{expected_dim}-term vocabulary; re-run the lda_models partitions."
        )
    context.log.info(
        f"Stacking {len(current)} model outputs ({len(stale)} stale skipped)"
    )

    stacked = concat_tables(current.values())
    out_dir = base / "topic_term_matrix"
    out_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(stacked, out_dir / "topics.parquet")

    months = sorted(set(stacked.column("month").to_pylist()))
    context.report_asset_materialization(
        metadata={
            "n_models": len(current),
            "n_stale_models_skipped": len(stale),
            "n_topic_vectors": stacked.num_rows,
            "vector_dim": expected_dim,
            "months": ", ".join(months),
            "output_path": str(out_dir / "topics.parquet"),
        }
    )


if __name__ == "__main__":
    with open_dagster_pipes():
        main()
