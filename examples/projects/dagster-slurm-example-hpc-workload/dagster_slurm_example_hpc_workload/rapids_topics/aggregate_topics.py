"""Stack topic-term vectors from all (month, seed) LDA models.

Reads every ``lda/month=*/seed=*/topic_terms.parquet`` written by the
partitioned training stage and concatenates them into a single table.
Skipped partitions (months below the document threshold) simply have
no file and drop out here.

Environment:
    RAPIDS_TOPICS_BASE  base dir (default: $HOME/rapids_topics)
"""

import os
from pathlib import Path

from dagster_pipes import PipesContext, open_dagster_pipes


def main():
    import pyarrow.parquet as pq
    from pyarrow import concat_tables

    context = PipesContext.get()
    base = Path(
        os.path.expandvars(os.environ.get("RAPIDS_TOPICS_BASE", "$HOME/rapids_topics"))
    ).expanduser()

    files = sorted((base / "lda").glob("month=*/seed=*/topic_terms.parquet"))
    if not files:
        raise RuntimeError(f"No topic_terms.parquet found under {base / 'lda'}")
    context.log.info(f"Stacking {len(files)} model outputs")

    stacked = concat_tables([pq.read_table(f) for f in files])
    out_dir = base / "topic_term_matrix"
    out_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(stacked, out_dir / "topics.parquet")

    months = sorted(set(stacked.column("month").to_pylist()))
    context.report_asset_materialization(
        metadata={
            "n_models": len(files),
            "n_topic_vectors": stacked.num_rows,
            "months": ", ".join(months),
            "output_path": str(out_dir / "topics.parquet"),
        }
    )


if __name__ == "__main__":
    with open_dagster_pipes():
        main()
