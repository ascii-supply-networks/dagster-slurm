"""Train one gensim LDA model for a (month, seed) partition.

Loads the shared dictionary built by ``prepare_corpus.py``, trains an
LDA model on the month's documents with ``random_state=seed`` and
writes the model's topic-term vectors (rows of ``get_topics()``, one
row per topic over the shared vocabulary) plus top terms to parquet.
The downstream UMAP/HDBSCAN stages cluster these rows across all
(month, seed) models into meta-topics, mirroring the multi-model
aggregation of the production pipeline.

Months with fewer than MIN_DOCS documents are skipped. Reuters-21578's
dated stories cluster in a few 1987 months (March alone is ~55%), and
several months have no usable documents.

Environment:
    RAPIDS_TOPICS_BASE  base dir (default: $HOME/rapids_topics)
    MONTH               partition month, e.g. "1987-03"
    SEED                integer random seed
    NUM_TOPICS          topics per model (default: 15)
    PASSES              gensim passes (default: 5)
    TOP_TERMS           top terms to store per topic (default: 10)
    MIN_DOCS            skip threshold (default: 50)
"""

import logging
import os
from pathlib import Path

from dagster_pipes import PipesContext, open_dagster_pipes


def main():
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq
    from gensim.corpora import Dictionary
    from gensim.models import LdaModel

    context = PipesContext.get()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    base = Path(
        os.path.expandvars(os.environ.get("RAPIDS_TOPICS_BASE", "$HOME/rapids_topics"))
    ).expanduser()
    month = os.environ["MONTH"]
    seed = int(os.environ["SEED"])
    num_topics = int(os.environ.get("NUM_TOPICS", "15"))
    passes = int(os.environ.get("PASSES", "5"))
    top_terms = int(os.environ.get("TOP_TERMS", "10"))
    min_docs = int(os.environ.get("MIN_DOCS", "50"))

    month_path = base / "corpus" / f"month={month}" / "docs.parquet"
    if not month_path.exists():
        context.log.warning(f"No corpus slice for {month}; skipping partition.")
        context.report_asset_materialization(
            metadata={"month": month, "seed": seed, "skipped": True, "n_docs": 0}
        )
        return

    table = pq.read_table(month_path)
    tokens = table.column("tokens").to_pylist()
    if len(tokens) < min_docs:
        context.log.warning(
            f"{month}: {len(tokens)} docs < MIN_DOCS={min_docs}; skipping."
        )
        context.report_asset_materialization(
            metadata={
                "month": month,
                "seed": seed,
                "skipped": True,
                "n_docs": len(tokens),
            }
        )
        return

    dictionary = Dictionary.load(str(base / "corpus" / "dictionary.dict"))
    bow = [dictionary.doc2bow(doc) for doc in tokens]
    context.log.info(
        f"Training LDA[{month}|seed={seed}]: {len(bow)} docs, "
        f"{len(dictionary)} terms, k={num_topics}, passes={passes}"
    )

    model = LdaModel(
        corpus=bow,
        id2word=dictionary,
        num_topics=num_topics,
        passes=passes,
        random_state=seed,
        alpha="auto",
        eta="auto",
    )

    topic_term = model.get_topics().astype(np.float32)
    terms = [
        [term for term, _ in model.show_topic(t, topn=top_terms)]
        for t in range(num_topics)
    ]
    out_dir = base / "lda" / f"month={month}" / f"seed={seed}"
    out_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "month": [month] * num_topics,
                "seed": [seed] * num_topics,
                "topic_id": list(range(num_topics)),
                "top_terms": terms,
                "vector": list(topic_term),
            }
        ),
        out_dir / "topic_terms.parquet",
    )

    perplexity = float(np.exp2(-model.log_perplexity(bow)))
    context.log.info(f"Done. perplexity={perplexity:.1f}")
    context.report_asset_materialization(
        metadata={
            "month": month,
            "seed": seed,
            "n_docs": len(bow),
            "num_topics": num_topics,
            "perplexity": round(perplexity, 1),
            "sample_topic": ", ".join(terms[0]),
            "output_dir": str(out_dir),
        }
    )


if __name__ == "__main__":
    with open_dagster_pipes():
        main()
