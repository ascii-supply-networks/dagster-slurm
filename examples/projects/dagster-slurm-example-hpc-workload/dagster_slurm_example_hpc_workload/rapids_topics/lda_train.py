"""Train one scikit-learn LDA model for a (month, seed) partition.

Loads the shared vocabulary built by ``prepare_corpus.py``, trains an
LDA model on the month's documents with ``random_state=seed`` and
writes the model's normalized topic-term vectors (one row per topic
over the shared vocabulary) plus top terms to parquet.
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
    MAX_ITER            maximum training iterations (default: 5)
    TOP_TERMS           top terms to store per topic (default: 10)
    MIN_DOCS            skip threshold (default: 50)
"""

import os
from pathlib import Path

from dagster_pipes import PipesContext, open_dagster_pipes


def main():
    import json

    import pyarrow as pa
    import pyarrow.parquet as pq
    from sklearn.decomposition import LatentDirichletAllocation
    from sklearn.feature_extraction.text import CountVectorizer

    context = PipesContext.get()

    base = Path(
        os.path.expandvars(os.environ.get("RAPIDS_TOPICS_BASE", "$HOME/rapids_topics"))
    ).expanduser()
    month = os.environ["MONTH"]
    seed = int(os.environ["SEED"])
    num_topics = int(os.environ.get("NUM_TOPICS", "15"))
    max_iter = int(os.environ.get("MAX_ITER", "5"))
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
    documents = table.column("body").to_pylist()
    if len(documents) < min_docs:
        context.log.warning(
            f"{month}: {len(documents)} docs < MIN_DOCS={min_docs}; skipping."
        )
        context.report_asset_materialization(
            metadata={
                "month": month,
                "seed": seed,
                "skipped": True,
                "n_docs": len(documents),
            }
        )
        return

    vocabulary = json.loads(
        (base / "corpus" / "vocabulary.json").read_text(encoding="utf-8")
    )
    vectorizer = CountVectorizer(vocabulary=vocabulary)
    document_term_matrix = vectorizer.transform(documents)
    context.log.info(
        f"Training LDA[{month}|seed={seed}]: {document_term_matrix.shape[0]} docs, "
        f"{document_term_matrix.shape[1]} terms, k={num_topics}, "
        f"max_iter={max_iter}"
    )

    model = LatentDirichletAllocation(
        n_components=num_topics,
        max_iter=max_iter,
        random_state=seed,
    ).fit(document_term_matrix)

    topic_term = model.components_.astype("float32")
    topic_term /= topic_term.sum(axis=1, keepdims=True)
    feature_names = vectorizer.get_feature_names_out()
    terms = [
        feature_names[topic.argsort()[-top_terms:][::-1]].tolist()
        for topic in topic_term
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

    perplexity = float(model.perplexity(document_term_matrix))
    context.log.info(f"Done. perplexity={perplexity:.1f}")
    context.report_asset_materialization(
        metadata={
            "month": month,
            "seed": seed,
            "n_docs": document_term_matrix.shape[0],
            "num_topics": num_topics,
            "perplexity": round(perplexity, 1),
            "sample_topic": ", ".join(terms[0]),
            "output_dir": str(out_dir),
        }
    )


if __name__ == "__main__":
    with open_dagster_pipes():
        main()
