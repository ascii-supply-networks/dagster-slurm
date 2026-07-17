"""Render the meta-topic map: 2D scatter of topics colored by cluster.

Each point is one topic from one (month, seed) LDA model; clusters are
meta-topics recurring across models. Cluster labels are the most
frequent top-terms among member topics. Writes a PNG plus a JSON
summary of the meta-topics.

Environment:
    RAPIDS_TOPICS_BASE  base dir (default: $HOME/rapids_topics)
    LABEL_TERMS         terms per cluster label (default: 3)
"""

import json
import os
from collections import Counter
from pathlib import Path

from dagster_pipes import PipesContext, open_dagster_pipes


def cluster_labels(
    clusters: list[int], top_terms: list[list[str]], n_label_terms: int
) -> dict[int, str]:
    """Label each cluster with its members' most frequent top-terms."""
    term_counts: dict[int, Counter] = {}
    for cluster, terms in zip(clusters, top_terms, strict=True):
        if cluster < 0:
            continue
        term_counts.setdefault(cluster, Counter()).update(terms)
    return {
        cluster: ", ".join(term for term, _ in counts.most_common(n_label_terms))
        for cluster, counts in term_counts.items()
    }


def main():
    import matplotlib  # ty: ignore[unresolved-import]

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt  # ty: ignore[unresolved-import]
    import numpy as np
    import pyarrow.parquet as pq

    context = PipesContext.get()
    base = Path(
        os.path.expandvars(os.environ.get("RAPIDS_TOPICS_BASE", "$HOME/rapids_topics"))
    ).expanduser()

    table = pq.read_table(base / "hdbscan" / "clusters.parquet")
    embedding = np.asarray(table.column("embedding").to_pylist(), dtype=np.float32)
    clusters = table.column("cluster").to_pylist()
    top_terms = table.column("top_terms").to_pylist()
    labels = cluster_labels(
        clusters, top_terms, int(os.environ.get("LABEL_TERMS", "3"))
    )

    fig, ax = plt.subplots(figsize=(10, 8))
    cluster_arr = np.asarray(clusters)
    noise = cluster_arr < 0
    ax.scatter(
        embedding[noise, 0], embedding[noise, 1], s=14, c="lightgray", label="noise"
    )
    cmap = plt.get_cmap("tab20")
    for i, cluster in enumerate(sorted(labels)):
        mask = cluster_arr == cluster
        ax.scatter(embedding[mask, 0], embedding[mask, 1], s=22, color=cmap(i % 20))
        cx, cy = embedding[mask, 0].mean(), embedding[mask, 1].mean()
        ax.annotate(
            labels[cluster],
            (cx, cy),
            fontsize=7,
            ha="center",
            bbox={"boxstyle": "round,pad=0.2", "fc": "white", "alpha": 0.75},
        )
    ax.set_title(
        "Reuters-21578 meta-topics: LDA topic-term vectors, "
        "UMAP-reduced, HDBSCAN-clustered"
    )
    ax.set_xticks([])
    ax.set_yticks([])

    out_dir = base / "topic_map"
    out_dir.mkdir(parents=True, exist_ok=True)
    png_path = out_dir / "topic_map.png"
    fig.savefig(png_path, dpi=150, bbox_inches="tight")

    summary = {
        str(cluster): {
            "label": label,
            "n_topics": int((cluster_arr == cluster).sum()),
        }
        for cluster, label in sorted(labels.items())
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2))

    context.report_asset_materialization(
        metadata={
            "n_meta_topics": len(labels),
            "n_noise_topics": int(noise.sum()),
            "plot_path": str(png_path),
            "summary_path": str(out_dir / "summary.json"),
        }
    )


if __name__ == "__main__":
    with open_dagster_pipes():
        main()
