"""Render the meta-topic map: 2D scatter of topics colored by cluster.

Each point is one topic from one (month, seed) LDA model; clusters are
meta-topics recurring across models. Cluster labels are the most
frequent top-terms among member topics. Writes a PNG plus a JSON
summary of the meta-topics on the cluster filesystem, and streams both
back through Dagster Pipes.

Environment:
    RAPIDS_TOPICS_BASE  base dir (default: $HOME/rapids_topics)
    LABEL_TERMS         terms per cluster label (default: 3)
"""

import base64
import json
import os
from collections import Counter
from pathlib import Path
from typing import Optional

from dagster_pipes import PipesContext, open_dagster_pipes

MAX_INLINE_PLOT_BYTES = 750_000


def inline_plot_metadata(png_bytes: bytes) -> Optional[dict]:
    """Markdown metadata embedding the PNG, or None when it is too large."""
    if len(png_bytes) > MAX_INLINE_PLOT_BYTES:
        return None
    encoded = base64.b64encode(png_bytes).decode("ascii")
    return {
        "raw_value": f"![topic map](data:image/png;base64,{encoded})",
        "type": "md",
    }


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

    metadata = {
        "n_meta_topics": len(labels),
        "n_noise_topics": int(noise.sum()),
        "plot_path": {"raw_value": str(png_path), "type": "path"},
        "summary_path": {"raw_value": str(out_dir / "summary.json"), "type": "path"},
        "meta_topics": {"raw_value": summary, "type": "json"},
    }
    plot_preview = inline_plot_metadata(png_path.read_bytes())
    if plot_preview is not None:
        metadata["topic_map_preview"] = plot_preview
    else:
        context.log.warning(
            f"topic_map.png exceeds {MAX_INLINE_PLOT_BYTES} bytes; "
            "reporting the remote path only."
        )
    context.report_asset_materialization(metadata=metadata)


if __name__ == "__main__":
    with open_dagster_pipes():
        main()
