"""Cluster the UMAP embedding with HDBSCAN (cuML on GPU, CPU fallback).

Groups the reduced topic vectors into meta-topics: clusters of topics
that recur across the per-(month, seed) LDA models. Label -1 is
HDBSCAN noise (topics that don't recur).

Environment:
    RAPIDS_TOPICS_BASE  base dir (default: $HOME/rapids_topics)
    MIN_CLUSTER_SIZE    HDBSCAN min_cluster_size (default: 3)
    MIN_SAMPLES         HDBSCAN min_samples (default: 1)
"""

import os
from pathlib import Path

from dagster_pipes import PipesContext, open_dagster_pipes

try:
    import cuml  # type: ignore

    cuml.set_global_output_type("numpy")
    _HAS_CUML = True
except ImportError:
    _HAS_CUML = False

# cuML 25.x HDBSCAN hard-caps ``min_samples`` at 1023; the CPU library
# accepts arbitrary values. Clamp on the cuML branch only.
CUML_HDBSCAN_MIN_SAMPLES_CAP = 1023


def clamped_min_samples(min_samples: int) -> int:
    """min_samples clamped to cuML's hard cap."""
    return min(min_samples, CUML_HDBSCAN_MIN_SAMPLES_CAP)


def make_hdbscan(*, min_cluster_size: int, min_samples: int):
    """HDBSCAN factory: cuML when available, contrib hdbscan otherwise."""
    if _HAS_CUML:
        from cuml.cluster import HDBSCAN as _HDBSCAN  # ty: ignore[unresolved-import]

        return _HDBSCAN(
            min_cluster_size=min_cluster_size,
            min_samples=clamped_min_samples(min_samples),
            output_type="numpy",
        )
    import hdbscan  # ty: ignore[unresolved-import]

    return hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=min_samples,
        core_dist_n_jobs=-1,
    )


def main():
    import numpy as np
    import pyarrow.parquet as pq
    from pyarrow import array as pa_array

    context = PipesContext.get()
    base = Path(
        os.path.expandvars(os.environ.get("RAPIDS_TOPICS_BASE", "$HOME/rapids_topics"))
    ).expanduser()

    table = pq.read_table(base / "umap" / "embedding.parquet")
    embedding = np.asarray(table.column("embedding").to_pylist(), dtype=np.float32)
    backend = "cuml (GPU)" if _HAS_CUML else "hdbscan (CPU)"
    context.log.info(f"HDBSCAN via {backend}: {embedding.shape[0]} points")

    clusterer = make_hdbscan(
        min_cluster_size=int(os.environ.get("MIN_CLUSTER_SIZE", "3")),
        min_samples=int(os.environ.get("MIN_SAMPLES", "1")),
    )
    labels = np.asarray(clusterer.fit_predict(embedding), dtype=np.int32)

    out_dir = base / "hdbscan"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = table.append_column("cluster", pa_array(labels))
    pq.write_table(out, out_dir / "clusters.parquet")

    n_clusters = int(len(set(labels.tolist()) - {-1}))
    noise_fraction = float((labels == -1).mean())
    context.log.info(f"{n_clusters} meta-topics, {noise_fraction:.1%} noise")
    context.report_asset_materialization(
        metadata={
            "backend": backend,
            "n_points": int(embedding.shape[0]),
            "n_meta_topics": n_clusters,
            "noise_fraction": round(noise_fraction, 3),
            "output_path": str(out_dir / "clusters.parquet"),
        }
    )


if __name__ == "__main__":
    with open_dagster_pipes():
        main()
