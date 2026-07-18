"""Reduce topic-term vectors with UMAP (cuML on GPU, umap-learn on CPU).

Reads the stacked topic-term matrix, reduces every topic vector
(dimension = shared vocabulary size) to N_COMPONENTS dimensions and
writes the embedding alongside the topic identity columns.

Backend selection happens at import time: if cuML is importable the
GPU implementation is used, otherwise umap-learn. This is what lets
one payload serve both the docker slurm cluster (CPU) and a GPU node.

Environment:
    RAPIDS_TOPICS_BASE  base dir (default: $HOME/rapids_topics)
    N_COMPONENTS        embedding dimensions (default: 2)
    N_NEIGHBORS         UMAP n_neighbors (default: 15)
    MIN_DIST            UMAP min_dist (default: 0.1)
    METRIC              UMAP metric (default: cosine)
    RANDOM_STATE        seed (default: 42)
"""

import os
import sys
from pathlib import Path

from dagster_pipes import PipesContext, open_dagster_pipes

ENV_REUSE_LINK = "rapids_topics_env_gpu"


def publish_env_for_reuse(env_prefix: Path, link: Path) -> bool:
    """Symlink ``link`` at the packed env this payload runs in.

    The first asset of each env family packs and pushes its environment;
    publishing it under a stable path lets the downstream assets point
    ``slurm_pre_deployed_env_path`` here and skip packing. Returns False
    when the interpreter is not inside a packed cluster env (local dev),
    detected by the ``<base>/activate.sh`` + ``<base>/env`` layout.
    """
    base = env_prefix.parent
    if env_prefix.name != "env" or not (base / "activate.sh").exists():
        return False
    tmp = link.with_name(link.name + ".tmp")
    if tmp.is_symlink() or tmp.exists():
        tmp.unlink()
    tmp.symlink_to(base)
    tmp.replace(link)  # atomic swap so readers never see a missing link
    return True

# ---- cuML compat -----------------------------------------------------
# cuML provides GPU UMAP with an API similar to umap-learn. Try cuML
# first and fall back to the CPU library, so the same payload runs on
# whichever env it lands in.
try:
    import cuml  # type: ignore

    cuml.set_global_output_type("numpy")
    _HAS_CUML = True
except ImportError:
    _HAS_CUML = False


def make_umap(
    *,
    n_components: int,
    n_neighbors: int,
    min_dist: float,
    metric: str,
    random_state: int,
    build_algo: str,
):
    """UMAP factory: cuML UMAP on GPU, umap-learn on CPU.

    ``build_algo`` only affects the cuML branch. "auto" (the cuML
    default) picks brute-force kNN for small inputs and nn-descent
    (GPU-accelerated approximate kNN) once the corpus is large enough
    to need it. Forcing "nn_descent" on a tiny input (< ~150 rows)
    crashes cuML with a CUDA invalid-argument error, because the
    approximate-graph degree collapses below the kernel's minimum, so
    keep the default unless the corpus is genuinely large.
    """
    if _HAS_CUML:
        from cuml.manifold import UMAP as _UMAP  # ty: ignore[unresolved-import]

        return _UMAP(
            n_components=n_components,
            n_neighbors=n_neighbors,
            min_dist=min_dist,
            metric=metric,
            random_state=random_state,
            build_algo=build_algo,
            verbose=True,
        )
    from umap import UMAP as _UMAP  # ty: ignore[unresolved-import]

    return _UMAP(
        n_components=n_components,
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        metric=metric,
        random_state=random_state,
        low_memory=True,
        verbose=True,
    )


def main():
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    context = PipesContext.get()
    base = Path(
        os.path.expandvars(os.environ.get("RAPIDS_TOPICS_BASE", "$HOME/rapids_topics"))
    ).expanduser()

    table = pq.read_table(base / "topic_term_matrix" / "topics.parquet")
    matrix = np.asarray(table.column("vector").to_pylist(), dtype=np.float32)
    n_samples = matrix.shape[0]
    backend = "cuml (GPU)" if _HAS_CUML else "umap-learn (CPU)"
    context.log.info(f"UMAP via {backend}: {n_samples} x {matrix.shape[1]}")

    # n_neighbors must stay below the sample count; the topic-term
    # matrix is small (one row per LDA topic across all models), so
    # clamp for the toy-scale default without capping large runs.
    n_neighbors = max(2, min(int(os.environ.get("N_NEIGHBORS", "15")), n_samples - 1))
    # "auto" lets cuML pick brute-force vs nn-descent by size; forcing
    # nn-descent on a small input crashes the CUDA kernel.
    build_algo = os.environ.get("UMAP_BUILD_ALGO", "auto")

    n_components = int(os.environ.get("N_COMPONENTS", "2"))
    reducer = make_umap(
        n_components=n_components,
        n_neighbors=n_neighbors,
        min_dist=float(os.environ.get("MIN_DIST", "0.1")),
        metric=os.environ.get("METRIC", "cosine"),
        random_state=int(os.environ.get("RANDOM_STATE", "42")),
        build_algo=build_algo,
    )
    embedding = np.asarray(reducer.fit_transform(matrix), dtype=np.float32)

    out_dir = base / "umap"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = table.drop_columns(["vector"]).append_column(
        "embedding", pa.array(list(embedding), type=pa.list_(pa.float32()))
    )
    pq.write_table(out, out_dir / "embedding.parquet")

    try:
        if publish_env_for_reuse(Path(sys.prefix), Path.home() / ENV_REUSE_LINK):
            context.log.info(
                f"Published env for downstream reuse: ~/{ENV_REUSE_LINK}"
            )
    except OSError as exc:
        context.log.warning(f"Could not publish env symlink: {exc}")

    context.report_asset_materialization(
        metadata={
            "backend": backend,
            "n_vectors": int(n_samples),
            "input_dim": int(matrix.shape[1]),
            "n_components": n_components,
            "n_neighbors": n_neighbors,
            "build_algo": build_algo if _HAS_CUML else "n/a (CPU)",
            "output_path": str(out_dir / "embedding.parquet"),
        }
    )


if __name__ == "__main__":
    with open_dagster_pipes():
        main()
