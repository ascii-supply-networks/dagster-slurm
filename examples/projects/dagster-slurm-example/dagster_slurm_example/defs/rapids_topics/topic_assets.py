"""RAPIDS topic-modeling example: LDA -> cuML UMAP -> cuML HDBSCAN.

A public-data version of a production Common Crawl topic-modeling
pipeline. The chain demonstrates Slurm scheduling with dagster-slurm:

- CPU stages (corpus prep, per-partition gensim LDA, aggregation) run
  as sbatch jobs in the ``workload-topic-modeling`` packed env.
- GPU stages (UMAP, HDBSCAN, report) run in the
  ``packaged-cluster-rapids`` env with cuML, requesting one GPU on
  supercomputer deployments. On GPU-less machines (docker slurm, CI,
  local dev) the payloads fall back to umap-learn / hdbscan.

``lda_models`` is partitioned by (month, seed): one Slurm job per
partition, the per-partition fan-out of the production pipeline at toy
scale (5 populated Reuters months x 3 seeds = 15 jobs).
"""

import os
from importlib.resources import files

import dagster as dg
from dagster_slurm import ComputeResource, SlurmRunConfig
from pydantic import Field

# Only the Reuters-21578 months with usable (dated, non-empty body)
# documents. May/Jul/Aug/Sep are empty in the corpus, so including them
# would create partitions that always skip. March holds ~55% of the docs.
MONTHS = ["1987-02", "1987-03", "1987-04", "1987-06", "1987-10"]
SEEDS = ["0", "1", "2"]

lda_partitions = dg.MultiPartitionsDefinition(
    {
        "month": dg.StaticPartitionsDefinition(MONTHS),
        "seed": dg.StaticPartitionsDefinition(SEEDS),
    }
)

GROUP = "rapids_topics"

_CPU_PACK_METADATA = {
    "slurm_pack_cmd": [
        "pixi",
        "run",
        "-e",
        "opstooling",
        "--frozen",
        "python",
        "scripts/pack_environment.py",
        "--env",
        "workload-topic-modeling",
        "--build-missing",
    ]
}

_RAPIDS_PACK_METADATA = {
    "slurm_pack_cmd": [
        "pixi",
        "run",
        "-e",
        "opstooling",
        "--frozen",
        "python",
        "scripts/pack_environment.py",
        "--env",
        "packaged-cluster-rapids",
        "--build-missing",
    ]
}


def _payload(name: str) -> str:
    return str(files("dagster_slurm_example_hpc_workload.rapids_topics") / name)


def _base_env() -> dict:
    """Forward RAPIDS_TOPICS_BASE when set; payloads default to $HOME."""
    base = os.getenv("RAPIDS_TOPICS_BASE", "").strip()
    return {"RAPIDS_TOPICS_BASE": base} if base else {}


def _is_supercomputer() -> bool:
    return "supercomputer" in os.getenv("DAGSTER_DEPLOYMENT", "development")


def _cpu_slurm_opts(cpus: int = 2, mem: str = "4G") -> dict:
    """Single-node CPU job sizing: modest in docker, larger on real HPC.

    ``gpus_per_node: 0`` is explicit so CPU stages never hold a GPU
    allocation on sites whose default queue is a GPU partition (e.g.
    datalab's GPU-a100s).
    """
    if _is_supercomputer():
        return {
            "nodes": 1,
            "cpus_per_task": 8,
            "mem": "32G",
            "gpus_per_node": 0,
        }
    return {"nodes": 1, "cpus_per_task": cpus, "mem": mem, "gpus_per_node": 0}


def _gpu_slurm_opts() -> dict:
    """GPU job sizing: one GPU on supercomputers, CPU fallback elsewhere."""
    if _is_supercomputer():
        return {
            "nodes": 1,
            "cpus_per_task": 8,
            "mem": "32G",
            "gpus_per_node": 1,
        }
    return {"nodes": 1, "cpus_per_task": 2, "mem": "4G", "gpus_per_node": 0}


class CorpusConfig(SlurmRunConfig):
    """Configuration for Reuters corpus preparation."""

    dict_keep_n: int = Field(
        default=20000,
        gt=0,
        description="Dictionary size cap (shared vocabulary across all models)",
    )


class LdaConfig(SlurmRunConfig):
    """Configuration for per-partition LDA training."""

    num_topics: int = Field(default=15, gt=0, le=500)
    passes: int = Field(default=5, gt=0, le=100)
    top_terms: int = Field(default=10, gt=0, le=50)
    min_docs: int = Field(
        default=50,
        gt=0,
        description="Skip months with fewer documents than this",
    )


class UmapConfig(SlurmRunConfig):
    """Configuration for UMAP reduction of topic-term vectors."""

    n_components: int = Field(default=2, gt=1, le=100)
    n_neighbors: int = Field(default=15, gt=1, le=200)
    min_dist: float = Field(default=0.1, ge=0.0, le=1.0)
    metric: str = Field(default="cosine")
    random_state: int = Field(default=42)
    build_algo: str = Field(
        default="auto",
        description=(
            "cuML UMAP kNN builder: 'auto' picks brute-force for small "
            "inputs and nn-descent at scale. Only set 'nn_descent' "
            "explicitly for large corpora (>~150 topic vectors); forcing "
            "it on a small input crashes cuML."
        ),
    )


class HdbscanConfig(SlurmRunConfig):
    """Configuration for HDBSCAN meta-topic clustering."""

    min_cluster_size: int = Field(default=3, gt=1)
    min_samples: int = Field(default=1, gt=0)


@dg.asset(
    group_name=GROUP,
    description="Download + parse Reuters-21578, build the shared dictionary",
    metadata=_CPU_PACK_METADATA,
)
def reuters_corpus(
    context: dg.AssetExecutionContext,
    compute: ComputeResource,
    config: CorpusConfig,
):
    return compute.run(
        context=context,
        payload_path=_payload("prepare_corpus.py"),
        config=config,
        extra_env={"DICT_KEEP_N": str(config.dict_keep_n), **_base_env()},
        extra_slurm_opts=_cpu_slurm_opts(),
    ).get_results()


@dg.asset(
    group_name=GROUP,
    partitions_def=lda_partitions,
    deps=[reuters_corpus],
    description=(
        "One gensim LDA model per (month, seed) partition: one Slurm job "
        "each, the fan-out stage of the pipeline"
    ),
    metadata=_CPU_PACK_METADATA,
)
def lda_models(
    context: dg.AssetExecutionContext,
    compute: ComputeResource,
    config: LdaConfig,
):
    keys = context.partition_key.keys_by_dimension  # type: ignore[union-attr]
    return compute.run(
        context=context,
        payload_path=_payload("lda_train.py"),
        config=config,
        extra_env={
            "MONTH": keys["month"],
            "SEED": keys["seed"],
            "NUM_TOPICS": str(config.num_topics),
            "PASSES": str(config.passes),
            "TOP_TERMS": str(config.top_terms),
            "MIN_DOCS": str(config.min_docs),
            **_base_env(),
        },
        extra_slurm_opts=_cpu_slurm_opts(),
    ).get_results()


@dg.asset(
    group_name=GROUP,
    deps=[lda_models],
    description="Stack topic-term vectors from all LDA models",
    metadata=_CPU_PACK_METADATA,
)
def topic_term_matrix(
    context: dg.AssetExecutionContext,
    compute: ComputeResource,
    config: SlurmRunConfig,
):
    return compute.run(
        context=context,
        payload_path=_payload("aggregate_topics.py"),
        config=config,
        extra_env=_base_env(),
        extra_slurm_opts=_cpu_slurm_opts(),
    ).get_results()


@dg.asset(
    group_name=GROUP,
    deps=[topic_term_matrix],
    description=(
        "UMAP reduction of topic-term vectors: cuML on GPU nodes, "
        "umap-learn fallback on CPU"
    ),
    metadata=_RAPIDS_PACK_METADATA,
)
def umap_embedding(
    context: dg.AssetExecutionContext,
    compute: ComputeResource,
    config: UmapConfig,
):
    return compute.run(
        context=context,
        payload_path=_payload("umap_reduce.py"),
        config=config,
        extra_env={
            "N_COMPONENTS": str(config.n_components),
            "N_NEIGHBORS": str(config.n_neighbors),
            "MIN_DIST": str(config.min_dist),
            "METRIC": config.metric,
            "RANDOM_STATE": str(config.random_state),
            "UMAP_BUILD_ALGO": config.build_algo,
            **_base_env(),
        },
        extra_slurm_opts=_gpu_slurm_opts(),
    ).get_results()


@dg.asset(
    group_name=GROUP,
    deps=[umap_embedding],
    description=(
        "HDBSCAN meta-topic clustering: cuML on GPU nodes, contrib "
        "hdbscan fallback on CPU"
    ),
    metadata=_RAPIDS_PACK_METADATA,
)
def hdbscan_meta_topics(
    context: dg.AssetExecutionContext,
    compute: ComputeResource,
    config: HdbscanConfig,
):
    return compute.run(
        context=context,
        payload_path=_payload("hdbscan_cluster.py"),
        config=config,
        extra_env={
            "MIN_CLUSTER_SIZE": str(config.min_cluster_size),
            "MIN_SAMPLES": str(config.min_samples),
            **_base_env(),
        },
        extra_slurm_opts=_gpu_slurm_opts(),
    ).get_results()


@dg.asset(
    group_name=GROUP,
    deps=[hdbscan_meta_topics],
    description="Labeled meta-topic scatter plot + JSON summary",
    metadata=_RAPIDS_PACK_METADATA,
)
def topic_map(
    context: dg.AssetExecutionContext,
    compute: ComputeResource,
    config: SlurmRunConfig,
):
    return compute.run(
        context=context,
        payload_path=_payload("topic_map.py"),
        config=config,
        extra_env=_base_env(),
        extra_slurm_opts=_cpu_slurm_opts(),
    ).get_results()
