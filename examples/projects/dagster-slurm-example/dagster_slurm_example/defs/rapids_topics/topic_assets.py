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
from typing import Optional

import dagster as dg
from dagster_slurm import ComputeResource, SlurmRunConfig
from dagster_slurm.config.environment import ExecutionMode
from pydantic import Field

MONTHS = ["1987-02", "1987-03", "1987-04", "1987-06", "1987-10"]
SEEDS = ["0", "1", "2"]

lda_partitions = dg.MultiPartitionsDefinition(
    {
        "month": dg.StaticPartitionsDefinition(MONTHS),
        "seed": dg.StaticPartitionsDefinition(SEEDS),
    }
)

GROUP = "rapids_topics"

_CPU_ENV_PIN = os.getenv("RAPIDS_TOPICS_CPU_ENV", "").strip()
_GPU_ENV_PIN = os.getenv("RAPIDS_TOPICS_GPU_ENV", "").strip()
_NO_REUSE = os.getenv("RAPIDS_TOPICS_NO_ENV_REUSE", "").strip() == "1"

_CPU_PACK_CMD = [
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

_RAPIDS_PACK_CMD = [
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

# Pack metadata for all assets; the optional env-var pin rides here
# because asset metadata takes precedence over per-run overrides.
_CPU_PACK_METADATA = {
    "slurm_pack_cmd": _CPU_PACK_CMD,
    **({"slurm_pre_deployed_env_path": _CPU_ENV_PIN} if _CPU_ENV_PIN else {}),
}
_RAPIDS_PACK_METADATA = {
    "slurm_pack_cmd": _RAPIDS_PACK_CMD,
    **({"slurm_pre_deployed_env_path": _GPU_ENV_PIN} if _GPU_ENV_PIN else {}),
}


def _published_env_path(
    context: dg.AssetExecutionContext, head_asset: str
) -> Optional[str]:
    """Env path the family-head payload published, or None.

    Read from the head asset's latest materialization metadata; None
    until the head has run once (the downstream asset then packs on its
    own) or when reuse is disabled.
    """
    if _NO_REUSE:
        return None
    event = context.instance.get_latest_materialization_event(dg.AssetKey(head_asset))
    if event is None or event.asset_materialization is None:
        return None
    value = event.asset_materialization.metadata.get("published_env_path")
    return value.value if isinstance(value, dg.TextMetadataValue) else None


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


class TopicSlurmConfig(SlurmRunConfig):
    """Slurm sizing + environment knobs, editable per run in the launchpad.

    Every field defaults to None, meaning "use the asset's deployment-aware
    default" (see ``_cpu_slurm_opts`` / ``_gpu_slurm_opts``). Overrides only
    apply in Slurm modes; local mode ignores them.
    """

    cpus_per_task: Optional[int] = Field(
        default=None, gt=0, description="Override CPUs for this run"
    )
    mem: Optional[str] = Field(
        default=None, description="Override memory request, e.g. '64G'"
    )
    time_limit: Optional[str] = Field(
        default=None, description="Override wall time, e.g. '00:30:00'"
    )
    gpus_per_node: Optional[int] = Field(
        default=None, ge=0, description="Override GPU count for this run"
    )
    pre_deployed_env_path: Optional[str] = Field(
        default=None,
        description=(
            "Skip env packing and use this already-extracted environment on "
            "the cluster (dir with activate.sh and env/bin/python). If the "
            "RAPIDS_TOPICS_*_ENV variable is also set, the variable (asset "
            "metadata) takes precedence; unset it to control this from the "
            "launchpad."
        ),
    )


def _merged_slurm_opts(defaults: dict, config: TopicSlurmConfig) -> dict:
    """Deployment-aware defaults, overridden by launchpad-set fields."""
    opts = dict(defaults)
    for field in ("cpus_per_task", "mem", "time_limit", "gpus_per_node"):
        value = getattr(config, field)
        if value is not None:
            opts[field] = value
    return opts


def _run_overrides(
    config: TopicSlurmConfig,
    compute: ComputeResource,
    published_path: Optional[str] = None,
) -> dict:
    """Extra compute.run kwargs; only emitted when a path is known.

    Launchpad config wins over the published path (env-var pins ride in
    asset metadata, which the client prefers over this kwarg). Dropped
    in local mode: LocalPipesClient does not accept the override kwarg,
    and there is no packed env to skip locally anyway.
    """
    if compute.mode == ExecutionMode.LOCAL:
        return {}
    path = config.pre_deployed_env_path or published_path
    if path:
        return {"pre_deployed_env_path_override": path}
    return {}


class CorpusConfig(TopicSlurmConfig):
    """Configuration for Reuters corpus preparation."""

    dict_keep_n: int = Field(
        default=20000,
        gt=0,
        description="Dictionary size cap (shared vocabulary across all models)",
    )


class LdaConfig(TopicSlurmConfig):
    """Configuration for per-partition LDA training."""

    num_topics: int = Field(default=15, gt=0, le=500)
    passes: int = Field(default=5, gt=0, le=100)
    top_terms: int = Field(default=10, gt=0, le=50)
    min_docs: int = Field(
        default=50,
        gt=0,
        description="Skip months with fewer documents than this",
    )


class UmapConfig(TopicSlurmConfig):
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


class HdbscanConfig(TopicSlurmConfig):
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
        extra_slurm_opts=_merged_slurm_opts(_cpu_slurm_opts(), config),
        **_run_overrides(config, compute),
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
        extra_slurm_opts=_merged_slurm_opts(_cpu_slurm_opts(), config),
        **_run_overrides(
            config, compute, _published_env_path(context, "reuters_corpus")
        ),
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
    config: TopicSlurmConfig,
):
    return compute.run(
        context=context,
        payload_path=_payload("aggregate_topics.py"),
        config=config,
        extra_env=_base_env(),
        extra_slurm_opts=_merged_slurm_opts(_cpu_slurm_opts(), config),
        **_run_overrides(
            config, compute, _published_env_path(context, "reuters_corpus")
        ),
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
        extra_slurm_opts=_merged_slurm_opts(_gpu_slurm_opts(), config),
        **_run_overrides(config, compute),
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
        extra_slurm_opts=_merged_slurm_opts(_gpu_slurm_opts(), config),
        **_run_overrides(
            config, compute, _published_env_path(context, "umap_embedding")
        ),
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
    config: TopicSlurmConfig,
):
    return compute.run(
        context=context,
        payload_path=_payload("topic_map.py"),
        config=config,
        extra_env=_base_env(),
        extra_slurm_opts=_merged_slurm_opts(_cpu_slurm_opts(), config),
        **_run_overrides(
            config, compute, _published_env_path(context, "umap_embedding")
        ),
    ).get_results()
