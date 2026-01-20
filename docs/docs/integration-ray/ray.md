---
sidebar_position: 1
title: Ray on Slurm
---

Ray is bundled as a first-class launcher in `dagster-slurm`. This page explains how to enable it, how scheduling works across the different execution modes, and how to keep observability when your Ray workers live inside a Slurm allocation.

## When to reach for Ray

Use Ray when your asset logic benefits from distributed execution or GPU scheduling but you still want Dagster to orchestrate upstream/downstream dependencies. Typical patterns include:

- Scaling model training and hyper-parameter sweeps across multiple Slurm nodes.
- Running Ray data pipelines that need high-throughput IO.
- Mixing CPU- and GPU-heavy stages in the same asset (leveraging heterogeneous allocations).

If your workload is single-process, the default Bash launcher remains simpler.

## Prerequisites

- `dagster-slurm>=1.5.0`
- A pixi environment (local or pre-deployed) that installs `ray>=2.48.0`
- Slurm partitions with the resources required by your Ray workers

For containerised development the bundled docker-compose stack already provides these pieces.

## Configure the compute resource

Ray support starts with the standard `ComputeResource` from `dagster_slurm`. You provide:

1. An execution mode (`local` or `slurm`)
2. A `RayLauncher` either as the default launcher or per asset
3. Optional advanced settings (session reuse and heterogeneous jobs are under active development).

```python title="definitions/assets.py"
import dagster as dg
from dagster_slurm import (
    ComputeResource,
    ExecutionMode,
    RayLauncher,
    SlurmQueueConfig,
    SlurmResource,
    SSHConnectionResource,
)

ssh = SSHConnectionResource.from_env(prefix="SLURM_EDGE_NODE_")
slurm = SlurmResource(
    ssh=ssh,
    queue=SlurmQueueConfig(partition="compute", qos="normal", time_limit="01:00:00"),
)

compute = ComputeResource(
    mode=ExecutionMode.SLURM,
    slurm=slurm,
    default_launcher=RayLauncher(
        num_cpus_per_node=32,
        num_gpus_per_node=4,
        ray_version="2.9.3",
    ),
)
```

Add `compute` to your asset's dependencies using Dagster's standard resource injection.

## Control-plane asset example

```python title="assets/train.py"
import dagster as dg
from dagster_slurm import ComputeResource, RayLauncher


@dg.asset(required_resource_keys={"compute"})
def train_model(context: dg.AssetExecutionContext) -> None:
    ray_launcher = RayLauncher(
        num_cpus_per_node=16,
        num_gpus_per_node=2,
        working_dir="ray_jobs/train_model",
    )

    completed = context.resources.compute.run(
        context=context,
        payload_path="python -m train.entrypoint",
        launcher=ray_launcher,
        extra_env={"WANDB_MODE": "offline"},
        extras={"experiment": context.run.run_id},
    )
    yield from completed.get_results()
```

Key points:

- Override the launcher per asset when resource requirements differ from the default.
- `payload_path` can be a script path or module invocation (`python -m ...`). The Ray launcher ensures the command runs inside the Ray head node.
- `extras` travel to the user-code process via Dagster Pipes.

## User-plane entrypoint

```python title="train/entrypoint.py"
import ray
from dagster_pipes import PipesContext, open_dagster_pipes


@ray.remote(num_gpus=1)
def train_shard(config: dict[str, str]) -> dict[str, float]:
    # Real training logic goes here
    return {"loss": 0.01, "accuracy": 0.98}


def main() -> None:
    context = PipesContext.get()
    context.log.info("Launching Ray jobs...")
    handles = [train_shard.remote({"shard": i}) for i in range(4)]
    results = ray.get(handles)
    context.report_asset_materialization(
        metadata={"shards": len(results), "max_accuracy": max(r["accuracy"] for r in results)}
    )
    context.log.info("Training complete")


if __name__ == "__main__":
    with open_dagster_pipes():
        ray.init(address="auto")  # Connect to the cluster inside the Slurm allocation
        main()
```

The launcher exposes the head node address via Ray defaults, so `ray.init(address="auto")` connects automatically when workers run inside the same allocation.

## Execution modes and Ray behaviour

| Mode | Behaviour |
| --- | --- |
| `local` | Starts a local Ray runtime on the Dagster node. Ideal for development, no Slurm required. |
| `slurm` | Submits a Slurm job per asset; the `RayLauncher` bootstraps a Ray cluster inside that allocation and tears it down afterwards. |

> Session reuse and heterogeneous jobs are planned enhancements. Their configuration knobs remain in the API but are considered experimental until documented otherwise.

To mix Ray with other launchers, override `launcher=` per asset or per step.

## Resource sizing

- `num_cpus_per_node` and `num_gpus_per_node` determine Ray resource limits inside each Slurm node.
- `max_workers` caps the total worker count across the allocation. Leave unset to use all nodes granted by Slurm.
- `ray_args` lets you pass cli flags (e.g. `["--include-dashboard=true"]`) for debugging.
- In heterogeneous jobs, assign tags via `component_id` so each sub-allocation can run different Ray workloads.

## Packaging the environment

Set `ComputeResource.auto_detect_platform=True` (default) so pixi packs the right platform build for your cluster. For faster startup in production:

1. Pre-build the environment via `pixi run deploy-prod-docker`.
2. Point `ComputeResource.pre_deployed_env_path` to the installation.

Ray workers inherit that environment automatically.

## Observability

- Dagster logs capture Ray's stdout/stderr from the head node. Use the Dagster UI metadata for quick metrics.
- Enable the Ray dashboard by exposing the web UI through SSH tunnelling (`ray_args=["--dashboard-host=0.0.0.0"]`), then forward the port from the edge node.
- Slurm usage metrics (CPU efficiency, memory high-water mark, elapsed time) appear in the asset materialization metadata automatically.

## Real-world examples

- **[Document processing with docling](./docling-example.md)** – Quick start guide for PDF to markdown conversion
  - [Full application documentation](../applications/document-preprocessing-docling.md) with architecture, scaling, and troubleshooting

## Troubleshooting

- **Cluster never starts** – Ensure `ray` is installed in the packed environment and that your Slurm partition allows the requested CPUs/GPUs. Check the job logs under `${SLURM_DEPLOYMENT_BASE_PATH}/.../run.log`.
- **Workers cannot reach the head node** – Verify that intra-node networking is allowed on your cluster. Set `head_node_address` explicitly in `RayLauncher` if your setup uses custom hostnames.
- **Hanging shutdowns** – When session reuse becomes available, call `compute.cleanup_cluster(cluster_id)` after large jobs to force Ray teardown.

Need more? Open an issue with your cluster details so we can extend the launcher helpers.
