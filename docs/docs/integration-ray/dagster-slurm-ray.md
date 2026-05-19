---
sidebar_position: 2
title: Dagster orchestration with Ray
---

This companion page focuses on how Dagster concepts map onto the Ray launcher and how to structure assets when multiple Ray jobs must cooperate inside the same pipeline.

## Resource wiring patterns

### Single launcher shared across many assets

```python title="repository.py"
from dagster import Definitions
from dagster_slurm import ComputeResource, RayLauncher

ray_launcher = RayLauncher(num_gpus_per_node=0)

defs = Definitions(
    assets=[train_model, evaluate_model, export_model],
    resources={
        "compute": ComputeResource(
            mode="slurm",
            slurm=slurm_resource,
            default_launcher=ray_launcher,
        ),
    },
)
```

The shared launcher keeps every asset on the same Ray launcher configuration while still running each one in its own Slurm job.

### One run-owned Ray allocation

When several dependent Ray assets use the same Slurm shape, configure the Ray resource for run scope:

```python title="repository.py"
from dagster import Definitions
from dagster_slurm import (
    ComputeResource,
    RayLauncher,
    SlurmAllocationScope,
    SlurmRunAllocationConfig,
)

defs = Definitions(
    assets=[train_model, evaluate_model, export_model],
    resources={
        "compute_ray": ComputeResource(
            mode="slurm",
            slurm=slurm_resource,
            default_launcher=RayLauncher(num_gpus_per_node=1),
            allocation_scope=SlurmAllocationScope.RUN,
            run_allocation=SlurmRunAllocationConfig(
                num_nodes=2,
                gpus_per_node=1,
                cpus_per_task=8,
                mem="64G",
                time_limit="04:00:00",
            ),
        ),
    },
)
```

This starts Ray once for the Dagster run and injects the same `RAY_ADDRESS` into each compatible asset step.

### Asset-specific overrides

```python title="assets/pipeline.py"
from dagster_slurm import RayLauncher


@asset(required_resource_keys={"compute"})
def preprocess(context):
    launcher = RayLauncher(num_gpus_per_node=0)
    return context.resources.compute.run(context=context, payload_path="python -m jobs.preprocess", launcher=launcher)


@asset(required_resource_keys={"compute"})
def train(context, preprocess):
    launcher = RayLauncher(num_gpus_per_node=4)
    return context.resources.compute.run(context=context, payload_path="python -m jobs.train", launcher=launcher)
```

Each asset requests different resources without modifying the global configuration.

## Coordinating multi-step Ray workloads

Use Dagster's `MultiAssetSensor`, `AssetCheck`, or `AutomationCondition` to coordinate complex Ray jobs:

- Launch a Ray Tune sweep from one asset, then materialize aggregated metrics in a follow-up asset that depends on the first.
- Fan out inference shards as individual materializations by emitting multiple Dagster outputs from a single Ray job.
- Use `context.defer_asset` to schedule dependent Ray assets once the cluster has produced a success marker.

## Compatibility rules

Run-scoped Ray allocation is intentionally strict. All assets in the run must use compatible Ray launcher settings and the same Slurm allocation shape. Put shared Slurm settings in `SlurmRunAllocationConfig`; keep asset-specific environment variables in `extra_env`.

## Mixing Ray with other launchers

You can orchestrate heterogeneous jobs by combining Ray with Bash or Spark (WIP) launchers:

```python
from dagster_slurm import BashLauncher, RayLauncher


@asset(required_resource_keys={"compute"})
def preprocess(context):
    return context.resources.compute.run(
        context=context,
        payload_path="python scripts/preprocess.py",
        launcher=BashLauncher(),
    )


@asset(required_resource_keys={"compute"})
def train(context, preprocess):
    return context.resources.compute.run(
        context=context,
        payload_path="python -m jobs.train",
        launcher=RayLauncher(num_gpus_per_node=4),
    )
```

Dagster handles dependencies exactly as usual; each launcher builds its runtime inside the Slurm allocation assigned to that asset.

## Observability hooks

- Emit rich metadata by returning `completed.get_results()` and writing to `context.log` inside the Ray entrypoint.
- Use `context.resources.compute.stream_logs(cluster_id=...)` to tail the head-node logs while debugging.
- Attach Ray events to asset checks for SLO-style monitoring.

## Recommended project layout

```
dagster-slurm-example/
├── dagster_slurm_example/
│   ├── assets/
│   │   ├── preprocess.py
│   │   └── training.py
│   ├── jobs/
│   │   ├── preprocess.py       # Bash launcher
│   │   ├── train/
│   │   │   ├── entrypoint.py   # Ray launcher
│   │   │   └── workers.py
│   └── resources/
│       └── __init__.py         # ComputeResource definition
└── pixi.toml
```

Keep Ray worker code close to the Dagster repository so `pixi-pack` bundles the right sources and dependencies.

## Next steps

- Configure Spark (WIP) alongside Ray using the [Spark integration guide](../integration-spark/spark.md).
- Explore the [API reference](../api/api_core.md#class-dagster_slurmraylauncherdata) for every launcher parameter.
- Share success stories or issues on GitHub—feedback helps shape the launcher roadmap.
