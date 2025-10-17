---
sidebar_position: 1
title: Choose an execution mode
---

`ComputeResource` adapts Dagster assets to different deployment targets. Picking the right execution mode keeps developer feedback loops tight while fitting production constraints.

## Mode summary

| Mode | Description | Typical use | Requirements |
| --- | --- | --- | --- |
| `local` | Runs assets directly on the Dagster code location process. | Laptop development, CI smoke tests. | No Slurm or SSH connectivity. |
| `slurm` | Submits one Slurm job per asset materialization. | Staging clusters, simple production setups. | `SlurmResource` with queue/partition configuration. |
| `slurm-session` | Keeps a Slurm allocation alive and reuses it across assets. | Production pipelines with warm clusters or expensive startup costs. | `SlurmResource` plus `SlurmSessionResource`. Optional cluster reuse toggles. |
| `slurm-hetjob` | Bundles multiple resource profiles into one heterogeneous Slurm job. | Workloads that mix CPU-only and GPU-heavy stages. | Slurm partitions that support heterogeneous submissions. |

Switch modes by updating the `ComputeResource.mode` field—asset code stays identical.

## Local mode

```python
compute = ComputeResource(
    mode="local",
    default_launcher=BashLauncher(),
)
```

- Zero Slurm dependencies.
- Perfect for authoring and unit testing assets.
- Combine with `LocalPipesClient` features such as log tailing.

## Slurm mode (per asset job)

```python
compute = ComputeResource(
    mode="slurm",
    slurm=SlurmResource(
        ssh=SSHConnectionResource.from_env(prefix="SLURM_EDGE_NODE_"),
        queue=SlurmQueueConfig(partition="compute", qos="normal"),
    ),
    default_launcher=BashLauncher(),
)
```

- Each run file-packages the asset environment using `pixi-pack` (unless `pre_deployed_env_path` is set).
- Jobs terminate as soon as the asset finishes—ideal for isolated workloads.
- Override `launcher=` on individual assets to run Ray or Spark workloads inside the allocation.

## Slurm session mode (cluster reuse)

```python
session = SlurmSessionResource(
    slurm=slurm,
    num_nodes=4,
    time_limit="06:00:00",
)

compute = ComputeResource(
    mode="slurm-session",
    slurm=slurm,
    session=session,
    default_launcher=RayLauncher(num_gpus_per_node=4),
    enable_cluster_reuse=True,
    cluster_reuse_tolerance=0.15,
)
```

- Dagster maintains a persistent allocation controlled by the session resource.
- With `enable_cluster_reuse=True`, launchers such as `RayLauncher` or `SparkLauncher` keep their clusters warm.
- Use `compute.cleanup_cluster(...)` to force teardown after heavyweight jobs.

## Heterogeneous job mode

```python
from dagster_slurm import ComputeResource, RayLauncher, SlurmResource
from dagster_slurm.managers import HetJobComponent

compute = ComputeResource(
    mode="slurm-hetjob",
    slurm=slurm,
    default_launcher=BashLauncher(),
)

@asset(required_resource_keys={"compute"})
def training_pipeline(context: AssetExecutionContext):
    components = [
        HetJobComponent(
            component_id="preprocess",
            launcher=BashLauncher(),
            extras={},
        ),
        HetJobComponent(
            component_id="train",
            launcher=RayLauncher(num_gpus_per_node=4),
            extras={},
        ),
    ]
    return context.resources.compute.run_heterogeneous(context=context, components=components)
```

- Stitch CPU and GPU workloads into one submission for better queue times.
- Each component can use a different launcher; Dagster coordinates metadata and logs.

## Choosing a launcher per mode

| Mode | Recommended launcher(s) | Notes |
| --- | --- | --- |
| `local` | `BashLauncher`, `RayLauncher` (single node) | Keeps development parity with production launchers. |
| `slurm` | `BashLauncher`, `RayLauncher`, `SparkLauncher` | Each asset gets a fresh allocation. |
| `slurm-session` | `RayLauncher`, `SparkLauncher` | Designed for clusters that benefit from reuse. |
| `slurm-hetjob` | Mix of launchers | Pair CPU-only steps with GPU steps efficiently. |

Remember to include launcher-specific dependencies (e.g. `ray`) in your pixi environment or `pyproject.toml`.
