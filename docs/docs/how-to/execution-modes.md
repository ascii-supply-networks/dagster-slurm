---
sidebar_position: 1
title: Choose an execution mode
---

`ComputeResource` adapts Dagster assets to different deployment targets. Picking the right execution mode keeps developer feedback loops tight while fitting production constraints.

## Mode summary

| Mode | Description | Typical use | Requirements |
| --- | --- | --- | --- |
| `local` | Runs assets directly on the Dagster code location process. | Laptop development, CI smoke tests. | No Slurm or SSH connectivity. |
| `slurm` | Submits one Slurm job per asset materialization. | Staging clusters, production pipelines today. | `SlurmResource` with queue/partition configuration. |

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

## Work in progress

Session reuse (`slurm-session`) and heterogeneous jobs (`slurm-hetjob`) are on the roadmap. Configuration stubs remain in the API so early adopters can experiment, but we recommend planning production systems around the stable `local` and `slurm` modes for now.



## Choosing a launcher per mode

| Mode | Recommended launcher(s) | Notes |
| --- | --- | --- |
| `local` | `BashLauncher`, `RayLauncher` (single node) | Keeps development parity with production launchers. |
| `slurm` | `BashLauncher`, `RayLauncher`, `SparkLauncher` | Each asset gets a fresh allocation. Session-based reuse will extend this list once stabilised. |

Remember to include launcher-specific dependencies (e.g. `ray`) in your pixi environment or `pyproject.toml`.
