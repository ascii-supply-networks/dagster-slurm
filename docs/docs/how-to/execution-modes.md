---
sidebar_position: 1
title: Choose an execution mode
---

`ComputeResource` adapts Dagster assets to different deployment targets. Picking the right execution mode keeps developer feedback loops tight while fitting production constraints.

:::info No persistent server required

`dagster-slurm` works well as a **laptop-first tool**. Submit long-running HPC jobs, close your laptop, and check results later — Slurm jobs run to completion independently of the Dagster process. A retry reattaches to any job that was still running when Dagster stopped. See [Laptop and one-off researcher deployments](./troubleshooting.md#laptop-and-one-off-researcher-deployments) for recommended setup.

:::

## Mode summary

| Mode    | Description                                                                                                                           | Typical use                                   | Requirements                                        |
| ------- | ------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------- | --------------------------------------------------- |
| `local` | Runs assets directly on the Dagster code location process.                                                                            | Laptop development, CI smoke tests.           | No Slurm or SSH connectivity.                       |
| `slurm` | Submits one Slurm job per asset materialization by default. Ray assets can explicitly share one run-owned Slurm allocation if needed. | Staging clusters, production pipelines today. | `SlurmResource` with queue/partition configuration. |

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
- Override `launcher=` on individual assets to run Ray or Spark (WIP) workloads inside the allocation.

## Slurm mode with run-scoped Ray allocation

Use `allocation_scope="run"` when a Dagster run selects several Ray-backed assets that should reuse one Slurm allocation and one Ray cluster. The default remains per-asset jobs.

```python
from dagster_slurm import (
    ComputeResource,
    RayLauncher,
    SlurmAllocationScope,
    SlurmRunAllocationConfig,
)

compute_ray = ComputeResource(
    mode="slurm",
    slurm=slurm,
    default_launcher=RayLauncher(num_gpus_per_node=1),
    allocation_scope=SlurmAllocationScope.RUN,
    run_allocation=SlurmRunAllocationConfig(
        num_nodes=2,
        gpus_per_node=1,
        cpus_per_task=8,
        mem="64G",
        time_limit="04:00:00",
        partition="gpu",
    ),
)
```

- The allocation belongs to the Dagster run, not to one asset.
- `dagster-slurm` starts Ray once, passes `RAY_ADDRESS` to each compatible asset step, and releases the allocation during resource teardown.
- Per-asset Slurm resource overrides must match the run allocation. Incompatible overrides fail before submission instead of silently creating a different scheduling model.
- The example project enables this for the Ray resource with `SLURM_ALLOCATION_SCOPE=run`.

## Executor choice and Dagster restarts

:::tip Use the in-process executor for reliable Slurm runs

When Dagster restarts (deploy, code reload, crash), it sends `SIGTERM` to running processes. With the **in-process executor**, `dagster-slurm` catches the signal and keeps monitoring the Slurm job — the run completes successfully.

With the **multiprocess executor**, the parent process unconditionally fails the run on `SIGTERM` (this is Dagster core behaviour). The Slurm job is preserved, and on re-launch `dagster-slurm` automatically discovers the existing job on the cluster — it **never resubmits**. Configure `run_retries` in `dagster.yaml` so the daemon re-launches failed runs automatically.

See [Troubleshooting: Dagster restarts and Slurm job survival](./troubleshooting.md#dagster-restarts-and-slurm-job-survival) for details.

:::

## Work in progress

Heterogeneous jobs (`slurm-hetjob`) remain experimental. The older `slurm-session` mode is still present for compatibility, but new Ray allocation reuse should use `mode="slurm"` with `allocation_scope="run"`.

## Choosing a launcher per mode

| Mode    | Recommended launcher(s)                              | Notes                                                                                               |
| ------- | ---------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `local` | `BashLauncher`, `RayLauncher` (single node)          | Keeps development parity with production launchers.                                                 |
| `slurm` | `BashLauncher`, `RayLauncher`, `SparkLauncher` (WIP) | Each asset gets a fresh allocation by default; Ray can opt into a shared run allocation explicitly. |

Remember to include launcher-specific dependencies (e.g. `ray`) in your pixi environment or `pyproject.toml`.
