---
sidebar_position: 1
title: Choose an execution mode
---

`ComputeResource` adapts Dagster assets to different deployment targets. Picking the right execution mode keeps developer feedback loops tight while fitting production constraints.

:::info No persistent server required

`dagster-slurm` works well as a **laptop-first tool**. Submit long-running HPC jobs, close your laptop, and check results later — Slurm jobs run to completion independently of the Dagster process. A retry reattaches to any job that was still running when Dagster stopped. See [Laptop and one-off researcher deployments](./troubleshooting.md#laptop-and-one-off-researcher-deployments) for recommended setup.

:::

## Mode summary

| Mode    | Description                                                | Typical use                                   | Requirements                                        |
| ------- | ---------------------------------------------------------- | --------------------------------------------- | --------------------------------------------------- |
| `local` | Runs assets directly on the Dagster code location process. | Laptop development, CI smoke tests.           | No Slurm or SSH connectivity.                       |
| `slurm` | Submits one Slurm job per asset materialization.           | Staging clusters, production pipelines today. | `SlurmResource` with queue/partition configuration. |

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

## Executor choice and Dagster restarts

:::tip Use the in-process executor for reliable Slurm runs

When Dagster restarts (deploy, code reload, crash), it sends `SIGTERM` to running processes. With the **in-process executor**, `dagster-slurm` catches the signal and keeps monitoring the Slurm job — the run completes successfully.

With the **multiprocess executor**, the parent process unconditionally fails the run on `SIGTERM` (this is Dagster core behaviour). The Slurm job is preserved, and on re-launch `dagster-slurm` automatically discovers the existing job on the cluster — it **never resubmits**. Configure `run_retries` in `dagster.yaml` so the daemon re-launches failed runs automatically.

See [Troubleshooting: Dagster restarts and Slurm job survival](./troubleshooting.md#dagster-restarts-and-slurm-job-survival) for details.

:::

## Work in progress

Session reuse (`slurm-session`) and heterogeneous jobs (`slurm-hetjob`) are on the roadmap. Configuration stubs remain in the API so early adopters can experiment, but we recommend planning production systems around the stable `local` and `slurm` modes for now.

## Choosing a launcher per mode

| Mode    | Recommended launcher(s)                              | Notes                                                                                          |
| ------- | ---------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `local` | `BashLauncher`, `RayLauncher` (single node)          | Keeps development parity with production launchers.                                            |
| `slurm` | `BashLauncher`, `RayLauncher`, `SparkLauncher` (WIP) | Each asset gets a fresh allocation. Session-based reuse will extend this list once stabilised. |

Remember to include launcher-specific dependencies (e.g. `ray`) in your pixi environment or `pyproject.toml`.
