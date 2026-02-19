---
sidebar_position: 3
title: Troubleshooting connectivity & logs
---

The `dagster-slurm` presets hide most SSH and Slurm plumbing, but a few deployment-specific quirks are worth calling out. The tips below keep log streaming responsive, explain how to retain run artefacts for inspection, and silence noisy shells in the Docker sandbox.

## Log streaming without ControlMaster

Some HPC centres (e.g. VSC-5) disable SSH ControlMaster sockets. When that happens the Dagster code location automatically falls back to password-aware connections and polls the remote log files. You still get live output, but a few tweaks make the experience smoother:

- Set `DAGSTER_SLURM_SSH_CONTROL_DIR` if your site restricts socket paths. The default is `~/.ssh/dagster-slurm`; point it to a writable directory on shared machines.
- Leave `SLURM_EDGE_NODE_FORCE_TTY=false` unless your cluster explicitly requires a TTY. Password-based sessions request one automatically.
- If you rely on jump hosts or OTP prompts, prefer a local `~/.ssh/config` entry. The same configuration is reused by the Pipes log tailer.

When the connection falls back to polling, Dagster prints a debug line such as:

```
Streaming ... via polling fallback (ControlMaster unavailable)
```

This is expected and no changes are required unless you notice missing output.

## Keep or delete remote run directories

Every successful run now schedules an asynchronous cleanup on the edge node:

```
nohup rm -rf /path/to/dagster_runs/runs/<run_id> >/dev/null 2>&1 &
```

That keeps storage quotas tidy without delaying Dagster shutdown. You can still opt out temporarily:

- Set `debug_mode=True` on the relevant `ComputeResource` to keep run folders around for inspection.
- Manually remove old folders with a periodic cron job if you prefer a deterministic retention policy:

```bash
find /home/$USER/dagster_runs/runs -mindepth 1 -maxdepth 1 -mtime +3 -exec rm -rf {} +
```

The `cleanup_on_failure` flag continues to apply—failed runs respect `debug_mode` and skip removal.

## Laptop and one-off researcher deployments

Not every HPC workflow needs a persistent Dagster server. A common pattern is running Dagster directly on a researcher's laptop or workstation: submit long-running Slurm jobs, close the lid, and check results hours or days later. `dagster-slurm` is designed to support this workflow.

**Why this works without persistent infrastructure:**

- **Slurm is the durable layer.** Once `sbatch` submits a job, the HPC scheduler owns it. The job runs to completion regardless of what happens to the Dagster process that submitted it. Logs, outputs, and exit codes are stored on the cluster.
- **Dagster is the orchestration layer.** It packages the environment, submits the job, and streams logs while it is running. But it does not need to be running for the Slurm job to finish.
- **Reattach on restart.** When you reopen your laptop and start Dagster again, `dagster-slurm` detects the still-running (or already-completed) Slurm job and picks up where it left off. It **never resubmits** — the retry only monitors or collects results from the existing job.

**Recommended setup for laptop deployments:**

```python
from dagster import Definitions, define_asset_job, in_process_executor

# In-process executor keeps runs green through Dagster restarts.
# For laptop use this is almost always the right choice — there is
# no benefit to multiprocess when you are submitting to Slurm anyway.
slurm_job = define_asset_job(
    "slurm_job",
    executor_def=in_process_executor,
)
```

With this setup, you can:

1. **Start Dagster** and materialize assets — jobs are submitted to the cluster.
2. **Stop Dagster** (or close your laptop) — Slurm jobs continue running on the cluster.
3. **Restart Dagster** — if the process was still monitoring a job when it was stopped, the in-process executor catches the signal and the run completes on the next startup. If the process was already gone, a retry reattaches.

There is no need for a persistent Dagster daemon, database server, or always-on VM. The HPC cluster provides the durability; Dagster provides the developer experience (log streaming, metadata, lineage, retries).

:::tip When to deploy persistent Dagster infrastructure

Consider a dedicated Dagster server (e.g. on a VM or Kubernetes) when you need:

- **Scheduled runs** (cron-like triggers via `ScheduleDefinition`)
- **Sensors** that react to external events
- **Team-wide visibility** across multiple users' runs
- **Automated backfill orchestration** across many partitions

For a single researcher iterating on HPC workloads, a laptop deployment is simpler and sufficient.

:::

## Dagster restarts and Slurm job survival

When Dagster restarts (e.g. a deploy, code location reload, or process crash), it sends `SIGTERM` to running processes. How this affects your Slurm jobs depends on the **executor** you are using.

### In-process executor (recommended for Slurm workloads)

With the in-process executor, `dagster-slurm` catches the `SIGTERM`, lets the Slurm job continue running, and keeps monitoring it. **The run completes successfully** — the Slurm job is never cancelled.

This is the recommended configuration when you expect frequent Dagster restarts and want green run results:

```python
from dagster import Definitions, define_asset_job, in_process_executor

slurm_job = define_asset_job(
    "slurm_job",
    executor_def=in_process_executor,
)
```

### Multiprocess executor

:::warning Multiprocess executor always fails runs on SIGTERM

With the multiprocess executor, each asset step runs in a **separate child process**. On SIGTERM, the **parent executor process** unconditionally raises `DagsterExecutionInterruptedError` and marks the run as **FAILURE** — this is Dagster core behaviour that `dagster-slurm` cannot override.

The Slurm job itself is **not cancelled** and continues running on the cluster. However, the Dagster run will show as failed.

:::

To recover automatically, configure **run-level retries** in your `dagster.yaml`. This tells the dagster-daemon to re-launch failed runs — and `dagster-slurm` **never resubmits** the Slurm job. It queries the cluster to find the existing job and either reattaches (if still running) or collects its results (if already completed):

```yaml
# dagster.yaml
run_retries:
  max_retries: 3
  retry_on_asset_or_op_failure: false # only retry on crashes/interrupts
```

On re-launch, `dagster-slurm` looks up the Slurm job submitted by the previous run using run tags. Three outcomes are possible:

1. **Job still running** — reattach and monitor to completion. No new submission.
2. **Job already completed** — collect metrics from `sacct` and mark the run as successful. No new submission.
3. **Job failed or not found** — only then does `dagster-slurm` submit a new job (the previous one genuinely failed).

This means run retries are always safe for HPC workloads: a lengthy training run will never be duplicated, even if Dagster restarts multiple times while the job is in progress.

:::note `run_retries` requires the dagster-daemon

The dagster-daemon process monitors failed runs and re-launches them. For laptop deployments without a daemon, use the **in-process executor** instead (no retry needed — the run survives SIGTERM).

:::

**Summary:**

| Executor     | On SIGTERM              | Run result                     | Slurm job     | Recommendation                                   |
| ------------ | ----------------------- | ------------------------------ | ------------- | ------------------------------------------------ |
| In-process   | Monitoring continues    | Success                        | Keeps running | Use this for laptop / no-daemon deployments      |
| Multiprocess | Run fails (unavoidable) | Failure, then Success on retry | Keeps running | Configure `run_retries` in dagster.yaml + daemon |

For **partitioned backfills** using the multiprocess executor (parallel partitions), each partition independently reattaches to its own Slurm job on retry.
