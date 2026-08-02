---
sidebar_position: 3
title: Slurm job metrics
---

`dagster-slurm` attaches `sacct` metrics to each Dagster materialization:

- standalone mode: metrics for the completed job;
- shared allocation: metrics for the completed `srun` step. The allocation remains active.

## Default metadata

| Key                              | Slurm source                         |
| -------------------------------- | ------------------------------------ |
| `slurm_job_id`                   | Job or allocation ID                 |
| `slurm_step_id`                  | Step ID, for shared allocations      |
| `elapsed_seconds`                | `Elapsed`                            |
| `node_hours`                     | `Elapsed × AllocNodes`               |
| `cpu_efficiency_pct`             | `TotalCPU / (Elapsed × AllocCPUS)`   |
| `max_memory_mb`                  | `MaxRSS`                             |
| `slurm_requested_gpus`           | `ReqTRES`                            |
| `slurm_allocated_gpus`           | `AllocTRES`                          |
| `slurm_gpu_utilization_avg_pct`  | `TRESUsageInAve: gres/gpuutil`       |
| `slurm_gpu_utilization_max_pct`  | `TRESUsageInMax: gres/gpuutil`       |
| `slurm_gpu_memory_max_mb`        | `TRESUsageInMax: gres/gpumem`        |
| `slurm_gpu_utilization_max_node` | `TRESUsageInMaxNode: gres/gpuutil`   |
| `slurm_tres_accounting`          | Raw job and step TRES rows as JSON   |
| `slurm_accounting_available`     | Whether `sacct` returned usable data |
| `slurm_gpu_accounting_available` | Whether GPU usage TRES were present  |

An unavailable metric is omitted. A reported zero remains `0`.

GPU utilization requires `AccountingStorageTRES=gres/gpu` and GPU GRES accounting,
usually `AutoDetect=nvml` for NVIDIA or `AutoDetect=rsmi` for AMD. MIG utilization is
not available through NVML. These values are post-run summaries, not an `nvtop`-style
time series.

All built-in metrics are enabled by default. Select a subset per run:

```python
from dagster_slurm import SlurmMetric

compute.run(
    context=context,
    payload_path="train.py",
    slurm_metrics={
        SlurmMetric.GPU_UTILIZATION_AVG,
        SlurmMetric.GPU_UTILIZATION_MAX,
        SlurmMetric.GPU_MEMORY_MAX,
    },
)
```

Pass `slurm_metrics=[]` to disable optional built-in metrics. Job and step IDs plus
`slurm_accounting_available` are always attached. Custom metrics are unaffected.

## Custom metrics

Pass a function, lambda, or closure to `ComputeResource.run()`:

```python
from dagster_slurm import ComputeResource, SlurmMetricsContext


def collect_energy(metrics: SlurmMetricsContext) -> dict[str, float]:
    target = metrics.step_id or str(metrics.job_id)
    value = metrics.ssh_pool.run(
        f"sacct -j {target} -n -o ConsumedEnergyRaw"
    ).strip()
    return {"site/energy_joules": float(value)}


def training(context, compute: ComputeResource):
    return compute.run(
        context=context,
        payload_path="train.py",
        metrics_collector=collect_energy,
    ).get_results()
```

`SlurmMetricsContext` provides `job_id`, `step_id`, `ssh_pool`, `slurm_resource`,
`session_resource`, and `default_metrics`.

Collector rules:

- keys must be strings;
- values must be valid Dagster metadata values;
- built-in keys cannot be replaced;
- errors are logged and do not fail the asset.

Do not return secrets: metadata is stored in Dagster run history.

## Persistent Ray allocations

For ordinary asset-owned `srun` steps, shared-allocation metrics are per asset.

With a persistent Ray cluster, Slurm accounts GPU work to the long-lived Ray worker
steps, not to each asset driver. Use a custom collector backed by Ray metrics or NVML
when per-asset GPU attribution is required.
