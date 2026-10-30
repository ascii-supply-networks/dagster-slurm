"""Dagster Slurm Integration.

Run Dagster assets on Slurm clusters with support for:

- Local dev mode (no SSH/Slurm)
- Per-asset Slurm submission (staging)
- Run-scoped Slurm Ray allocation (opt-in)
- Session mode with operator fusion (production)
- Multiple launchers (Bash, Ray, Spark—WIP)
"""

from dagster_slurm.launchers.base import ComputeLauncher

from .config.runtime import SlurmRunConfig
from .helpers.metrics import SlurmMetric, SlurmMetricsCallback, SlurmMetricsContext
from .launchers.ray import RayLauncher, RayPortConfig
from .launchers.script import BashLauncher
from .launchers.spark import SparkLauncher
from .pipes_clients.local_pipes_client import LocalPipesClient
from .pipes_clients.slurm_pipes_client import SlurmPipesClient
from .ray import run_with_ray_reserve_topup, wait_for_stable_ray_resources
from .resources.compute import ComputeResource
from .resources.session import (
    SlurmAllocation,
    SlurmAllocationEnded,
    SlurmAllocationScope,
    SlurmRunAllocationConfig,
    SlurmSessionResource,
    SlurmStepDrained,
    SlurmStepExecutionResult,
    SlurmWorkerAllocationConfig,
)
from .resources.slurm import SlurmQueueConfig, SlurmResource
from .resources.ssh import SSHConnectionResource
from .scaling import (
    AllocationSnapshot,
    ReconcileResult,
    SbatchTestOnlyEstimator,
    SessionSnapshot,
    StartEstimate,
    StartEstimator,
    WorkerIntent,
)
from .sensors import build_slurm_orphan_reconcile_sensor, reconcile_orphaned_slurm_runs

__all__ = [
    "AllocationSnapshot",
    "BashLauncher",
    "ComputeLauncher",
    "ComputeResource",
    "LocalPipesClient",
    "RayLauncher",
    "RayPortConfig",
    "ReconcileResult",
    "SSHConnectionResource",
    "SbatchTestOnlyEstimator",
    "SessionSnapshot",
    "SlurmAllocation",
    "SlurmAllocationEnded",
    "SlurmAllocationScope",
    "SlurmMetric",
    "SlurmMetricsCallback",
    "SlurmMetricsContext",
    "SlurmPipesClient",
    "SlurmQueueConfig",
    "SlurmResource",
    "SlurmRunAllocationConfig",
    "SlurmRunConfig",
    "SlurmSessionResource",
    "SlurmStepDrained",
    "SlurmStepExecutionResult",
    "SlurmWorkerAllocationConfig",
    "SparkLauncher",  # experimental Spark support
    "StartEstimate",
    "StartEstimator",
    "WorkerIntent",
    "build_slurm_orphan_reconcile_sensor",
    "reconcile_orphaned_slurm_runs",
    "run_with_ray_reserve_topup",
    "wait_for_stable_ray_resources",
]
