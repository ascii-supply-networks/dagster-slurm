"""Dagster Slurm Integration.

Run Dagster assets on Slurm clusters with support for:

- Local dev mode (no SSH/Slurm)
- Per-asset Slurm submission (staging)
- Run-scoped Slurm Ray allocation (opt-in)
- Session mode with operator fusion (production)
- Multiple launchers (Bash, Ray, Spark—WIP)
"""

# Config classes
from .config.runtime import SlurmRunConfig

# Core resources
from .helpers.metrics import SlurmMetric, SlurmMetricsCallback, SlurmMetricsContext
from .launchers.ray import RayLauncher, RayPortConfig

# Launchers
from .launchers.script import BashLauncher
from .launchers.spark import SparkLauncher
from dagster_slurm.launchers.base import ComputeLauncher

# Clients (for advanced usage)
from .pipes_clients.local_pipes_client import LocalPipesClient
from .pipes_clients.slurm_pipes_client import SlurmPipesClient
from .ray import run_with_ray_reserve_topup, wait_for_stable_ray_resources
from .resources.compute import ComputeResource
from .resources.session import (
    SlurmAllocation,
    SlurmAllocationScope,
    SlurmRunAllocationConfig,
    SlurmSessionResource,
)
from .resources.slurm import SlurmQueueConfig, SlurmResource
from .resources.ssh import SSHConnectionResource
from .sensors import build_slurm_orphan_reconcile_sensor, reconcile_orphaned_slurm_runs

__all__ = [
    # Main facade (most users only need this)
    "ComputeResource",
    # Run-time config (for launchpad configuration)
    "SlurmRunConfig",
    # Configuration resources
    "SlurmResource",
    "SlurmQueueConfig",
    "SSHConnectionResource",
    "SlurmSessionResource",
    "SlurmAllocation",
    "SlurmAllocationScope",
    "SlurmRunAllocationConfig",
    # Launchers
    "BashLauncher",
    "RayLauncher",
    "RayPortConfig",
    "ComputeLauncher",
    "SparkLauncher",  # experimental Spark support
    # Advanced: Direct client access
    "LocalPipesClient",
    "SlurmPipesClient",
    "SlurmMetric",
    "SlurmMetricsCallback",
    "SlurmMetricsContext",
    # Ray workload helpers
    "run_with_ray_reserve_topup",
    "wait_for_stable_ray_resources",
    # Sensors / reconciliation helpers
    "build_slurm_orphan_reconcile_sensor",
    "reconcile_orphaned_slurm_runs",
]
