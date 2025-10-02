"""
Dagster Slurm Integration.

Run Dagster assets on Slurm clusters with support for:
- Local dev mode (no SSH/Slurm)
- Per-asset Slurm submission (staging)
- Session mode with operator fusion (production)
- Multiple launchers (Bash, Ray, Spark)
"""

# Core resources
from .resources.compute import ComputeResource
from .resources.slurm import SlurmResource, SlurmQueueConfig
from .resources.ssh import SSHConnectionResource
from .resources.session import SlurmSessionResource, SlurmAllocation


# Launchers
from .launchers.script import BashLauncher
from .launchers.ray import RayLauncher
from .launchers.spark import SparkLauncher

# Clients (for advanced usage)
from .pipes_clients.local_pipes_client import LocalPipesClient
from .pipes_clients.slurm_pipes_client import SlurmPipesClient

__all__ = [
    # Main facade (most users only need this)
    "ComputeResource",
    # Configuration resources
    "SlurmResource",
    "SlurmQueueConfig",
    "SSHConnectionResource",
    "SlurmSessionResource",
    "SlurmAllocation",
    # Launchers
    "BashLauncher",
    "RayLauncher",
    "SparkLauncher",
    # Advanced: Direct client access
    "LocalPipesClient",
    "SlurmPipesClient",
]
