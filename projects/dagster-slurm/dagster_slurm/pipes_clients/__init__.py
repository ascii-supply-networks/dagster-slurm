"""Dagster Pipes clients."""

from .local_pipes_client import LocalPipesClient
from .slurm_pipes_client import SlurmPipesClient

__all__ = [
    "LocalPipesClient",
    "SlurmPipesClient",
]
