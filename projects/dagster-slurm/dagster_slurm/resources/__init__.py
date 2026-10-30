"""Dagster resources for Slurm integration."""

from .session import (
    SlurmAllocation,
    SlurmAllocationEnded,
    SlurmAllocationScope,
    SlurmRunAllocationConfig,
    SlurmSessionResource,
    SlurmStepDrained,
    SlurmStepExecutionResult,
    SlurmWorkerAllocationConfig,
)
from .slurm import SlurmQueueConfig, SlurmResource
from .ssh import SSHConnectionResource

__all__ = [
    "SSHConnectionResource",
    "SlurmAllocation",
    "SlurmAllocationEnded",
    "SlurmAllocationScope",
    "SlurmQueueConfig",
    "SlurmResource",
    "SlurmRunAllocationConfig",
    "SlurmSessionResource",
    "SlurmStepDrained",
    "SlurmStepExecutionResult",
    "SlurmWorkerAllocationConfig",
]
