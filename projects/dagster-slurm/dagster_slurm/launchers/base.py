"""Abstract launcher interface."""

from abc import ABC, abstractmethod
from typing import Dict, Optional, List, Any, Literal
from dataclasses import dataclass
from dagster_slurm.config.runtime import RuntimeVariant
import dagster as dg


@dataclass
class ExecutionPlan:
    """Unified execution plan - supports multiple backend types."""

    kind: RuntimeVariant
    payload: Any  # script lines, Ray job, etc.
    environment: Dict[str, str]
    resources: Dict[str, Any]


class ComputeLauncher(dg.ConfigurableResource):
    """Generate execution plans for workloads."""

    @abstractmethod
    def prepare_execution(
        self,
        payload_path: str,
        working_dir: str,
        pipes_context: Dict[str, str],
        extra_env: Optional[Dict[str, str]] = None,
        allocation_context: Optional[Dict[str, Any]] = None,
    ) -> ExecutionPlan:
        """
        Prepare execution plan.

        Args:
            payload_path: Path to Python script
            working_dir: Execution directory (contains messages.jsonl)
            pipes_context: Dagster Pipes environment variables
            extra_env: Additional environment variables
            allocation_context: Optional session allocation info

        Returns:
            ExecutionPlan describing how to execute
        """
        pass
