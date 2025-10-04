from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from dagster import ConfigurableResource

from ..config.runtime import RuntimeVariant


@dataclass
class ExecutionPlan:
    """Plan for executing a workload."""

    kind: RuntimeVariant  # "shell_script", "ray", "spark"
    payload: list[str]  # Shell script lines
    environment: Dict[str, str]  # Environment variables
    resources: Dict[str, Any]  # Resource requirements
    auxiliary_scripts: Dict[str, str] = field(default_factory=dict)


class ComputeLauncher(ConfigurableResource):
    """Base class for compute launchers."""

    def prepare_execution(
        self,
        payload_path: str,
        python_executable: str,
        working_dir: str,
        pipes_context: Dict[str, str],
        extra_env: Optional[Dict[str, str]] = None,
        allocation_context: Optional[Dict[str, Any]] = None,
        activation_script: Optional[str] = None,
    ) -> ExecutionPlan:
        """Prepare execution plan.

        Args:
            payload_path: Path to Python script on remote
            python_executable: Python interpreter path
            working_dir: Working directory
            pipes_context: Dagster Pipes environment
            extra_env: Additional environment variables
            allocation_context: Slurm allocation info (for session mode)
            activation_script: Environment activation script

        Returns:
            ExecutionPlan with script and metadata

        """
        raise NotImplementedError
