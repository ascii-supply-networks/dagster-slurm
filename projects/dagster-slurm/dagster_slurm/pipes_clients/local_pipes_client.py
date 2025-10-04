"""Local Pipes client for dev mode."""

import os
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

from dagster import (
    AssetExecutionContext,
    PipesClient,
    PipesEnvContextInjector,
    get_dagster_logger,
    open_pipes_session,
)

from ..helpers.message_readers import LocalMessageReader
from ..launchers.base import ComputeLauncher
from ..runners.local_runner import LocalRunner


class LocalPipesClient(PipesClient):
    """Pipes client for local execution (dev mode).
    No SSH, no Slurm - just runs scripts locally via subprocess.
    """

    def __init__(
        self,
        launcher: ComputeLauncher,
        base_dir: str = "/tmp/dagster_local_runs",
        require_pixi: bool = True,
    ):
        """Args:
        launcher: Launcher to generate execution plans
        base_dir: Base directory for run artifacts
        require_pixi: Require active pixi environment.

        """
        super().__init__()
        self.launcher = launcher
        self.base_dir = base_dir
        self.require_pixi = require_pixi
        self.runner = LocalRunner()
        self.logger = get_dagster_logger()

        if require_pixi and not self._check_pixi_active():
            raise RuntimeError(
                "Local mode requires active pixi environment. Run: pixi shell -e dev"
            )

    def _check_pixi_active(self) -> bool:
        """Verify running in pixi environment."""
        return (
            "PIXI_ENVIRONMENT_NAME" in os.environ or "PIXI_PROJECT_ROOT" in os.environ
        )

    def run(  # type: ignore[override]
        self,
        context: AssetExecutionContext,
        *,
        payload_path: str,
        python_executable: Optional[str] = None,
        extra_env: Optional[Dict[str, str]] = None,
        extras: Optional[Dict[str, Any]] = None,
        extra_slurm_opts: Optional[Dict[str, Any]] = None,
    ) -> Iterator:
        """Execute payload locally.

        Args:
            context: Dagster execution context
            payload_path: Path to Python script to execute
            python_executable: Python interpreter (defaults to current)
            extra_env: Additional environment variables
            extras: Extra data to pass via Pipes

        Yields:
            Dagster events (materializations, logs, etc.)

        """
        # Use current Python by default
        if python_executable is None:
            python_executable = sys.executable

        run_id = context.run_id or uuid.uuid4().hex
        working_dir = f"{self.base_dir}/{run_id}"
        messages_path = f"{working_dir}/messages.jsonl"

        # Create working directory
        Path(working_dir).mkdir(parents=True, exist_ok=True)

        # Setup Pipes communication
        context_injector = PipesEnvContextInjector()
        message_reader = LocalMessageReader(
            messages_path=messages_path,
            include_stdio=True,
        )

        with open_pipes_session(
            context=context,
            context_injector=context_injector,
            message_reader=message_reader,
            extras=extras,
        ) as session:
            # Get Pipes environment
            pipes_env = session.get_bootstrap_env_vars()

            # Generate execution plan
            execution_plan = self.launcher.prepare_execution(
                payload_path=payload_path,
                python_executable=python_executable,
                working_dir=working_dir,
                pipes_context=pipes_env,  # type: ignore
                extra_env=extra_env,
                allocation_context=None,  # No allocation in local mode
            )

            # Execute locally
            self.logger.info(f"Executing locally: {payload_path}")

            try:
                self.runner.execute_script(
                    script_lines=execution_plan.payload,
                    working_dir=working_dir,
                    wait=True,
                )
            except Exception as e:
                self.logger.error(f"Local execution failed: {e}")
                raise

            # Yield results
            for event in session.get_results():
                yield event
