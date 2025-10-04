import shlex
from pathlib import Path
from typing import Any, Dict, Optional

import dagster as dg

from dagster_slurm.config.runtime import RuntimeVariant

from .base import ComputeLauncher, ExecutionPlan


class BashLauncher(ComputeLauncher):
    """Executes Python scripts via bash.

    Uses the self-contained pixi environment extracted at runtime.
    Sources the activation script provided by pixi-pack.
    """

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
        """Generate bash execution plan.

        Args:
            payload_path: Path to Python script on remote host
            python_executable: Python from extracted environment
            working_dir: Working directory
            pipes_context: Dagster Pipes environment variables
            extra_env: Additional environment variables
            allocation_context: Slurm allocation info (for session mode)
            activation_script: Path to activation script (provided by pixi-pack)

        Returns:
            ExecutionPlan with shell script

        """
        messages_path = f"{working_dir}/messages.jsonl"
        date_fmt = "date +%Y-%m-%dT%H:%M:%S%z"

        # Quote paths for safety
        python_quoted = shlex.quote(python_executable)
        payload_quoted = shlex.quote(payload_path)

        # Build header
        script = f"""#!/bin/bash
set -euo pipefail

# Ensure messages file exists
: > "{messages_path}" || true

echo "[$({date_fmt})] ========================================="
echo "[$({date_fmt})] Dagster Asset Execution"
echo "[$({date_fmt})] Working dir: {working_dir}"
echo "[$({date_fmt})] Payload: {payload_path}"
echo "[$({date_fmt})] Python: {python_executable}"
echo "[$({date_fmt})] ========================================="

"""

        # Activate environment using provided activation script
        if activation_script:
            activation_quoted = shlex.quote(activation_script)
            script += f"""# Activate pixi-packed environment
echo "[$({date_fmt})] Activating environment from: {activation_script}"
source {activation_quoted}
echo "[$({date_fmt})] Environment activated"
echo "[$({date_fmt})] Python version: $(python --version 2>&1)"
echo "[$({date_fmt})] Which python: $(which python)"

"""
        else:
            # Fallback - shouldn't happen but be defensive
            logger = dg.get_dagster_logger()
            logger.warning("No activation script provided, using PATH fallback")
            env_dir = str(Path(python_executable).parent.parent)
            env_dir_quoted = shlex.quote(env_dir)
            script += f"""# WARNING: No activation script provided
export PATH={env_dir_quoted}/bin:$PATH
export LD_LIBRARY_PATH={env_dir_quoted}/lib:${{LD_LIBRARY_PATH:-}}

"""

        # Add Dagster Pipes environment
        script += "# Dagster Pipes environment\n"
        for key, value in pipes_context.items():
            script += f"export {key}={shlex.quote(value)}\n"
        script += "\n"

        # Add extra environment variables
        if extra_env:
            script += "# Additional environment variables\n"
            for key, value in extra_env.items():
                script += f"export {key}={shlex.quote(str(value))}\n"
            script += "\n"

        # Add allocation context
        if allocation_context:
            nodes = ",".join(allocation_context.get("nodes", []))
            num_nodes = allocation_context.get("num_nodes", 0)
            head_node = allocation_context.get("head_node", "")

            script += f"""# Slurm allocation context
export SLURM_ALLOCATION_NODES="{nodes}"
export SLURM_ALLOCATION_NUM_NODES="{num_nodes}"
export SLURM_ALLOCATION_HEAD_NODE="{head_node}"
echo "[$({date_fmt})] Running in allocation with {num_nodes} nodes"

"""

        # Add Python verification and execution
        script += f"""# Verify Python executable
if [ ! -f {python_quoted} ]; then
  echo "[$({date_fmt})] ERROR: Python executable not found: {python_executable}"
  exit 1
fi

# Verify Python works
if ! {python_quoted} --version; then
  echo "[$({date_fmt})] ERROR: Python executable failed to run"
  exit 1
fi

# Launch payload
echo "[$({date_fmt})] Launching payload..."
echo ""
exec {python_quoted} {payload_quoted}
"""

        # Split into lines for ExecutionPlan
        script_lines = script.split("\n")

        return ExecutionPlan(
            kind=RuntimeVariant.SHELL,
            payload=script_lines,
            environment=pipes_context,
            resources={},
        )
