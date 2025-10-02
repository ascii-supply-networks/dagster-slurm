import shlex
from typing import Dict, Optional, List, Any
from .base import ComputeLauncher, ExecutionPlan
from dagster_slurm.config.runtime import RuntimeVariant
from pydantic import Field


class BashLauncher(ComputeLauncher):
    """Executes Python scripts via bash."""

    activate_sh: Optional[str] = Field(
        default=None, description="Global activation script (fallback)"
    )

    def prepare_execution(
        self,
        payload_path: str,
        python_executable: str,
        working_dir: str,
        pipes_context: Dict[str, str],
        extra_env: Optional[Dict[str, str]] = None,
        allocation_context: Optional[Dict[str, Any]] = None,
    ) -> ExecutionPlan:
        """Generate bash execution plan."""

        messages_path = f"{working_dir}/messages.jsonl"
        date_fmt = "date +%Y-%m-%dT%H:%M:%S%z"

        script_lines = [
            "#!/bin/bash",
            "set -euo pipefail",
            "",
            "# Ensure messages file exists",
            f': > "{messages_path}" || true',
            "",
            f'echo "[$({date_fmt})] ========================================="',
            f'echo "[$({date_fmt})] Dagster Asset Execution"',
            f'echo "[$({date_fmt})] Working dir: {working_dir}"',
            f'echo "[$({date_fmt})] Payload: {payload_path}"',
            f'echo "[$({date_fmt})] Python: {python_executable}"',
            f'echo "[$({date_fmt})] ========================================="',
            "",
        ]

        # Dagster Pipes context
        script_lines.append("# Dagster Pipes environment")
        for key, value in pipes_context.items():
            script_lines.append(f"export {key}={shlex.quote(value)}")
        script_lines.append("")

        # Extra environment
        if extra_env:
            script_lines.append("# Additional environment variables")
            for key, value in extra_env.items():
                script_lines.append(f"export {key}={shlex.quote(str(value))}")
            script_lines.append("")

        # ✅ FIXED: Environment activation with correct if/elif/else/fi structure
        script_lines.extend(
            [
                "# Install per-run environment if present",
                f"if [ -f {shlex.quote(working_dir)}/environment.sh ]; then",
                f'  echo "[$({date_fmt})] Installing per-run environment..."',
                f"  chmod +x {shlex.quote(working_dir)}/environment.sh",
                f"  cd {shlex.quote(working_dir)} && ./environment.sh",
                f'  echo "[$({date_fmt})] Environment installed"',
                "fi",
                "",
                "# Activate environment (per-run or global)",
                f"if [ -f {shlex.quote(working_dir)}/activate.sh ]; then",
                f'  echo "[$({date_fmt})] Activating per-run environment"',
                f"  source {shlex.quote(working_dir)}/activate.sh",
            ]
        )

        # ✅ Add elif/else ONLY if activate_sh is provided
        if self.activate_sh:
            script_lines.extend(
                [
                    f"elif [ -f {shlex.quote(self.activate_sh)} ]; then",
                    f'  echo "[$({date_fmt})] Activating global environment"',
                    f"  source {shlex.quote(self.activate_sh)}",
                ]
            )

        # ✅ Close the if statement properly
        script_lines.extend(
            [
                "fi",  # ✅ This closes the "if [ -f .../activate.sh ]" statement
                "",
            ]
        )

        # Session allocation context
        if allocation_context:
            script_lines.extend(
                [
                    "# Slurm allocation context",
                    f'export SLURM_ALLOCATION_NODES="{",".join(allocation_context.get("nodes", []))}"',
                    f'export SLURM_ALLOCATION_NUM_NODES="{allocation_context.get("num_nodes", 0)}"',
                    f'export SLURM_ALLOCATION_HEAD_NODE="{allocation_context.get("head_node", "")}"',
                    f'echo "[$({date_fmt})] Running in allocation with {allocation_context.get("num_nodes", 0)} nodes"',
                    "",
                ]
            )

        # Execute payload
        script_lines.extend(
            [
                f'echo "[$({date_fmt})] Launching payload..."',
                'echo ""',
                f"exec {shlex.quote(python_executable)} {shlex.quote(payload_path)}",
            ]
        )

        return ExecutionPlan(
            kind=RuntimeVariant.SHELL,
            payload=script_lines,
            environment=pipes_context,
            resources={"cpus": 1, "mem": "1G"},
        )
