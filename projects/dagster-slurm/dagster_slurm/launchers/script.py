import shlex
from typing import Dict, Optional, List, Any
from .base import ComputeLauncher, ExecutionPlan
from dagster_slurm.config.runtime import RuntimeVariant


class BashLauncher(ComputeLauncher):
    """
    Executes Python scripts via bash.
    Simple, reliable, works everywhere.
    """

    def __init__(self, activate_sh: Optional[str] = None):
        """
        Args:
            activate_sh: Global activation script (fallback)
        """
        self.activate_sh = activate_sh

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

        script_lines = [
            "#!/bin/bash",
            "set -euo pipefail",
            "",
            "# Ensure messages file exists",
            f': > "{messages_path}" || true',
            "",
            f'echo "[$(date -Is)] ========================================="',
            f'echo "[$(date -Is)] Dagster Asset Execution"',
            f'echo "[$(date -Is)] Working dir: {working_dir}"',
            f'echo "[$(date -Is)] Payload: {payload_path}"',
            f'echo "[$(date -Is)] Python: {python_executable}"',
            f'echo "[$(date -Is)] ========================================="',
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

        # Environment activation
        script_lines.extend(
            [
                "# Activate Python environment",
                f"if [ -f {shlex.quote(working_dir)}/environment.sh ]; then",
                '  echo "[$(date -Is)] Installing per-run environment..."',
                f"  chmod +x {shlex.quote(working_dir)}/environment.sh",
                f"  cd {shlex.quote(working_dir)} && ./environment.sh",
                '  echo "[$(date -Is)] Environment installed"',
                "fi",
                "",
                f"if [ -f {shlex.quote(working_dir)}/activate.sh ]; then",
                '  echo "[$(date -Is)] Activating per-run environment"',
                f"  source {shlex.quote(working_dir)}/activate.sh",
            ]
        )

        if self.activate_sh:
            script_lines.extend(
                [
                    f"elif [ -f {shlex.quote(self.activate_sh)} ]; then",
                    '  echo "[$(date -Is)] Activating global environment"',
                    f"  source {shlex.quote(self.activate_sh)}",
                ]
            )

        script_lines.extend(
            [
                "else",
                '  echo "[$(date -Is)] WARN: No environment activation" >&2',
                "fi",
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
                    f'echo "[$(date -Is)] Running in allocation with {allocation_context.get("num_nodes", 0)} nodes"',
                    "",
                ]
            )

        # Execute payload
        script_lines.extend(
            [
                'echo "[$(date -Is)] Launching payload..."',
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
