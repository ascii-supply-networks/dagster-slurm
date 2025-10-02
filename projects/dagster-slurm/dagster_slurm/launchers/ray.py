"""Ray cluster launcher."""

import shlex
from typing import Dict, Optional, List, Any
from .base import ComputeLauncher, ExecutionPlan
from dagster_slurm.config.runtime import RuntimeVariant


class RayLauncher(ComputeLauncher):
    """
    Ray distributed computing launcher.
    Modes:
    - Local: Single-node Ray
    - Session: Multi-node Ray cluster across allocation
    - Connect: Connect to existing cluster
    """

    def __init__(
        self,
        *,
        activate_sh: Optional[str] = None,
        num_gpus_per_node: int = 0,
        ray_address: Optional[str] = None,
        dashboard_port: int = 8265,
        object_store_memory_gb: Optional[int] = None,
    ):
        """
        Args:
            activate_sh: Environment activation script
            num_gpus_per_node: GPUs to allocate per node
            ray_address: Connect to existing cluster (skip startup)
            dashboard_port: Ray dashboard port
            object_store_memory_gb: Object store size (None = auto)
        """
        self.activate_sh = activate_sh
        self.num_gpus_per_node = num_gpus_per_node
        self.ray_address = ray_address
        self.dashboard_port = dashboard_port
        self.object_store_memory = object_store_memory_gb

    def prepare_execution(
        self,
        payload_path: str,
        python_executable: str,
        working_dir: str,
        pipes_context: Dict[str, str],
        extra_env: Optional[Dict[str, str]] = None,
        allocation_context: Optional[Dict[str, Any]] = None,
    ) -> ExecutionPlan:
        """Generate Ray execution plan."""

        messages_path = f"{working_dir}/messages.jsonl"

        script_lines = [
            "#!/bin/bash",
            "set -euo pipefail",
            "",
            f': > "{messages_path}" || true',
            f'echo "[$(date -Is)] Starting Ray execution"',
            "",
        ]

        # Dagster Pipes context
        for key, value in pipes_context.items():
            script_lines.append(f"export {key}={shlex.quote(value)}")
        script_lines.append("")

        # Extra environment
        if extra_env:
            for key, value in extra_env.items():
                script_lines.append(f"export {key}={shlex.quote(str(value))}")
            script_lines.append("")

        # Environment activation
        if self.activate_sh:
            script_lines.extend(
                [
                    f"if [ -f {shlex.quote(self.activate_sh)} ]; then",
                    f"  source {shlex.quote(self.activate_sh)}",
                    "fi",
                    "",
                ]
            )

        # Ray setup based on mode
        if self.ray_address:
            # Mode: Connect to existing cluster
            script_lines.extend(
                [
                    f'export RAY_ADDRESS="{self.ray_address}"',
                    f'echo "[$(date -Is)] Connecting to Ray cluster: {self.ray_address}"',
                    "",
                ]
            )
        elif allocation_context:
            # Mode: Start cluster in Slurm allocation
            script_lines.extend(
                self._generate_cluster_startup(allocation_context, working_dir)
            )
        else:
            # Mode: Start local single-node Ray
            script_lines.extend(self._generate_local_startup())

        # Execute payload
        script_lines.extend(
            [
                f'echo "[$(date -Is)] Executing Ray payload..."',
                f"exec {shlex.quote(python_executable)} {shlex.quote(payload_path)}",
            ]
        )

        return ExecutionPlan(
            kind=RuntimeVariant.RAY,
            payload=script_lines,
            environment={**pipes_context, **(extra_env or {})},
            resources={
                "cpus": allocation_context.get("num_nodes", 1)
                if allocation_context
                else 1,
                "gpus": self.num_gpus_per_node,
            },
        )

    def _generate_local_startup(self) -> List[str]:
        """Generate Ray startup for local mode."""
        obj_store = (
            f"--object-store-memory={self.object_store_memory}000000000"
            if self.object_store_memory
            else ""
        )

        return [
            'echo "[$(date -Is)] Starting local Ray cluster"',
            f"ray start --head --port=6379 \\",
            f"  --dashboard-host=127.0.0.1 --dashboard-port={self.dashboard_port} \\",
            f"  --num-gpus={self.num_gpus_per_node} {obj_store} --block &",
            "sleep 5",
            'export RAY_ADDRESS="auto"',
            'echo "[$(date -Is)] Ray ready (local mode)"',
            "",
        ]

    def _generate_cluster_startup(
        self,
        allocation_context: Dict[str, Any],
        working_dir: str,
    ) -> List[str]:
        """Generate Ray cluster startup for Slurm allocation."""

        nodes = allocation_context.get("nodes", [])
        head_node = allocation_context.get(
            "head_node", nodes[0] if nodes else "localhost"
        )
        num_nodes = len(nodes)

        obj_store = (
            f"--object-store-memory={self.object_store_memory}000000000"
            if self.object_store_memory
            else ""
        )

        return [
            "# Start Ray cluster across Slurm allocation",
            f'echo "[$(date -Is)] Starting Ray on {num_nodes} nodes"',
            f'HEAD_NODE="{head_node}"',
            "CURRENT_NODE=$(hostname)",
            "",
            "# Determine node role",
            'if [ "$CURRENT_NODE" == "$HEAD_NODE" ]; then',
            '  ROLE="head"',
            "else",
            '  ROLE="worker"',
            "fi",
            "",
            "# Start Ray head node",
            'if [ "$ROLE" == "head" ]; then',
            '  echo "[$(date -Is)] Starting Ray head node"',
            "  ray start --head --port=6379 \\",
            f"    --dashboard-host=0.0.0.0 --dashboard-port={self.dashboard_port} \\",
            f"    --num-gpus={self.num_gpus_per_node} {obj_store} --block &",
            "  sleep 10",
            "  RAY_HEAD_IP=$(hostname -I | awk '{print $1}')",
            '  RAY_ADDRESS="$RAY_HEAD_IP:6379"',
            f'  echo "$RAY_ADDRESS" > {working_dir}/ray_address.txt',
            f'  echo "[$(date -Is)] Ray head started at $RAY_ADDRESS"',
            "fi",
            "",
            "# Start Ray worker nodes",
            'if [ "$ROLE" == "worker" ]; then',
            '  echo "[$(date -Is)] Starting Ray worker node"',
            "  sleep 15  # Wait for head to be ready",
            f"  RAY_ADDRESS=$(cat {working_dir}/ray_address.txt)",
            '  ray start --address="$RAY_ADDRESS" \\',
            f"    --num-gpus={self.num_gpus_per_node} {obj_store} --block &",
            "  sleep 5",
            f'  echo "[$(date -Is)] Ray worker connected to $RAY_ADDRESS"',
            "fi",
            "",
            "# Wait for cluster to be ready",
            "sleep 5",
            'export RAY_ADDRESS="auto"',
            'echo "[$(date -Is)] Ray cluster ready"',
            "",
        ]
