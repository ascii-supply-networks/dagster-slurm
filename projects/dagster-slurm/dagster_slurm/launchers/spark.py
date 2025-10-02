import shlex
from typing import Dict, Optional, List, Any
from .base import ComputeLauncher, ExecutionPlan
from dagster_slurm.config.runtime import RuntimeVariant


class SparkLauncher(ComputeLauncher):
    """
    Apache Spark launcher.
    Modes:
    - Local: Single-node Spark
    - Session: Spark cluster across Slurm allocation
    - Standalone: Submit to existing Spark cluster
    """

    def __init__(
        self,
        *,
        spark_home: str = "/opt/spark",
        master_url: Optional[str] = None,
        executor_memory: str = "4g",
        executor_cores: int = 2,
        driver_memory: str = "2g",
        activate_sh: Optional[str] = None,
    ):
        """
        Args:
            spark_home: Path to Spark installation
            master_url: Connect to existing cluster (e.g., spark://host:7077)
            executor_memory: Memory per executor
            executor_cores: Cores per executor
            driver_memory: Driver memory
            activate_sh: Environment activation script
        """
        self.spark_home = spark_home
        self.master_url = master_url
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        self.driver_memory = driver_memory
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
        """Generate Spark execution plan."""

        messages_path = f"{working_dir}/messages.jsonl"

        script_lines = [
            "#!/bin/bash",
            "set -euo pipefail",
            "",
            f': > "{messages_path}" || true',
            f'echo "[$(date -Is)] Starting Spark execution"',
            f"export SPARK_HOME={shlex.quote(self.spark_home)}",
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

        # Spark setup based on mode
        if self.master_url:
            # Mode: Connect to existing cluster
            script_lines.extend(
                [
                    f'export SPARK_MASTER_URL="{self.master_url}"',
                    f'echo "[$(date -Is)] Using existing Spark cluster: {self.master_url}"',
                    "",
                ]
            )
        elif allocation_context:
            # Mode: Start cluster in Slurm allocation
            script_lines.extend(
                self._generate_cluster_startup(allocation_context, working_dir)
            )
        else:
            # Mode: Local Spark
            script_lines.extend(
                [
                    'export SPARK_MASTER_URL="local[*]"',
                    'echo "[$(date -Is)] Using local Spark"',
                    "",
                ]
            )

        # Submit Spark job
        script_lines.extend(
            [
                'echo "[$(date -Is)] Submitting Spark job..."',
                f"$SPARK_HOME/bin/spark-submit \\",
                f'  --master "$SPARK_MASTER_URL" \\',
                f"  --executor-memory {self.executor_memory} \\",
                f"  --executor-cores {self.executor_cores} \\",
                f"  --driver-memory {self.driver_memory} \\",
                f"  {shlex.quote(payload_path)}",
            ]
        )

        return ExecutionPlan(
            kind=RuntimeVariant.SPARK,
            payload=script_lines,
            environment={**pipes_context, **(extra_env or {})},
            resources={
                "cpus": self.executor_cores,
                "mem": self.executor_memory,
            },
        )

    def _generate_cluster_startup(
        self,
        allocation_context: Dict[str, Any],
        working_dir: str,
    ) -> List[str]:
        """Generate Spark cluster startup for Slurm allocation."""

        nodes = allocation_context.get("nodes", [])
        head_node = allocation_context.get("head_node", nodes[0])
        worker_nodes = nodes[1:] if len(nodes) > 1 else []

        return [
            "# Start Spark cluster across Slurm allocation",
            f'echo "[$(date -Is)] Starting Spark on {len(nodes)} nodes"',
            f'HEAD_NODE="{head_node}"',
            "CURRENT_NODE=$(hostname)",
            "",
            "# Start Spark master on head node",
            'if [ "$CURRENT_NODE" == "$HEAD_NODE" ]; then',
            '  echo "[$(date -Is)] Starting Spark master"',
            "  $SPARK_HOME/sbin/start-master.sh",
            "  sleep 10",
            '  MASTER_URL=$(cat $SPARK_HOME/logs/spark-*-org.apache.spark.deploy.master.Master-1-*.out | grep "Starting Spark master" | grep -oP "spark://[^,]+")',
            f'  echo "$MASTER_URL" > {working_dir}/spark_master_url.txt',
            f'  echo "[$(date -Is)] Spark master started at $MASTER_URL"',
            "fi",
            "",
            "# Start Spark workers",
            "sleep 15",
            f"MASTER_URL=$(cat {working_dir}/spark_master_url.txt)",
            'if [ "$CURRENT_NODE" != "$HEAD_NODE" ]; then',
            '  echo "[$(date -Is)] Starting Spark worker"',
            '  $SPARK_HOME/sbin/start-worker.sh "$MASTER_URL"',
            "fi",
            "",
            'export SPARK_MASTER_URL="$MASTER_URL"',
            'echo "[$(date -Is)] Spark cluster ready"',
            "",
        ]
