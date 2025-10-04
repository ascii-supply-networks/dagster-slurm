"""Apache Spark launcher."""

import shlex
from typing import Any, Dict, Optional

from pydantic import Field

from dagster_slurm.config.runtime import RuntimeVariant

from .base import ComputeLauncher, ExecutionPlan


class SparkLauncher(ComputeLauncher):
    """Apache Spark launcher.

    Modes:
    - Local: Single-node Spark (no allocation_context)
    - Cluster: Spark cluster across Slurm allocation (via allocation_context)
    - Standalone: Connect to existing Spark cluster (via master_url)
    """

    spark_home: str = Field(
        default="/opt/spark", description="Path to Spark installation"
    )
    master_url: Optional[str] = Field(
        default=None,
        description="Connect to existing cluster (e.g., spark://host:7077)",
    )
    executor_memory: str = Field(default="4g", description="Memory per executor")
    executor_cores: int = Field(default=2, description="Cores per executor")
    driver_memory: str = Field(default="2g", description="Driver memory")
    num_executors: Optional[int] = Field(
        default=None, description="Number of executors (None = auto from allocation)"
    )

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
        """Generate Spark execution plan."""
        messages_path = f"{working_dir}/messages.jsonl"
        date_fmt = "date +%Y-%m-%dT%H:%M:%S%z"

        shlex.quote(python_executable)
        payload_quoted = shlex.quote(payload_path)
        spark_home_quoted = shlex.quote(self.spark_home)

        # Build header
        script = f"""#!/bin/bash
set -euo pipefail

: > "{messages_path}" || true
echo "[$({date_fmt})] ========================================="
echo "[$({date_fmt})] Spark Workload Execution"
echo "[$({date_fmt})] Working dir: {working_dir}"
echo "[$({date_fmt})] ========================================="

export SPARK_HOME={spark_home_quoted}
export PATH=$SPARK_HOME/bin:$PATH

"""

        # Environment activation
        if activation_script:
            activation_quoted = shlex.quote(activation_script)
            script += f"""# Activate environment
echo "[$({date_fmt})] Activating environment..."
source {activation_quoted}
echo "[$({date_fmt})] Environment activated"

"""

        # Dagster Pipes environment
        script += "# Dagster Pipes environment\n"
        for key, value in pipes_context.items():
            script += f"export {key}={shlex.quote(value)}\n"
        script += "\n"

        # Extra environment
        if extra_env:
            script += "# Additional environment variables\n"
            for key, value in extra_env.items():
                script += f"export {key}={shlex.quote(str(value))}\n"
            script += "\n"

        # Spark setup based on mode
        if self.master_url:
            # Mode: Connect to existing cluster
            master_quoted = shlex.quote(self.master_url)
            script += f"""# Connect to existing Spark cluster
export SPARK_MASTER_URL={master_quoted}
echo "[$({date_fmt})] Using existing Spark cluster: {self.master_url}"

"""
        elif allocation_context:
            # Mode: Start cluster in Slurm allocation
            script += self._generate_cluster_startup(
                allocation_context, working_dir, date_fmt
            )
        else:
            # Mode: Local Spark
            script += f"""# Use local Spark
export SPARK_MASTER_URL="local[*]"
echo "[$({date_fmt})] Using local Spark"

"""

        # Submit Spark job
        num_exec = (
            self.num_executors or (allocation_context.get("num_nodes", 1) - 1)
            if allocation_context
            else 1
        )

        script += f"""# Submit Spark job
echo "[$({date_fmt})] Submitting Spark job..."
$SPARK_HOME/bin/spark-submit \\
  --master "$SPARK_MASTER_URL" \\
  --executor-memory {self.executor_memory} \\
  --executor-cores {self.executor_cores} \\
  --driver-memory {self.driver_memory} \\
  --num-executors {num_exec} \\
  {payload_quoted}

exit_code=$?
echo "[$({date_fmt})] Spark job finished with exit code $exit_code"

# Cleanup Spark cluster if we started it
if [ -f {working_dir}/spark_cleanup.sh ]; then
  echo "[$({date_fmt})] Cleaning up Spark cluster..."
  bash {working_dir}/spark_cleanup.sh
fi

exit $exit_code
"""

        return ExecutionPlan(
            kind=RuntimeVariant.SPARK,
            payload=script.split("\n"),
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
        date_fmt: str,
    ) -> str:
        """Generate Spark cluster startup for Slurm allocation."""
        nodes = allocation_context.get("nodes", [])
        head_node = allocation_context.get(
            "head_node", nodes[0] if nodes else "localhost"
        )

        return f"""# Start Spark cluster across Slurm allocation
echo "[$({date_fmt})] Starting Spark on {len(nodes)} nodes"
HEAD_NODE="{head_node}"

# Start Spark master on head node
echo "[$({date_fmt})] Starting Spark master on $HEAD_NODE"
srun --nodes=1 --ntasks=1 -w $HEAD_NODE \\
  bash -c '$SPARK_HOME/sbin/start-master.sh' &

sleep 10

# Get master URL
MASTER_URL=$(srun --nodes=1 --ntasks=1 -w $HEAD_NODE \\
  bash -c 'cat $SPARK_HOME/logs/spark-*-org.apache.spark.deploy.master.Master-*.out | grep -oP "spark://[^,]+" | head -1')

echo "$MASTER_URL" > {working_dir}/spark_master_url.txt
echo "[$({date_fmt})] Spark master started at $MASTER_URL"

# Start Spark workers on all nodes
echo "[$({date_fmt})] Starting Spark workers..."
for node in {" ".join(nodes)}; do
  echo "[$({date_fmt})] Starting worker on $node"
  srun --nodes=1 --ntasks=1 -w $node \\
    bash -c "$SPARK_HOME/sbin/start-worker.sh $MASTER_URL" &
  sleep 2
done

export SPARK_MASTER_URL="$MASTER_URL"
echo "[$({date_fmt})] Spark cluster ready with {len(nodes)} nodes"

# Create cleanup script
cat > {working_dir}/spark_cleanup.sh <<'CLEANUP_EOF'
#!/bin/bash
echo "[$({date_fmt})] Stopping Spark cluster..."
for node in {" ".join(nodes)}; do
  srun --nodes=1 --ntasks=1 -w $node \\
    bash -c '$SPARK_HOME/sbin/stop-worker.sh' || true
done
srun --nodes=1 --ntasks=1 -w {head_node} \\
  bash -c '$SPARK_HOME/sbin/stop-master.sh' || true
CLEANUP_EOF
chmod +x {working_dir}/spark_cleanup.sh

"""
