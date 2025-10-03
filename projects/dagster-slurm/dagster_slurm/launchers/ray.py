"""Ray cluster launcher with robust startup/shutdown."""

import shlex
from typing import Dict, Optional, Any
from .base import ComputeLauncher, ExecutionPlan
from dagster_slurm.config.runtime import RuntimeVariant
from pydantic import Field


class RayLauncher(ComputeLauncher):
    """
    Ray distributed computing launcher.

    Features:
    - Robust cluster startup with sentinel-based shutdown
    - Graceful cleanup on SIGTERM/SIGINT
    - Worker registration monitoring
    - Automatic head node detection
    - IPv4/IPv6 normalization

    Modes:
    - Local: Single-node Ray
    - Cluster: Multi-node Ray cluster across Slurm allocation (via allocation_context)
    - Connect: Connect to existing cluster (via ray_address)
    """

    # Ray configuration
    num_gpus_per_node: int = Field(default=0, description="GPUs to allocate per node")
    ray_address: Optional[str] = Field(
        default=None, description="Connect to existing cluster (skip startup)"
    )
    dashboard_port: int = Field(default=8265, description="Ray dashboard port")
    object_store_memory_gb: Optional[int] = Field(
        default=None, description="Object store size (None = auto)"
    )
    redis_password: Optional[str] = Field(
        default=None, description="Redis password (None = auto-generate with uuidgen)"
    )
    ray_port: int = Field(default=6379, description="Ray head port")
    grace_period: int = Field(
        default=10, description="Seconds to wait for graceful shutdown"
    )
    head_startup_timeout: int = Field(
        default=60, description="Seconds to wait for head to be ready"
    )
    worker_startup_delay: int = Field(
        default=1, description="Seconds between worker starts"
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
        """Generate Ray execution plan."""
        messages_path = f"{working_dir}/messages.jsonl"
        date_fmt = "date +%Y-%m-%dT%H:%M:%S%z"
        python_quoted = shlex.quote(python_executable)
        payload_quoted = shlex.quote(payload_path)

        # Build header
        script = f"""#!/bin/bash
  set -euo pipefail
  : > "{messages_path}" || true
  echo "[$({date_fmt})] ========================================="
  echo "[$({date_fmt})] Ray Workload Execution"
  echo "[$({date_fmt})] Working dir: {working_dir}"
  echo "[$({date_fmt})] ========================================="
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

        # Initialize auxiliary_scripts dict
        auxiliary_scripts = {}

        # Ray setup based on mode
        if self.ray_address:
            # Mode: Connect to existing cluster
            script += f"""# Connect to existing Ray cluster
  export RAY_ADDRESS={shlex.quote(self.ray_address)}
  echo "[$({date_fmt})] Connecting to Ray cluster: {self.ray_address}"
  """
        elif allocation_context:
            # Mode: Start cluster in Slurm allocation (session mode)
            cluster_script, aux_scripts = self._generate_cluster_template(
                allocation_context, working_dir, date_fmt
            )
            script += cluster_script
            auxiliary_scripts.update(aux_scripts)
        else:
            # Mode: Detect if running in multi-node Slurm job, otherwise local
            script += f"""# Detect Ray mode
  if [[ -n "${{SLURM_CLUSTER_MODE:-}}" ]]; then
      echo "[$({date_fmt})] Detected multi-node Slurm allocation ($SLURM_JOB_NUM_NODES nodes)"
  """
            # Add cluster template
            cluster_script, aux_scripts = self._generate_cluster_template(
                working_dir, date_fmt
            )
            script += cluster_script
            auxiliary_scripts.update(aux_scripts)

            script += f"""else
      echo "[$({date_fmt})] Single-node mode - starting local Ray"
  """
            # Add local template
            local_lines = self._generate_local_template(date_fmt)
            for line in local_lines.split("\n"):
                if line.strip():
                    script += f"    {line}\n"
                else:
                    script += "\n"
            script += "fi\n\n"

        # Execute payload
        script += f"""# Execute Ray payload
  echo "[$({date_fmt})] Executing Ray payload..."
  {python_quoted} {payload_quoted}
  exit_code=$?
  echo "[$({date_fmt})] Payload finished with exit code $exit_code"
  # Cleanup handled by trap
  exit $exit_code
  """

        return ExecutionPlan(
            kind=RuntimeVariant.RAY,
            payload=script.split("\n"),
            environment={**pipes_context, **(extra_env or {})},
            resources={
                "nodes": allocation_context.get("num_nodes", 1)
                if allocation_context
                else 1,
                "gpus": self.num_gpus_per_node,
            },
            auxiliary_scripts=auxiliary_scripts,
        )

    def _generate_local_template(self, date_fmt: str) -> str:
        """Generate Ray startup for local mode."""
        # Build object store argument if specified
        obj_store = ""
        if self.object_store_memory_gb is not None:
            bytes_value = self.object_store_memory_gb * 1_000_000_000
            obj_store = f"--object-store-memory={bytes_value}"

        return f"""# Start local Ray cluster
echo "[$({date_fmt})] Starting local Ray cluster"

# Cleanup function - MUST be defined before trap
cleanup_ray() {{
  echo "[$({date_fmt})] Stopping Ray..."
  ray stop --force || true
  echo "[$({date_fmt})] Ray stopped"
}}

# Set trap BEFORE starting Ray
trap cleanup_ray EXIT SIGINT SIGTERM

# Start Ray head
ray start --head --port={self.ray_port} \\
  --dashboard-host=127.0.0.1 --dashboard-port={self.dashboard_port} \\
  --num-gpus={self.num_gpus_per_node} {obj_store}

export RAY_ADDRESS="127.0.0.1:{self.ray_port}"

# Wait for Ray to be ready
echo "[$({date_fmt})] Waiting for Ray to be ready..."
for i in $(seq 1 {self.head_startup_timeout}); do
  if ray status --address "$RAY_ADDRESS" &>/dev/null; then
    echo "[$({date_fmt})] Ray is ready (local mode)"
    break
  fi
  if [[ $i -eq {self.head_startup_timeout} ]]; then
    echo "[$({date_fmt})] ERROR: Ray failed to start within {self.head_startup_timeout} seconds" >&2
    exit 1
  fi
  sleep 1
done

echo "[$({date_fmt})] Ray cluster ready"
ray status --address "$RAY_ADDRESS" 2>/dev/null || true

"""

    # --------------------------
    def _generate_cluster_template(
        self,
        working_dir: str,
        date_fmt: str,
        allocation_context: Optional[Dict[str, Any]] = None,
    ) -> tuple[str, dict]:
        """
        Generates a robust and simple Ray cluster startup script.
        - Moves readiness-check logic into the worker nodes.
        - The main script simply launches daemons and then the user payload.
        """
        redis_pw = self.redis_password or "$(uuidgen)"

        # Build object store argument
        obj_store_arg = ""
        if self.object_store_memory_gb is not None:
            bytes_value = int(self.object_store_memory_gb * 1_000_000_000)
            obj_store_arg = f"--object-store-memory={bytes_value}"

        # Determine temp dir path
        if allocation_context:
            slurm_job_id = allocation_context.get("slurm_job_id", "$$")
            temp_dir_path = f"/tmp/ray-{slurm_job_id}"
        else:
            temp_dir_path = "/tmp/ray-$SLURM_JOB_ID"
        temp_dir_arg = f"--temp-dir={temp_dir_path}"

        # Determine node script and variables based on mode
        if allocation_context:
            nodes = allocation_context.get("nodes", [])
            num_nodes_var = str(len(nodes))
            nodes_array_init = f"nodes_array=({' '.join(nodes)})"
        else:
            num_nodes_var = "$SLURM_JOB_NUM_NODES"
            nodes_array_init = 'nodes=$(scontrol show hostnames "$SLURM_JOB_NODELIST"); nodes_array=($nodes)'

        main_script = f"""# ========================================
  # Ray Cluster Startup
  # ========================================
  echo "[$({date_fmt})] Starting Ray on {num_nodes_var} nodes"

  # This trap ensures that when the main script exits, it waits for all
  # backgrounded srun processes to complete. Slurm's job teardown will
  # send SIGTERM to these steps, allowing them to clean up gracefully.
  trap 'echo "Main script finished, waiting for srun steps..."; wait' EXIT

  # ===== Compute head address =====
  redis_password={redis_pw}

  # Initialize node array based on mode
  {nodes_array_init}
  head_node="${{nodes_array[0]}}"

  # Get IP from head node
  ip=$(srun --nodes=1 --ntasks=1 -w "$head_node" hostname --ip-address | awk '{{print $1}}')
  port={self.ray_port}
  ip_head="$ip:$port"
  export RAY_ADDRESS="$ip_head"

  echo "[$({date_fmt})] Head node: $head_node | IP: $ip_head"

  # ===== Start HEAD =====
  echo "[$({date_fmt})] Launching Ray head on $head_node..."
  # The srun step is backgrounded, but the 'ray start --block' command inside it runs in the foreground of that step.
  srun --export=ALL --nodes=1 --ntasks=1 -w "$head_node" \\
  bash --noprofile --norc -c '
    set -e
    cleanup_node() {{ echo "Head received signal, stopping Ray..."; ray stop -v || true; rm -rf {temp_dir_path}; exit 0; }}
    trap cleanup_node TERM INT
    
    echo "Ray head starting..."
    ray start --head -v \\
      --node-ip-address="'"$ip"'" \\
      --port="'"$port"'" \\
      --dashboard-host=0.0.0.0 \\
      --dashboard-port={self.dashboard_port} \\
      --num-gpus={self.num_gpus_per_node} \\
      {obj_store_arg} \\
      {temp_dir_arg} \\
      --redis-password="'"$redis_password"'" \\
      --block
  ' &

  # Give head a moment to start before launching workers
  echo "[$({date_fmt})] Waiting 5s for head process to initialize..."
  sleep 5

  # ===== Start WORKERS =====
  worker_num=$(({num_nodes_var} - 1))
  if (( worker_num > 0 )); then
    for ((i = 1; i < {num_nodes_var}; i++)); do
      node_i="${{nodes_array[$i]}}"
      echo "[$({date_fmt})] Launching worker $i on $node_i..."
      
      srun --export=ALL --nodes=1 --ntasks=1 -w "$node_i" \\
      bash --noprofile --norc -c '
        set -e
        cleanup_node() {{ echo "Worker received signal, stopping Ray..."; ray stop -v || true; rm -rf {temp_dir_path}; exit 0; }}
        trap cleanup_node TERM INT

        # Worker waits for the head to be reachable before attempting to start.
        echo "Worker on $(hostname) is waiting for head at '"$ip_head"'..."
        for try in {{1..120}}; do # Wait for up to 2 minutes
          if timeout 1 bash -c "cat < /dev/null > /dev/tcp/$(echo "'"$ip_head"'" | cut -d: -f1)/$(echo "'"$ip_head"'" | cut -d: -f2)"; then
            echo "Worker on $(hostname) can reach head. Starting ray worker..."
            break
          fi
          if [[ $try -eq 120 ]]; then
            echo "ERROR: Worker on $(hostname) could not connect to head after 120 seconds." >&2
            exit 1
          fi
          sleep 1
        done
        
        ray start -v \\
          --address="'"$ip_head"'" \\
          --redis-password="'"$redis_password"'" \\
          --num-gpus={self.num_gpus_per_node} \\
          {obj_store_arg} \\
          {temp_dir_arg} \\
          --block
      ' &
      
      sleep 1
    done
  fi

  echo "[$({date_fmt})] All Ray srun steps launched. The user payload will now run."
  echo "[$({date_fmt})] The ray.init() in the python script is responsible for the final connection wait."
  # The main script now continues to the user payload.
  """
        # No auxiliary scripts are needed with this inline approach.
        return main_script, {}
