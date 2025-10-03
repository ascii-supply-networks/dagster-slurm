"""Ray cluster launcher with robust startup/shutdown."""

import shlex
from typing import Dict, Optional, Any
from .base import ComputeLauncher, ExecutionPlan
from dagster_slurm.config.runtime import RuntimeVariant
from pydantic import Field
import textwrap


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
        default=60, description="Seconds to wait for graceful shutdown"
    )
    head_startup_timeout: int = Field(
        default=40, description="Seconds to wait for head to be ready"
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

        # Ray setup based on mode
        if self.ray_address:
            # Mode: Connect to existing cluster
            script += f"""# Connect to existing Ray cluster
  export RAY_ADDRESS={shlex.quote(self.ray_address)}
  echo "[$({date_fmt})] Connecting to Ray cluster: {self.ray_address}"

  """
        elif allocation_context:
            # Mode: Start cluster in Slurm allocation (session mode)
            script += self._generate_cluster_template(
                allocation_context, working_dir, date_fmt
            )
        else:
            # Mode: Detect if running in multi-node Slurm job, otherwise local
            # Generate both branches inline to avoid quote escaping issues
            script += f"""# Detect Ray mode
  if [[ -n "${{SLURM_CLUSTER_MODE:-}}" ]]; then
      echo "[$({date_fmt})] Detected multi-node Slurm allocation ($SLURM_JOB_NUM_NODES nodes)"
  """
            # Add cluster template (indented for the if block)
            cluster_lines = self._generate_cluster_template_from_env(
                working_dir, date_fmt
            )
            script += cluster_lines

            script += f"""else
      echo "[$({date_fmt})] Single-node mode - starting local Ray"
  """
            # Add local template (indented for the else block)
            local_lines = self._generate_local_template(date_fmt)
            # Indent each line by 4 spaces for the else block
            for line in local_lines.split("\n"):
                if line.strip():  # Don't indent empty lines
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
        ray stop --grace-period {self.grace_period} || true
        echo "[$({date_fmt})] Ray stopped"
      }}
      
      # Set trap BEFORE starting Ray
      trap cleanup_ray EXIT SIGINT SIGTERM
      
      # Start Ray head (remove --block since we're backgrounding)
      ray start --head --port={self.ray_port} \\
        --dashboard-host=127.0.0.1 --dashboard-port={self.dashboard_port} \\
        --num-gpus={self.num_gpus_per_node} {obj_store}
      
      export RAY_ADDRESS="127.0.0.1:{self.ray_port}"
      
      # Wait for Ray to be ready
      echo "[$({date_fmt})] Waiting for Ray to be ready..."
      for i in {{1..{self.head_startup_timeout}}}; do
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

    def _generate_ray_node_script(self, node_type: str) -> str:
        """Generate the script that runs inside bash -c on compute nodes."""
        obj_store = ""
        if self.object_store_memory_gb is not None:
            bytes_value = self.object_store_memory_gb * 1_000_000_000
            obj_store = f"--object-store-memory={bytes_value}"

        if node_type == "head":
            ray_cmd = f"""ray start --head -v \\
      --node-ip-address=\\"\\$ip\\" \\
      --port=\\"\\$port\\" \\
      --dashboard-host=0.0.0.0 --dashboard-port={self.dashboard_port} \\
      --num-gpus={self.num_gpus_per_node} {obj_store} \\
      --redis-password=\\"\\$redis_password\\" \\
      --block &"""
        else:  # worker
            ray_cmd = f"""ray start -v \\
      --address=\\"\\$ip_head\\" \\
      --redis-password=\\"\\$redis_password\\" \\
      --num-gpus={self.num_gpus_per_node} {obj_store} \\
      --block &"""

        return f"""set -euo pipefail
  cleanup_node() {{
    ray stop -v --grace-period {self.grace_period} || true
    exit 0
  }}
  trap cleanup_node TERM INT
  {ray_cmd}
  ray_pid=\\$!
  while :; do
    [[ -f \\"\\$SENTINEL\\" ]] && cleanup_node
    if ! kill -0 \\"\\$ray_pid\\" 2>/dev/null; then exit 0; fi
    sleep 1
  done"""

    def _generate_cluster_template(
        self,
        allocation_context: Dict[str, Any],
        working_dir: str,
        date_fmt: str,
    ) -> str:
        """Generate ROBUST Ray cluster startup for Slurm allocation."""
        num_nodes = len(allocation_context.get("nodes", []))
        slurm_job_id = allocation_context.get("slurm_job_id", "$$")
        redis_pw = self.redis_password or "$(uuidgen)"

        head_script = self._generate_ray_node_script("head")
        worker_script = self._generate_ray_node_script("worker")

        script = f"""# ========================================
    # Robust Ray Cluster Startup
    # ========================================
    echo "[$({date_fmt})] Starting Ray on {num_nodes} nodes"
    # Shared sentinel file for coordinated shutdown
    SENTINEL="{working_dir}/.ray_shutdown.{slurm_job_id}"
    # Cleanup function
    cleanup() {{
      echo "[$({date_fmt})] Performing cleanup..."
      : > "$SENTINEL"
      sleep 2
      wait || true
      rm -f "$SENTINEL" || true
      echo "[$({date_fmt})] Cleanup complete"
    }}
    trap cleanup EXIT SIGINT SIGTERM
    # ===== Compute head address =====
    redis_password={redis_pw}
    export redis_password
    # Get all nodes in allocation
    nodes=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
    nodes_array=($nodes)
    head_node="${{nodes_array[0]}}"
    ip=$(srun --nodes=1 --ntasks=1 -w "$head_node" hostname --ip-address)
    # Normalize to IPv4 if multiple IPs
    if [[ "$ip" == *" "* ]]; then
      IFS=" " read -r -a ADDR <<< "$ip"
      if [[ ${{#ADDR[0]}} -gt 16 ]]; then
        ip="${{ADDR[1]}}"
      else
        ip="${{ADDR[0]}}"
      fi
      echo "[$({date_fmt})] Multiple IPs detected; using IPv4 $ip"
    fi
    port={self.ray_port}
    ip_head="$ip:$port"
    export ip_head
    export RAY_ADDRESS="$ip_head"
    echo "[$({date_fmt})] Head node: $head_node | IP: $ip_head"
    # ===== Start HEAD =====
    echo "[$({date_fmt})] Starting Ray head at $head_node"
    export SENTINEL
    export ip
    export port
    export redis_password
    srun --export=ALL --nodes=1 --ntasks=1 -w "$head_node" bash -c "{head_script}" &
    # ===== Wait for head ready =====
    echo "[$({date_fmt})] Waiting for Ray head..."
    for i in {{{{1..{self.head_startup_timeout}}}}}; do
      if ray status --address "$ip_head" &>/dev/null; then
        echo "[$({date_fmt})] Ray head is ready"
        break
      fi
      if [[ $i -eq {self.head_startup_timeout} ]]; then
        echo "[$({date_fmt})] ERROR: Ray head failed to start" >&2
        exit 1
      fi
      sleep 2
    done
    # ===== Start WORKERS =====
    worker_num=$((SLURM_JOB_NUM_NODES - 1))
    if (( worker_num > 0 )); then
      for ((i = 1; i <= worker_num; i++)); do
        node_i="${{nodes_array[$i]}}"
        echo "[$({date_fmt})] Starting worker $i at $node_i"
        srun --export=ALL --nodes=1 --ntasks=1 -w "$node_i" bash -c "{worker_script}" &
        sleep {self.worker_startup_delay}
      done
    fi
    echo "[$({date_fmt})] All Ray nodes started. Waiting for registration..."
    sleep 5
    echo "[$({date_fmt})] Ray cluster ready with {num_nodes} nodes"
    ray status --address "$ip_head" || true
    """
        return script

    def _generate_cluster_template_from_env(
        self,
        working_dir: str,
        date_fmt: str,
    ) -> str:
        """Generate Ray cluster startup using Slurm environment variables."""
        redis_pw = self.redis_password or "$(uuidgen)"

        head_script = self._generate_ray_node_script("head")
        worker_script = self._generate_ray_node_script("worker")

        script = f"""# ========================================
    # Robust Ray Cluster Startup (from Slurm env)
    # ========================================
    echo "[$({date_fmt})] Starting Ray on $SLURM_JOB_NUM_NODES nodes"
    # Shared sentinel file for coordinated shutdown
    SENTINEL="{working_dir}/.ray_shutdown.$SLURM_JOB_ID"
    # Cleanup function
    cleanup() {{
      echo "[$({date_fmt})] Performing cleanup..."
      : > "$SENTINEL"
      sleep 2
      wait || true
      rm -f "$SENTINEL" || true
      echo "[$({date_fmt})] Cleanup complete"
    }}
    trap cleanup EXIT SIGINT SIGTERM
    # ===== Compute head address =====
    redis_password={redis_pw}
    export redis_password
    # Get all nodes in allocation
    nodes=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
    nodes_array=($nodes)
    head_node="${{nodes_array[0]}}"
    ip=$(srun --nodes=1 --ntasks=1 -w "$head_node" hostname --ip-address)
    # Normalize to IPv4 if multiple IPs
    if [[ "$ip" == *" "* ]]; then
      IFS=" " read -r -a ADDR <<< "$ip"
      if [[ ${{#ADDR[0]}} -gt 16 ]]; then
        ip="${{ADDR[1]}}"
      else
        ip="${{ADDR[0]}}"
      fi
      echo "[$({date_fmt})] Multiple IPs detected; using IPv4 $ip"
    fi
    port={self.ray_port}
    ip_head="$ip:$port"
    export ip_head
    export RAY_ADDRESS="$ip_head"
    echo "[$({date_fmt})] Head node: $head_node | IP: $ip_head"
    # ===== Start HEAD =====
    echo "[$({date_fmt})] Starting Ray head at $head_node"
    export SENTINEL
    export ip
    export port
    export redis_password
    srun --export=ALL --nodes=1 --ntasks=1 -w "$head_node" bash -c "{head_script}" &
    # ===== Wait for head ready =====
    echo "[$({date_fmt})] Waiting for Ray head..."
    for i in {{{{1..{self.head_startup_timeout}}}}}; do
      if ray status --address "$ip_head" &>/dev/null; then
        echo "[$({date_fmt})] Ray head is ready"
        break
      fi
      if [[ $i -eq {self.head_startup_timeout} ]]; then
        echo "[$({date_fmt})] ERROR: Ray head failed to start" >&2
        exit 1
      fi
      sleep 2
    done
    # ===== Start WORKERS =====
    worker_num=$(($SLURM_JOB_NUM_NODES - 1))
    if (( worker_num > 0 )); then
      for ((i = 1; i <= worker_num; i++)); do
        node_i="${{nodes_array[$i]}}"
        echo "[$({date_fmt})] Starting worker $i at $node_i"
        srun --export=ALL --nodes=1 --ntasks=1 -w "$node_i" bash -c "{worker_script}" &
        sleep {self.worker_startup_delay}
      done
    fi
    echo "[$({date_fmt})] All Ray nodes started. Waiting for registration..."
    sleep 5
    echo "[$({date_fmt})] Ray cluster ready with $SLURM_JOB_NUM_NODES nodes"
    ray status --address "$ip_head" || true
    """
        return script
