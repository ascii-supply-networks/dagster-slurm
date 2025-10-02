"""Ray cluster launcher with robust startup/shutdown."""

import shlex
from typing import Dict, Optional, List, Any
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
    - Session: Multi-node Ray cluster across Slurm allocation
    - Connect: Connect to existing cluster
    """

    # ✅ ALL parameters as Pydantic Fields - NO __init__ method!
    activate_sh: Optional[str] = Field(
        default=None, description="Environment activation script"
    )
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
    ) -> ExecutionPlan:
        """Generate Ray execution plan."""

        messages_path = f"{working_dir}/messages.jsonl"

        # Build environment variables section
        env_vars = "\n".join(
            f"export {key}={shlex.quote(value)}" for key, value in pipes_context.items()
        )

        if extra_env:
            env_vars += "\n" + "\n".join(
                f"export {key}={shlex.quote(str(value))}"
                for key, value in extra_env.items()
            )

        # Environment activation
        activation = ""
        if self.activate_sh:
            activation = f"""
if [ -f {shlex.quote(self.activate_sh)} ]; then
  source {shlex.quote(self.activate_sh)}
fi
"""

        # Ray setup based on mode
        if self.ray_address:
            # Mode: Connect to existing cluster
            ray_setup = f"""
export RAY_ADDRESS="{self.ray_address}"
echo "$(date +%Y-%m-%dT%H:%M:%S%z)] Connecting to Ray cluster: {self.ray_address}"
"""
        elif allocation_context:
            # Mode: Start cluster in Slurm allocation (ROBUST VERSION)
            ray_setup = self._generate_cluster_template(allocation_context, working_dir)
        else:
            # Mode: Start local single-node Ray
            ray_setup = self._generate_local_template()

        # Complete script
        script = f"""#!/bin/bash
set -euo pipefail

: > "{messages_path}" || true
echo "$(date +%Y-%m-%dT%H:%M:%S%z)] Starting Ray execution"

# ===== Dagster Pipes Environment =====
{env_vars}

# ===== Environment Activation =====
{activation}

# ===== Ray Setup =====
{ray_setup}

# ===== Execute Payload =====
echo "$(date +%Y-%m-%dT%H:%M:%S%z)] Executing Ray payload..."
{python_executable} {shlex.quote(payload_path)}
exit_code=$?
echo "$(date +%Y-%m-%dT%H:%M:%S%z)] Payload finished with exit code $exit_code"

# Cleanup handled by trap
exit $exit_code
"""

        return ExecutionPlan(
            kind=RuntimeVariant.RAY,
            payload=script.split("\n"),
            environment={**pipes_context, **(extra_env or {})},
            resources={
                "cpus": allocation_context.get("num_nodes", 1)
                if allocation_context
                else 1,
                "gpus": self.num_gpus_per_node,
            },
        )

    def _generate_local_template(self) -> str:
        """Generate Ray startup for local mode."""
        # Build object store argument if specified
        obj_store = ""
        if self.object_store_memory_gb is not None:
            bytes_value = self.object_store_memory_gb * 1_000_000_000
            obj_store = f"--object-store-memory={bytes_value}"

        return f"""
    echo "$(date +%Y-%m-%dT%H:%M:%S%z)] Starting local Ray cluster"

    # Cleanup function - MUST be defined before trap
    cleanup_ray() {{
    echo "$(date +%Y-%m-%dT%H:%M:%S%z)] Stopping Ray..."
    ray stop --grace-period {self.grace_period} || true
    
    # Kill any remaining Ray processes
    if [[ -n "${{RAY_HEAD_PID:-}}" ]] && kill -0 "$RAY_HEAD_PID" 2>/dev/null; then
        kill "$RAY_HEAD_PID" 2>/dev/null || true
        wait "$RAY_HEAD_PID" 2>/dev/null || true
    fi
    
    echo "$(date +%Y-%m-%dT%H:%M:%S%z)] Ray stopped"
    }}

    # Set trap BEFORE starting Ray
    trap cleanup_ray EXIT SIGINT SIGTERM

    # Redirect Ray startup stderr to suppress connection messages that arrive late
    ray start --head --port={self.ray_port} \\
    --dashboard-host=127.0.0.1 --dashboard-port={self.dashboard_port} \\
    --num-gpus={self.num_gpus_per_node} {obj_store} --block \\
    2>/dev/null &

    RAY_HEAD_PID=$!
    export RAY_ADDRESS="127.0.0.1:{self.ray_port}"

    # Wait for Ray to be ready (up to {self.head_startup_timeout} seconds)
    echo "$(date +%Y-%m-%dT%H:%M:%S%z)] Waiting for Ray to be ready..."
    for i in {{1..{self.head_startup_timeout}}}; do
    if ray status --address "$RAY_ADDRESS" &>/dev/null; then
        echo "$(date +%Y-%m-%dT%H:%M:%S%z)] Ray is ready (local mode)"
        break
    fi
    
    # Check if Ray process died
    if ! kill -0 "$RAY_HEAD_PID" 2>/dev/null; then
        echo "$(date +%Y-%m-%dT%H:%M:%S%z)] ERROR: Ray process died during startup" >&2
        exit 1
    fi
    
    if [[ $i -eq {self.head_startup_timeout} ]]; then
        echo "$(date +%Y-%m-%dT%H:%M:%S%z)] ERROR: Ray failed to start within {self.head_startup_timeout} seconds" >&2
        exit 1
    fi
    
    sleep 1
    done

    echo "$(date +%Y-%m-%dT%H:%M:%S%z)] Ray cluster ready"
    ray status --address "$RAY_ADDRESS" 2>/dev/null || true
    """

    def _generate_cluster_template(
        self,
        allocation_context: Dict[str, Any],
        working_dir: str,
    ) -> str:
        """
        Generate ROBUST Ray cluster startup for Slurm allocation.
        Based on the template with sentinel-based cleanup.
        """

        num_nodes = len(allocation_context.get("nodes", []))
        slurm_job_id = allocation_context.get("slurm_job_id", "$$")

        # Build object store argument if specified
        obj_store = ""
        if self.object_store_memory_gb is not None:
            bytes_value = self.object_store_memory_gb * 1_000_000_000
            obj_store = f"--object-store-memory={bytes_value}"

        redis_pw = self.redis_password or "$(uuidgen)"

        return f"""
# ========================================
# Robust Ray Cluster Startup
# ========================================
echo "$(date +%Y-%m-%dT%H:%M:%S%z)] Starting Ray on {num_nodes} nodes"

# Shared sentinel file for coordinated shutdown
SENTINEL="{working_dir}/.ray_shutdown.{slurm_job_id}"

# Cleanup function
cleanup() {{
  echo "$(date +%Y-%m-%dT%H:%M:%S%z)] Performing cleanup..."
  : > "$SENTINEL"  # Signal all Ray processes to stop
  sleep 2          # Let processes notice sentinel
  wait || true     # Wait for background srun jobs
  rm -f "$SENTINEL" || true
  echo "$(date +%Y-%m-%dT%H:%M:%S%z)] Cleanup complete"
}}
trap cleanup EXIT SIGINT SIGTERM

# ===== Compute head address =====
redis_password={redis_pw}
export redis_password

# Get all nodes in allocation
nodes=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
nodes_array=($nodes)

head_node=${{nodes_array[0]}}
ip=$(srun --nodes=1 --ntasks=1 -w "$head_node" hostname --ip-address)

# Normalize to IPv4 if multiple IPs
if [[ "$ip" == *" "* ]]; then
  IFS=' ' read -ra ADDR <<< "$ip"
  if [[ ${{#ADDR[0]}} -gt 16 ]]; then
    ip=${{ADDR[1]}}
  else
    ip=${{ADDR[0]}}
  fi
  echo "$(date +%Y-%m-%dT%H:%M:%S%z)] Multiple IPs detected; using IPv4 $ip"
fi

port={self.ray_port}
ip_head="$ip:$port"
export ip_head
export RAY_ADDRESS="$ip_head"
echo "$(date +%Y-%m-%dT%H:%M:%S%z)] Head node: $head_node | IP: $ip_head"

# ===== Start HEAD (daemon watching sentinel) =====
echo "$(date +%Y-%m-%dT%H:%M:%S%z)] Starting Ray head at $head_node"
srun --export=ALL --nodes=1 --ntasks=1 -w "$head_node" \\
  bash --noprofile --norc -lc '
    set -euo pipefail
    SENTINEL="'"$SENTINEL"'"
    
    # Graceful shutdown on TERM/INT
    cleanup_node() {{
      ray stop -v --grace-period {self.grace_period} || true
      exit 0
    }}
    trap cleanup_node TERM INT
    
    # Start Ray head
    ray start --head -v \\
      --node-ip-address="'"$ip"'" \\
      --port="'"$port"'" \\
      --dashboard-host=0.0.0.0 --dashboard-port={self.dashboard_port} \\
      --num-gpus={self.num_gpus_per_node} {obj_store} \\
      --redis-password="'"$redis_password"'" \\
      --block &
    ray_pid=$!
    
    # Watch for shutdown sentinel
    while :; do
      [[ -f "$SENTINEL" ]] && cleanup_node
      if ! kill -0 "$ray_pid" 2>/dev/null; then exit 0; fi
      sleep 1
    done
  ' &

# ===== Wait for head ready =====
echo "$(date +%Y-%m-%dT%H:%M:%S%z)] Waiting for Ray head..."
for i in {{1..{self.head_startup_timeout}}}; do
  if ray status --address "$ip_head" &>/dev/null; then
    echo "$(date +%Y-%m-%dT%H:%M:%S%z)] Ray head is ready"
    break
  fi
  if [[ $i -eq {self.head_startup_timeout} ]]; then
    echo "$(date +%Y-%m-%dT%H:%M:%S%z)] ERROR: Ray head failed to start" >&2
    exit 1
  fi
  sleep 2
done

# ===== Start WORKERS (each watches sentinel) =====
worker_num=$((SLURM_JOB_NUM_NODES - 1))
if (( worker_num > 0 )); then
  for ((i = 1; i <= worker_num; i++)); do
    node_i=${{nodes_array[$i]}}
    echo "$(date +%Y-%m-%dT%H:%M:%S%z)] Starting worker $i at $node_i"
    
    srun --export=ALL --nodes=1 --ntasks=1 -w "$node_i" \\
      bash --noprofile --norc -lc '
        set -euo pipefail
        SENTINEL="'"$SENTINEL"'"
        
        cleanup_node() {{
          ray stop -v --grace-period {self.grace_period} || true
          exit 0
        }}
        trap cleanup_node TERM INT
        
        # Start Ray worker
        ray start -v \\
          --address="'"$ip_head"'" \\
          --redis-password="'"$redis_password"'" \\
          --num-gpus={self.num_gpus_per_node} {obj_store} \\
          --block &
        ray_pid=$!
        
        # Watch for shutdown
        while :; do
          [[ -f "$SENTINEL" ]] && cleanup_node
          if ! kill -0 "$ray_pid" 2>/dev/null; then exit 0; fi
          sleep 1
        done
      ' &
    
    sleep {self.worker_startup_delay}  # Avoid thundering herd
  done
fi

echo "$(date +%Y-%m-%dT%H:%M:%S%z)] All Ray nodes started. Waiting for registration..."
sleep 5

echo "$(date +%Y-%m-%dT%H:%M:%S%z)] Ray cluster ready with {num_nodes} nodes"
ray status --address "$ip_head" || true
"""
