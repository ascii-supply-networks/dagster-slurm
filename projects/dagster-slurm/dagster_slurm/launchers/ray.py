"""Ray cluster launcher with robust startup/shutdown."""

import shlex
from typing import Any, Dict, Optional

from pydantic import Field

from dagster_slurm.config.runtime import RuntimeVariant

from .base import ComputeLauncher, ExecutionPlan


class RayLauncher(ComputeLauncher):
    """Ray distributed computing launcher.

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
        date_fmt = "date +%Y-%m-%dT%H:%M:%S%z"
        python_command = f"{shlex.quote(python_executable)} {shlex.quote(payload_path)}"

        # Build header for the main script
        script = f"""#!/bin/bash
set -euo pipefail
echo "[$({date_fmt})] ========================================="
echo "[$({date_fmt})] Ray Workload Launcher"
echo "[$({date_fmt})] Working dir: {working_dir}"
echo "[$({date_fmt})] ========================================="
"""
        # Export all environment variables
        script += "# Exporting environment variables...\n"
        for key, value in {**pipes_context, **(extra_env or {})}.items():
            script += f"export {key}={shlex.quote(str(value))}\n"
        script += "\n"

        auxiliary_scripts = {}

        # Ray setup based on mode
        if self.ray_address:
            # Mode: Connect to existing cluster.
            script += f"""# Connect to existing Ray cluster
export RAY_ADDRESS={shlex.quote(self.ray_address)}
echo "[$({date_fmt})] Connecting to Ray cluster: {self.ray_address}"
echo "[$({date_fmt})] Executing payload..."
"""
            if activation_script:
                script += f"source {shlex.quote(activation_script)}\n"
            script += f"{python_command}\n"

        elif allocation_context:
            # Mode: Start cluster in a pre-existing Slurm allocation (session mode)
            if not activation_script:
                raise ValueError(
                    "An 'activation_script' is required for multi-node Ray execution in session mode."
                )

            cluster_payload, aux_scripts = self._generate_cluster_template(
                python_executable=python_executable,
                payload_path=payload_path,
                working_dir=working_dir,
                date_fmt=date_fmt,
                activation_script=activation_script,
                allocation_context=allocation_context,
            )
            script += cluster_payload
            auxiliary_scripts.update(aux_scripts)

        else:
            # Mode: Standalone job. Could be multi-node (on Slurm) or single-node (local or Slurm).
            script += f"""# Detect Ray mode
if [[ -n "${{SLURM_JOB_ID:-}}" && "${{SLURM_JOB_NUM_NODES:-1}}" -gt 1 ]]; then
    echo "[$({date_fmt})] Detected multi-node Slurm allocation ($SLURM_JOB_NUM_NODES nodes)"
"""
            # ** THE MINIMAL FIX IS HERE **
            # We now use the ROBUST template generator for the multi-node Slurm case.
            # It requires an activation script, so we add a check.
            if not activation_script:
                script += '    echo "ERROR: An activation_script is required for multi-node Ray execution but was not provided." >&2; exit 1\n'
            else:
                cluster_payload, aux_scripts = self._generate_cluster_template(
                    python_executable=python_executable,
                    payload_path=payload_path,
                    working_dir=working_dir,
                    date_fmt=date_fmt,
                    activation_script=activation_script,
                    allocation_context=None,
                )
                script += cluster_payload
                auxiliary_scripts.update(aux_scripts)

            script += f"""
else
    echo "[$({date_fmt})] Single-node mode detected. Starting local Ray cluster..."
"""
            # This 'local' block is used for both true local dev and single-node Slurm jobs.
            # It works for both because it correctly handles an optional activation_script.
            local_lines = self._generate_local_template(date_fmt, activation_script)
            script += local_lines

            script += f"""
echo "[$({date_fmt})] Executing payload in local mode..."
{python_command}
"""
            script += "fi\n\n"

        return ExecutionPlan(
            kind=RuntimeVariant.RAY,
            payload=script.split("\n"),
            environment={},
            resources={
                "nodes": allocation_context.get("num_nodes", 1)
                if allocation_context
                else 1,
                "gpus": self.num_gpus_per_node,
            },
            auxiliary_scripts=auxiliary_scripts,
        )

    def _generate_local_template(
        self, date_fmt: str, activation_script: Optional[str]
    ) -> str:
        """Generate Ray startup for local (single-node) mode."""
        # Build object store argument if specified
        obj_store = ""
        if self.object_store_memory_gb is not None:
            bytes_value = self.object_store_memory_gb * 1_000_000_000
            obj_store = f"--object-store-memory={bytes_value}"

        # **THE FIX IS HERE**: Only add the activation block if the script is provided.
        activation_block = ""
        if activation_script:
            activation_block = f"""
    # Activate environment for local Ray
    echo "[$({date_fmt})] Activating environment for local Ray..."
    source {shlex.quote(activation_script)}
    echo "[$({date_fmt})] Environment activated."
    """
        # The rest of the function remains the same
        return f"""{activation_block}
    # Start local Ray cluster
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

    def _generate_cluster_template(
        self,
        python_executable: str,
        payload_path: str,
        working_dir: str,
        date_fmt: str,
        activation_script: str,  # Now a required argument
        allocation_context: Optional[Dict[str, Any]] = None,
    ) -> tuple[str, dict]:
        """Generates a robust Ray cluster startup script using a head-node-driven architecture.
        The main sbatch script simply launches a driver script on the head node.

        Returns:
            Tuple of (main_script_payload, auxiliary_scripts_dict)

        """
        redis_pw = self.redis_password or "$(uuidgen)"

        # Build object store argument
        obj_store_arg = ""
        if self.object_store_memory_gb is not None:
            bytes_value = int(self.object_store_memory_gb * 1_000_000_000)
            obj_store_arg = f"--object-store-memory={bytes_value}"

        # Determine temp dir path
        temp_dir_path = "/tmp/ray-$SLURM_JOB_ID"
        temp_dir_arg = f"--temp-dir={temp_dir_path}"

        # --- Script for the Worker Nodes ---
        ray_worker_script = f"""#!/bin/bash
  set -e
  # This script is launched by the driver on the head node.
  # Arguments: $1=activation_script, $2=ip_head, $3=redis_password
  activation_script="$1"
  ip_head="$2"
  redis_password="$3"

  echo "Worker on $(hostname) activating environment: $activation_script"
  source "$activation_script"

  cleanup_node() {{ echo "Worker on $(hostname) stopping Ray..."; ray stop -v; rm -rf {temp_dir_path}; exit 0; }}
  trap cleanup_node TERM INT

  echo "Worker on $(hostname) starting and connecting to $ip_head..."
  ray start -v \\
    --address="$ip_head" \\
    --redis-password="$redis_password" \\
    --num-gpus={self.num_gpus_per_node} \\
    {obj_store_arg} \\
    {temp_dir_arg} \\
    --block
  """

        # --- The Main Driver Script (runs on the head node) ---
        ray_driver_script = f"""#!/bin/bash
  set -e
  # Arguments: $1=activation_script
  activation_script="$1"

  echo "======================================="
  echo "Ray Cluster Driver Script Started on $(hostname)"
  echo "Activating environment: $activation_script"
  echo "======================================="
  source "$activation_script"


  cleanup() {{
    echo "Driver script received signal. Stopping Ray cluster..."
    ray stop -v
    wait || true
    rm -rf {temp_dir_path}
    echo "Driver cleanup complete."
  }}
  trap cleanup EXIT SIGINT SIGTERM

  # ===== 1. Start Head Node =====
  ip=$(hostname --ip-address | awk '{{print $1}}')
  port={self.ray_port}
  ip_head="$ip:$port"
  redis_password="{redis_pw}"

  echo "Starting Ray head on this node ($(hostname)) at $ip_head..."
  ray start --head -v \\
    --node-ip-address="$ip" \\
    --port="$port" \\
    --dashboard-host=0.0.0.0 \\
    --dashboard-port={self.dashboard_port} \\
    --num-gpus={self.num_gpus_per_node} \\
    {obj_store_arg} \\
    {temp_dir_arg} \\
    --redis-password="$redis_password"

  # ===== 2. Wait for Head to be Ready =====
  echo "Waiting for local Ray head to be ready..."
  for i in {{1..60}}; do
    if ray status &>/dev/null; then
      echo "Ray head is ready."
      break
    fi
    if [[ $i -eq 60 ]]; then
      echo "ERROR: Local Ray head failed to start in 60 seconds." >&2
      exit 1
    fi
    sleep 1
  done

  # ===== 3. Start Worker Nodes =====
  all_nodes=($(scontrol show hostnames "$SLURM_JOB_NODELIST"))
  head_node_name=$(hostname)
  worker_nodes=()
  for node in "${{all_nodes[@]}}"; do
      if [[ "$node" != "$head_node_name" ]]; then
          worker_nodes+=("$node")
      fi
  done

  echo "Head node is: $head_node_name"
  echo "Worker nodes are: ${{worker_nodes[@]}}"

  for node_i in "${{worker_nodes[@]}}"; do
    echo "Launching worker on $node_i..."
    # Pass the activation script path to the worker script
    srun --export=ALL --nodes=1 --ntasks=1 -w "$node_i" \\
      {working_dir}/ray_worker.sh "$activation_script" "$ip_head" "$redis_password" &
    sleep 1
  done

  echo "All worker launch commands issued. Waiting for registration..."
  sleep 5

  # ===== 4. Verify Cluster and Run Payload =====
  echo "Final cluster status:"
  ray status

  echo "======================================="
  echo "Executing user payload..."
  echo "======================================="

  # The placeholder for the user's python command
  COMMAND_PLACEHOLDER

  exit_code=$?
  echo "======================================="
  echo "User payload finished with exit code $exit_code."
  echo "======================================="
  exit $exit_code
  """
        # **FIX #1**: Replace the plain placeholder string.
        python_command_placeholder = (
            f"{shlex.quote(python_executable)} {shlex.quote(payload_path)}"
        )
        ray_driver_script = ray_driver_script.replace(
            "COMMAND_PLACEHOLDER", python_command_placeholder
        )

        # --- The Main sbatch Script Payload ---
        # This now just launches the driver on the head node, passing the activation script.
        main_sbatch_payload = f"""
  # Find the first node in the allocation to act as the head node.
  nodes=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
  nodes_array=($nodes)
  head_node="${{nodes_array[0]}}"

  echo "Designated head node is: $head_node"
  echo "Launching cluster driver script on head node..."

  # **FIX #2**: Pass the activation script path to the driver.
  srun --nodes=1 --ntasks=1 -w "$head_node" {working_dir}/ray_driver.sh "{activation_script}"
  """

        auxiliary_scripts = {
            "ray_driver.sh": ray_driver_script,
            "ray_worker.sh": ray_worker_script,
        }

        # This architecture is primarily for standalone mode. Session mode would require adaption.
        if allocation_context:
            raise NotImplementedError(
                "This robust driver architecture is designed for standalone sbatch jobs."
            )

        return main_sbatch_payload, auxiliary_scripts
