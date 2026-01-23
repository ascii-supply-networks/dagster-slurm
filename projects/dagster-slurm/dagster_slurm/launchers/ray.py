"""Ray cluster launcher with robust startup/shutdown."""

import shlex
from typing import Any, Dict, Optional, Literal

from pydantic import Field

from dagster_slurm.config.runtime import RuntimeVariant

from .base import ComputeLauncher, ExecutionPlan
import dagster as dg


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
    ray_start_args: list[str] = Field(
        default_factory=list,
        description="Extra arguments to pass to ray start on the head node.",
    )
    redis_password: Optional[str] = Field(
        default=None, description="Redis password (None = auto-generate with uuidgen)"
    )
    ray_port: int = Field(default=6379, description="Ray head port")
    grace_period: int = Field(
        default=5, description="Seconds to wait for graceful shutdown"
    )
    head_startup_timeout: int = Field(
        default=120, description="Seconds to wait for head to be ready"
    )
    worker_startup_delay: int = Field(
        default=1, description="Seconds between worker starts"
    )
    pre_start_commands: list[str] = Field(
        default_factory=list,
        description="Optional shell commands to run before ray start (e.g., ulimit).",
    )
    worker_cpu_bind: Optional[str] = Field(
        default=None,
        description=(
            "Optional value for srun --cpu-bind when starting workers. "
            "Leave unset to inherit Slurm defaults."
        ),
    )
    use_head_ip: bool = Field(
        default=True, description="Use node IP instead of hostname for Ray head."
    )
    dashboard_host: str = Field(
        default="0.0.0.0",
        description="Bind host for Ray dashboard (e.g., 0.0.0.0 or 127.0.0.1).",
    )
    port_strategy: Literal["fixed", "hash_jobid"] = Field(
        default="hash_jobid",
        description="'fixed' or 'hash_jobid' for head/dashboard ports.",
    )

    def _render_override_block(self, date_fmt: str) -> str:
        if not self.pre_start_commands:
            return ""
        lines = []
        for command in self.pre_start_commands:
            lines.append(
                f'echo "[$({date_fmt})] Applying pre-start command: {command}"'
            )
            lines.append(command)
        return "\n    ".join(lines)

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
            # Mode: Connect to existing cluster
            script += f"""# Connect to existing Ray cluster
    export RAY_ADDRESS={shlex.quote(self.ray_address)}
    echo "[$({date_fmt})] Connecting to Ray cluster: {self.ray_address}"
    echo "[$({date_fmt})] Executing payload..."
    """
            if activation_script:
                script += f"source {shlex.quote(activation_script)}\n"
            script += f"{python_command}\n"

        elif allocation_context:
            # Mode: Start cluster in pre-existing allocation (session mode)
            if not activation_script:
                raise ValueError(
                    "activation_script required for multi-node Ray in session mode"
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
            # Mode: Standalone job (single-node or multi-node Slurm)
            script += f"""# Detect Ray mode
    if [[ -n "${{SLURM_JOB_ID:-}}" && "${{SLURM_JOB_NUM_NODES:-1}}" -gt 1 ]]; then
        echo "[$({date_fmt})] Detected multi-node Slurm allocation ($SLURM_JOB_NUM_NODES nodes)"
    """
            if not activation_script:
                script += '    echo "ERROR: activation_script required for multi-node Ray" >&2; exit 1\n'
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
            local_lines = self._generate_local_template(
                date_fmt,
                activation_script,
                working_dir,  # working_dir used for log archival
            )
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
        self, date_fmt: str, activation_script: Optional[str], working_dir: str
    ) -> str:
        """Generate Ray startup for local (single-node) mode."""
        override_block = self._render_override_block(date_fmt)
        if override_block:
            override_block = f"    {override_block}\n"
        # Build object store argument if specified
        obj_store = ""
        if self.object_store_memory_gb is not None:
            bytes_value = self.object_store_memory_gb * 1_000_000_000
            obj_store = f"--object-store-memory={bytes_value}"

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
    # Cross-platform timeout wrapper (macOS doesn't have timeout command)
    run_with_timeout() {{
      local timeout_sec=$1
      shift
      if command -v timeout >/dev/null 2>&1; then
        timeout "$timeout_sec" "$@"
      elif command -v gtimeout >/dev/null 2>&1; then
        gtimeout "$timeout_sec" "$@"
      else
        # Fallback for macOS without coreutils
        ( "$@" ) & local pid=$!
        ( sleep "$timeout_sec"; kill -TERM "$pid" 2>/dev/null ) &
        wait "$pid"
      fi
    }}

    # Compute ports (optionally hash by SLURM_JOB_ID)
    port="{self.ray_port}"
    dash_port="{self.dashboard_port}"
    if [[ "{self.port_strategy}" == "hash_jobid" && -n "${{SLURM_JOB_ID:-}}" ]]; then
        off=$(( SLURM_JOB_ID % 1000 ))
        port=$(( {self.ray_port} + off ))
        dash_port=$(( {self.dashboard_port} + off ))
    fi
    # Resolve head address for local mode (prefer Slurm node IP when available)
    head_bind_addr="127.0.0.1"
    if [[ -n "${{SLURM_JOB_ID:-}}" && "{str(self.use_head_ip).lower()}" == "true" ]]; then
      head_node_name="$(hostname)"
      if command -v getent >/dev/null 2>&1; then
        ipv4=$(getent ahostsv4 "$head_node_name" | awk 'NR==1{{print $1}}')
        if [[ -n "$ipv4" ]]; then
          head_bind_addr="$ipv4"
        else
          ipv6=$(getent ahostsv6 "$head_node_name" | awk 'NR==1{{print $1}}')
          if [[ -n "$ipv6" ]]; then head_bind_addr="$ipv6"; fi
        fi
      elif command -v hostname >/dev/null 2>&1; then
        ipv4=$(hostname -I 2>/dev/null | awk '{{print $1}}')
        if [[ -n "$ipv4" ]]; then head_bind_addr="$ipv4"; fi
      fi
    fi
    head_adv="$head_bind_addr"
    if [[ "$head_adv" == *:* ]]; then head_adv="[$head_adv]"; fi
    # Use /tmp for Ray sockets (Unix socket path limit: 107 bytes)
    # $HOME/dagster_runs paths are too long and cause socket errors
    temp_dir_arg=""
    RAY_TMP_DIR=""  # Global for cleanup function
    if [[ -n "${{SLURM_JOB_ID:-}}" ]]; then
      # Use SLURM_TMPDIR if available (per-job temp dir)
      if [[ -n "${{SLURM_TMPDIR:-}}" ]]; then
        RAY_TMP_DIR="${{SLURM_TMPDIR}}/ray"
        mkdir -p "$RAY_TMP_DIR"
        echo "[$({date_fmt})] Using SLURM_TMPDIR: $RAY_TMP_DIR"
      else
        # Try very short /tmp path first (avoids Unix socket 107-byte path limit)
        SHORT_TMP="/tmp/r${{SLURM_JOB_ID}}"
        if mkdir -p "$SHORT_TMP" 2>/dev/null; then
          RAY_TMP_DIR="$SHORT_TMP"
          echo "[$({date_fmt})] Using /tmp: $RAY_TMP_DIR"
        else
          # /tmp not writable - use $HOME/.r<jobid> (much shorter than working dir)
          RAY_TMP_DIR="$HOME/.r${{SLURM_JOB_ID}}"
          mkdir -p "$RAY_TMP_DIR"
          echo "[$({date_fmt})] Using HOME temp dir: $RAY_TMP_DIR"
        fi
      fi
      export RAY_TMPDIR="$RAY_TMP_DIR"
      temp_dir_arg="--temp-dir=$RAY_TMP_DIR"
      echo "[$({date_fmt})] Ray temp directory: $RAY_TMP_DIR ($(echo -n "$RAY_TMP_DIR" | wc -c) chars)"
    fi
{override_block}    # Start local Ray cluster
    echo "[$({date_fmt})] Starting local Ray cluster"
    # Cleanup function - runs on exit, cancellation, or failure
    # This MUST succeed even if Ray never started or failed
    cleanup_ray() {{
      local exit_code=$?
      echo "[$({date_fmt})] Cleanup triggered (exit code: $exit_code, signal: ${{1:-none}})"

      # Stop Ray with timeout (to prevent hanging during cancellation)
      echo "[$({date_fmt})] Stopping Ray..."
      # Use timeout to prevent ray stop from blocking forever (30 second timeout)
      run_with_timeout 30 ray stop --force 2>&1 || {{
        local ray_stop_exit=$?
        if [[ $ray_stop_exit -eq 124 ]]; then
          echo "[$({date_fmt})] ⚠ Ray stop timed out after 30s, force killing..."
          pkill -9 -f "ray::" 2>&1 || true
        else
          echo "[$({date_fmt})] Ray stop failed with exit code $ray_stop_exit (ignored)"
        fi
      }}
      echo "[$({date_fmt})] ✓ Ray stopped"

      # Stop background log sync
      if [[ -n "${{LOG_SYNC_PID:-}}" ]]; then
        echo "[$({date_fmt})] Stopping background log sync (PID: $LOG_SYNC_PID)..."
        kill -9 "$LOG_SYNC_PID" 2>/dev/null || true
      fi

      # Final sync of any remaining logs
      if [[ -n "$RAY_TMP_DIR" ]] && [[ -d "$RAY_TMP_DIR" ]]; then
        echo "[$({date_fmt})] Final log sync from $RAY_TMP_DIR..."
        for session_dir in "$RAY_TMP_DIR"/session_*/logs; do
          if [[ -d "$session_dir" ]]; then
            session_name=$(basename "$(dirname "$session_dir")")
            target_dir="{working_dir}/ray_logs/$session_name/logs"
            mkdir -p "$target_dir" 2>/dev/null || true
            rsync -a "$session_dir"/ "$target_dir/" 2>/dev/null || true
          fi
        done
      fi

      # Clean up temp directory (best effort)
      if [[ -n "$RAY_TMP_DIR" ]] && [[ -d "$RAY_TMP_DIR" ]]; then
        echo "[$({date_fmt})] Removing $RAY_TMP_DIR..."
        rm -rf "$RAY_TMP_DIR" 2>&1 || true
      fi

      echo "[$({date_fmt})] ✓ Cleanup complete"
    }}

    # Set trap for ALL exit scenarios (normal, error, cancel, kill)
    trap cleanup_ray EXIT SIGINT SIGTERM ERR

    # Start continuous log archival in background (survives scancel better than traps)
    # This ensures logs are preserved even if SIGKILL terminates the main process
    # IMPORTANT: Only sync logs/, not the entire session dir (Ray needs runtime files!)
    ray_logs_archive="{working_dir}/ray_logs"
    mkdir -p "$ray_logs_archive" 2>/dev/null || true

    {{
      while true; do
        if [[ -n "$RAY_TMP_DIR" ]] && [[ -d "$RAY_TMP_DIR" ]]; then
          # Only sync session_*/logs/ directories, not runtime files
          for session_dir in "$RAY_TMP_DIR"/session_*/logs; do
            if [[ -d "$session_dir" ]]; then
              session_name=$(basename "$(dirname "$session_dir")")
              target_dir="$ray_logs_archive/$session_name/logs"
              mkdir -p "$target_dir" 2>/dev/null || true
              # Copy logs (don't remove source - Ray may still write to them)
              rsync -a "$session_dir"/ "$target_dir/" 2>/dev/null || true
            fi
          done
        fi
        sleep 5  # Sync every 5 seconds
      done
    }} &
    LOG_SYNC_PID=$!
    echo "[$({date_fmt})] Started background log sync (PID: $LOG_SYNC_PID)"

    # Start Ray head
    unset RAY_ADDRESS 2>/dev/null || true
    export RAY_DASHBOARD_ADDRESS="http://$head_adv:$dash_port"
    export RAY_NODE_IP_ADDRESS="$head_bind_addr"
    ray start --head --port=$port --node-ip-address="$head_bind_addr" \
        --dashboard-host={self.dashboard_host} --dashboard-port=$dash_port \
        --num-gpus={self.num_gpus_per_node} {obj_store} {(" " + " ".join(self.ray_start_args)) if self.ray_start_args else ""} $temp_dir_arg
    export RAY_ADDRESS="$head_adv:$port"
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
        activation_script: str,
        allocation_context: Optional[Dict[str, Any]] = None,
    ) -> tuple[str, dict]:
        """
        Generates a robust Ray cluster startup script with proper shutdown.
        """
        redis_pw = self.redis_password or "$(uuidgen)"

        common_args = []
        if self.object_store_memory_gb is not None:
            # must end with space for correct command formatting
            bytes_value = int(self.object_store_memory_gb * 1_000_000_000)
            common_args.append(f"--object-store-memory={bytes_value}")

        if self.worker_cpu_bind is not None:
            if self.worker_cpu_bind == "_none_":
                # If we see our special string, use the literal 'none'
                cpu_bind_option = "--cpu-bind=none "
            else:
                # Otherwise, use the string value directly
                cpu_bind_option = f"--cpu-bind={self.worker_cpu_bind} "
        else:
            cpu_bind_option = ""

        if cpu_bind_option != "":
            dg.get_dagster_logger().info(f"Using CPU bind of: {cpu_bind_option}")

        head_args = (
            [
                "--head",
                "-v",
                "--node-ip-address=$head_bind_addr",
                "--port=$port",
                f"--dashboard-host={self.dashboard_host}",
                "--dashboard-port=$dash_port",
                f"--num-gpus={self.num_gpus_per_node}",
                "--redis-password=$redis_password",
                f"--temp-dir=$RAY_CLUSTER_TMP",
            ]
            + common_args
            + self.ray_start_args
        )

        worker_args = [
            "-v",
            "--address=$ip_head",
            "--redis-password=$redis_password",
            f"--num-gpus={self.num_gpus_per_node}",
            "--temp-dir=$RAY_CLUSTER_TMP",
        ] + common_args

        head_cmd_str = " \\\n    ".join(head_args)
        worker_cmd_str = " \\\n    ".join(worker_args)

        # --- Worker Script ---
        ray_worker_script = f"""#!/bin/bash
    set -e
    activation_script="$1"
    ip_head="$2"
    redis_password="$3"
    echo "Worker on $(hostname) activating environment: $activation_script"
    source "$activation_script"
    {self._render_override_block(date_fmt)}
    # Cross-platform timeout wrapper
    run_with_timeout() {{
      local timeout_sec=$1
      shift
      if command -v timeout >/dev/null 2>&1; then
        timeout "$timeout_sec" "$@"
      elif command -v gtimeout >/dev/null 2>&1; then
        gtimeout "$timeout_sec" "$@"
      else
        ( "$@" ) & local pid=$!
        ( sleep "$timeout_sec"; kill -TERM "$pid" 2>/dev/null ) &
        wait "$pid"
      fi
    }}
    cleanup_node() {{
        echo "[$({date_fmt})] Worker on $(hostname) shutting down..."

        # Stop background log sync
        if [[ -n "${{WORKER_LOG_SYNC_PID:-}}" ]]; then
            echo "[$({date_fmt})] Stopping background log sync (PID: $WORKER_LOG_SYNC_PID)..."
            kill -9 "$WORKER_LOG_SYNC_PID" 2>/dev/null || true
        fi

        # Final sync of worker logs
        worker_logs_dir="{working_dir}/ray_logs/worker_$(hostname)"
        for session_dir in "$RAY_CLUSTER_TMP"/session_*/logs; do
          if [[ -d "$session_dir" ]]; then
            session_name=$(basename "$(dirname "$session_dir")")
            target_dir="$worker_logs_dir/$session_name/logs"
            if mkdir -p "$target_dir" 2>/dev/null; then
              echo "[$({date_fmt})] Final worker log sync to $target_dir..."
              rsync -a "$session_dir"/ "$target_dir/" 2>&1 || true
              echo "[$({date_fmt})] ✓ Worker logs synced"
            else
              echo "[$({date_fmt})] ⚠ Cannot access $target_dir, logs not archived"
            fi
          fi
        done

        # Stop Ray
        run_with_timeout 30 ray stop --force 2>/dev/null || true

        # Cleanup temp dir
        rm -rf $RAY_CLUSTER_TMP 2>/dev/null || true
        echo "[$({date_fmt})] ✓ Worker cleanup complete"
        exit 0
    }}
    trap cleanup_node TERM INT EXIT ERR

    # Start continuous log sync in background for worker
    # Only sync logs/, not runtime files
    worker_logs_dir="{working_dir}/ray_logs/worker_$(hostname)"
    mkdir -p "$worker_logs_dir" 2>/dev/null || true

    {{
      while true; do
        for session_dir in "$RAY_CLUSTER_TMP"/session_*/logs; do
          if [[ -d "$session_dir" ]]; then
            session_name=$(basename "$(dirname "$session_dir")")
            target_dir="$worker_logs_dir/$session_name/logs"
            mkdir -p "$target_dir" 2>/dev/null || true
            rsync -a "$session_dir"/ "$target_dir/" 2>/dev/null || true
          fi
        done
        sleep 5
      done
    }} &
    WORKER_LOG_SYNC_PID=$!
    echo "[$({date_fmt})] Started background worker log sync (PID: $WORKER_LOG_SYNC_PID)"

    # Determine Ray temp directory (short path to avoid 107-byte socket limit)
    if [[ -n "${{SLURM_TMPDIR:-}}" ]]; then
        export RAY_CLUSTER_TMP="${{SLURM_TMPDIR}}/r$SLURM_JOB_ID"
        echo "[$({date_fmt})] Using SLURM_TMPDIR for Ray"
    elif mkdir -p "/tmp/r$SLURM_JOB_ID" 2>/dev/null; then
        export RAY_CLUSTER_TMP="/tmp/r$SLURM_JOB_ID"
        echo "[$({date_fmt})] Using /tmp for Ray"
    else
        export RAY_CLUSTER_TMP="$HOME/.r$SLURM_JOB_ID"
        echo "[$({date_fmt})] Using HOME for Ray"
    fi
    echo "[$({date_fmt})] Ray temp directory: $RAY_CLUSTER_TMP ($(echo -n "$RAY_CLUSTER_TMP" | wc -c) chars)"
    mkdir -p "$RAY_CLUSTER_TMP"

    echo "Worker on $(hostname) starting and connecting to $ip_head..."
    ray start {worker_cmd_str} --block
    """

        ray_driver_script = f"""#!/bin/bash
    set -e
    activation_script="$1"
    echo "======================================="
    echo "Ray Cluster Driver Script Started on $(hostname)"
    echo "Activating environment: $activation_script"
    echo "======================================="
    source "$activation_script"
    {self._render_override_block(date_fmt)}
    # Cross-platform timeout wrapper
    run_with_timeout() {{
      local timeout_sec=$1
      shift
      if command -v timeout >/dev/null 2>&1; then
        timeout "$timeout_sec" "$@"
      elif command -v gtimeout >/dev/null 2>&1; then
        gtimeout "$timeout_sec" "$@"
      else
        ( "$@" ) & local pid=$!
        ( sleep "$timeout_sec"; kill -TERM "$pid" 2>/dev/null ) &
        wait "$pid"
      fi
    }}

    # Define all variables first
    # Figure out ports 
    port="{self.ray_port}"
    dash_port="{self.dashboard_port}"
    if [[ "{self.port_strategy}" == "hash_jobid" && -n "${{SLURM_JOB_ID:-}}" ]]; then
        # keep in user space; avoid reserved/system ports 
        off=$(( SLURM_JOB_ID % 1000 ))
        port=$(( {self.ray_port} + off ))
        dash_port=$(( {self.dashboard_port} + off ))
    fi
    
    # Choose head node (first host in allocation)
    head_node_name=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -n1)
    # Resolve what Ray should BIND to (and what workers should CONNECT to)
    head_bind_addr="$head_node_name"
    if [[ "{str(self.use_head_ip).lower()}" == "true" ]]; then
      # Prefer IPv4; fall back to IPv6; finally fall back to hostname
      ipv4=$(getent ahostsv4 "$head_node_name" | awk 'NR==1{{print $1}}')
      if [[ -n "$ipv4" ]]; then
          head_bind_addr="$ipv4"
      else
          ipv6=$(getent ahostsv6 "$head_node_name" | awk 'NR==1{{print $1}}')
          if [[ -n "$ipv6" ]]; then head_bind_addr="$ipv6"; fi
      fi
    fi
    # Bracketize IPv6 for Ray's --address / RAY_ADDRESS usage 
    head_adv="$head_bind_addr"
    if [[ "$head_adv" == *:* ]]; then head_adv="[$head_adv]"; fi
    ip_head="$head_adv:$port"
    unset RAY_ADDRESS 2>/dev/null || true
    export RAY_ADDRESS="$ip_head"
    export RAY_NODE_IP_ADDRESS="$head_bind_addr"
    export RAY_DASHBOARD_ADDRESS="http://$head_adv:$dash_port"

    redis_password="{redis_pw}"
    WORKER_PIDS=()
    worker_nodes=()

    cleanup() {{
        exit_code=$?
        echo "======================================="
        echo "[$({date_fmt})] Initiating cluster shutdown (payload exit code: $exit_code, signal: ${{1:-none}})..."
        echo "======================================="

        # Capture error logs if job failed
        if [[ "$exit_code" -ne 0 ]]; then
            echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" >&2
            echo "PAYLOAD FAILED OR SCRIPT EXITED UNEXPECTEDLY! Capturing logs..." >&2
            for node in "${{worker_nodes[@]}}"; do
                echo "--- WORKER NODE ($node) RAYLET LOG ---" >&2
                srun --nodes=1 --ntasks=1 -w "$node" bash -c 'tail -n 50 $(find $RAY_CLUSTER_TMP/session_*/logs/raylet.out -type f 2>/dev/null | sort | tail -n 1)' || echo "Worker log on $node not found." >&2
            done
            echo "--- HEAD NODE ($(hostname)) RAYLET LOG ---" >&2
            tail -n 50 $(find $RAY_CLUSTER_TMP/session_*/logs/raylet.out -type f 2>/dev/null | sort | tail -n 1) || echo "Head raylet log not found." >&2
            echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" >&2
        fi

        # Terminate worker processes
        if [ ${{#WORKER_PIDS[@]}} -gt 0 ]; then
            echo "[$({date_fmt})] Terminating ${{#WORKER_PIDS[@]}} worker srun process(es)..."
            kill -TERM "${{WORKER_PIDS[@]}}" 2>/dev/null || true
            sleep 2
            kill -9 "${{WORKER_PIDS[@]}}" 2>/dev/null || true
        fi

        # Stop Ray head with timeout
        echo "[$({date_fmt})] Force stopping Ray head node..."
        run_with_timeout 30 ray stop --force 2>/dev/null || {{
            echo "[$({date_fmt})] ⚠ Ray stop timed out, force killing..."
            pkill -9 -f "ray::" 2>/dev/null || true
        }}

        # Stop background log sync
        if [[ -n "${{LOG_SYNC_PID:-}}" ]]; then
            echo "[$({date_fmt})] Stopping background log sync (PID: $LOG_SYNC_PID)..."
            kill -9 "$LOG_SYNC_PID" 2>/dev/null || true
        fi

        # Final sync of any remaining logs
        echo "[$({date_fmt})] Final log sync from $RAY_CLUSTER_TMP..."
        for session_dir in "$RAY_CLUSTER_TMP"/session_*/logs; do
          if [[ -d "$session_dir" ]]; then
            session_name=$(basename "$(dirname "$session_dir")")
            target_dir="{working_dir}/ray_logs/$session_name/logs"
            mkdir -p "$target_dir" 2>/dev/null || true
            rsync -a "$session_dir"/ "$target_dir/" 2>/dev/null || true
          fi
        done

        echo "[$({date_fmt})] Cleaning up temporary files..."
        rm -rf $RAY_CLUSTER_TMP 2>/dev/null || true
        echo "[$({date_fmt})] ✓ Shutdown complete"
    }}
    trap cleanup EXIT SIGINT SIGTERM ERR

    # Start continuous log archival in background (survives scancel)
    # Only sync logs/, not runtime files
    ray_logs_archive="{working_dir}/ray_logs"
    mkdir -p "$ray_logs_archive" 2>/dev/null || true

    {{
      while true; do
        for session_dir in "$RAY_CLUSTER_TMP"/session_*/logs; do
          if [[ -d "$session_dir" ]]; then
            session_name=$(basename "$(dirname "$session_dir")")
            target_dir="$ray_logs_archive/$session_name/logs"
            mkdir -p "$target_dir" 2>/dev/null || true
            rsync -a "$session_dir"/ "$target_dir/" 2>/dev/null || true
          fi
        done
        sleep 5
      done
    }} &
    LOG_SYNC_PID=$!
    echo "[$({date_fmt})] Started background log sync (PID: $LOG_SYNC_PID)"

    # Determine Ray temp directory (short path to avoid 107-byte socket limit)
    if [[ -n "${{SLURM_TMPDIR:-}}" ]]; then
        export RAY_CLUSTER_TMP="${{SLURM_TMPDIR}}/r$SLURM_JOB_ID"
        echo "[$({date_fmt})] Using SLURM_TMPDIR for Ray"
    elif mkdir -p "/tmp/r$SLURM_JOB_ID" 2>/dev/null; then
        export RAY_CLUSTER_TMP="/tmp/r$SLURM_JOB_ID"
        echo "[$({date_fmt})] Using /tmp for Ray"
    else
        export RAY_CLUSTER_TMP="$HOME/.r$SLURM_JOB_ID"
        echo "[$({date_fmt})] Using HOME for Ray"
    fi
    echo "[$({date_fmt})] Ray temp directory: $RAY_CLUSTER_TMP ($(echo -n "$RAY_CLUSTER_TMP" | wc -c) chars)"
    mkdir -p "$RAY_CLUSTER_TMP"

    # ===== 1. Start Head Node =====
    echo "[$({date_fmt})] Starting Ray head on this node ($(hostname)) at $ip_head..."
    ray start {head_cmd_str}
    export RAY_ADDRESS="$ip_head"

    # ===== 2. Wait for Head to be Ready =====
    echo "Waiting for Ray head to be ready..."
    for i in {{1..{self.head_startup_timeout}}}; do
        if ray status &>/dev/null; then echo "✓ Ray head is ready"; break; fi
        if [[ $i -eq {self.head_startup_timeout} ]]; then echo "ERROR: Ray head failed to start" >&2; exit 1; fi
        sleep 1
    done

    # ===== 3. Start Worker Nodes =====
    all_nodes=($(scontrol show hostnames "$SLURM_JOB_NODELIST"))
    for node in "${{all_nodes[@]}}"; do
        if [[ "$node" != "$head_node_name" ]]; then worker_nodes+=("$node"); fi
    done
    echo "Head node: $head_node_name"; echo "Worker nodes: ${{worker_nodes[@]}}"
    for node_i in "${{worker_nodes[@]}}"; do
        echo "Launching worker on $node_i..."
        srun {cpu_bind_option}--nodes=1 --ntasks=1 -w "$node_i" \\
            {working_dir}/ray_worker.sh "$activation_script" "$ip_head" "$redis_password" &
        WORKER_PIDS+=($!)
        sleep {self.worker_startup_delay}
    done

    # ===== 4. Wait for All Workers to Register =====
    echo "Waiting briefly for worker processes to launch..."
    sleep 5 # Give workers a few seconds to start or fail
    for pid in "${{WORKER_PIDS[@]}}"; do
        # 'kill -0' checks if the process exists. If it doesn't, kill returns a non-zero exit code.
        if ! kill -0 $pid 2>/dev/null; then
            echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" >&2
            echo "ERROR: A worker process (PID $pid) died immediately after launch." >&2
            echo "This almost certainly means the 'ray start' command on the worker node failed." >&2
            echo "Check slurm-<jobid>.err for errors from the worker node." >&2
            echo "The most likely cause is a network issue preventing the worker from reaching the head." >&2
            echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" >&2
            exit 1
        fi
    done
    echo "✓ All worker processes are running. Now checking for Ray registration..."

    expected_nodes=${{SLURM_JOB_NUM_NODES:-1}}
    echo "Waiting for all $expected_nodes nodes to register..."
    for i in {{1..36}}; do
        # This is the robust way: capture output first, then grep it.
        # This prevents grep's non-zero exit code from triggering 'set -e'.
        status_output=$(ray status 2>/dev/null || echo "ray status failed")
        live_nodes=$(echo "$status_output" | grep -c "node_")

        if [[ "$live_nodes" -ge "$expected_nodes" ]]; then
            echo "✓ Success! $live_nodes of $expected_nodes nodes are active."
            break
        fi
        echo "-> Waiting: $live_nodes of $expected_nodes nodes active. Retrying in 5s..."
        sleep 5
        if [[ $i -eq 36 ]]; then
            echo "ERROR: Cluster did not come up within 3 minutes." >&2
            echo "$status_output" >&2
            exit 1
        fi
    done

    # ===== 5. Run Payload =====
    echo "Executing user payload..."
    export RAY_NODE_IP_ADDRESS="$head_bind_addr"
    {shlex.quote(python_executable)} {shlex.quote(payload_path)}
    """

        # --- Main sbatch payload (unchanged) ---
        main_sbatch_payload = f"""
    nodes=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
    nodes_array=($nodes)
    head_node="${{nodes_array[0]}}"
    echo "Designated head node: $head_node"
    srun --nodes=1 --ntasks=1 -w "$head_node" {working_dir}/ray_driver.sh "{activation_script}"
    """
        auxiliary_scripts = {
            "ray_driver.sh": ray_driver_script,
            "ray_worker.sh": ray_worker_script,
        }

        if allocation_context:
            raise NotImplementedError("This architecture is for standalone sbatch jobs")

        return main_sbatch_payload, auxiliary_scripts
