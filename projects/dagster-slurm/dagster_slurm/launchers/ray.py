"""Ray cluster launcher with robust startup/shutdown."""

import shlex
from pathlib import Path
from typing import Any, Dict, Literal, Optional

from pydantic import Field, model_validator

from dagster_slurm.config.runtime import RuntimeVariant
from dagster_slurm.helpers.ray_dashboard import RAY_DASHBOARD_URL_MARKER

from .base import ComputeLauncher, ExecutionPlan
import dagster as dg


class RayPortConfig(dg.Config):
    """Port pool and fixed-port settings for a Ray cluster."""

    range_start: int = Field(
        default=10000,
        ge=1,
        le=65535,
        description="First port available to job-specific port blocks.",
    )
    range_end: int = Field(
        default=29999,
        ge=1,
        le=65535,
        description="Last port available to job-specific port blocks.",
    )
    block_size: int = Field(
        default=1000,
        ge=17,
        le=65535,
        description=(
            "Ports reserved per concurrent Ray cluster. Sixteen ports are "
            "reserved for services; the remainder is the worker port range."
        ),
    )
    lock_dir: str = Field(
        default="/tmp/dagster-slurm-ray-ports",
        min_length=1,
        description="Node-local lock directory shared by concurrent Slurm users.",
    )
    node_manager_port: int = Field(default=6700, ge=1, le=65535)
    object_manager_port: int = Field(default=6701, ge=1, le=65535)
    redis_shard_port: int = Field(default=6702, ge=1, le=65535)
    runtime_env_agent_port: int = Field(default=6703, ge=1, le=65535)
    dashboard_agent_grpc_port: int = Field(default=6704, ge=1, le=65535)
    dashboard_agent_listen_port: int = Field(default=6705, ge=1, le=65535)
    metrics_export_port: int = Field(default=6706, ge=1, le=65535)
    ray_client_server_port: int = Field(default=10001, ge=1, le=65535)
    min_worker_port: int = Field(default=10002, ge=1, le=65535)
    max_worker_port: int = Field(default=19999, ge=1, le=65535)

    @model_validator(mode="after")
    def validate_port_ranges(self):
        if self.range_end < self.range_start:
            raise ValueError("range_end must be greater than or equal to range_start")
        range_size = self.range_end - self.range_start + 1
        if range_size < self.block_size:
            raise ValueError(
                "Ray port range must contain at least one complete port block"
            )
        if self.min_worker_port > self.max_worker_port:
            raise ValueError(
                "min_worker_port must be less than or equal to max_worker_port"
            )
        return self


def _render_ray_port_assignments(
    *,
    ray_port: int,
    dashboard_port: int,
    port_strategy: str,
    port_config: RayPortConfig,
) -> str:
    """Render collision-resistant ports for every Ray service on a node."""
    block_count = (
        port_config.range_end - port_config.range_start + 1
    ) // port_config.block_size
    return f"""# Assign every Ray service from one job-specific port block.
port="{ray_port}"
dash_port="{dashboard_port}"
node_manager_port="{port_config.node_manager_port}"
object_manager_port="{port_config.object_manager_port}"
redis_shard_port="{port_config.redis_shard_port}"
runtime_env_agent_port="{port_config.runtime_env_agent_port}"
dashboard_agent_grpc_port="{port_config.dashboard_agent_grpc_port}"
dashboard_agent_listen_port="{port_config.dashboard_agent_listen_port}"
metrics_export_port="{port_config.metrics_export_port}"
ray_client_server_port="{port_config.ray_client_server_port}"
min_worker_port="{port_config.min_worker_port}"
max_worker_port="{port_config.max_worker_port}"
_ray_port_lock_fd=""
if [[ "{port_strategy}" == "random" ]]; then
    if ! command -v flock >/dev/null 2>&1; then
        echo "ERROR: port_strategy=random requires flock (util-linux)" >&2
        exit 1
    fi
    _port_lock_root={shlex.quote(port_config.lock_dir)}
    if [[ ! -d "$_port_lock_root" ]]; then
        if (umask 000; mkdir "$_port_lock_root") 2>/dev/null; then
            chmod 1777 "$_port_lock_root"
        elif [[ ! -d "$_port_lock_root" ]]; then
            echo "ERROR: Cannot create Ray port lock directory $_port_lock_root" >&2
            exit 1
        fi
    fi
    if [[ ! -r "$_port_lock_root" || ! -w "$_port_lock_root" || ! -x "$_port_lock_root" ]]; then
        echo "ERROR: Ray port lock directory is not accessible: $_port_lock_root" >&2
        exit 1
    fi
    ray_port_block_in_use() {{
        local block_start="$1"
        local block_end="$2"
        command -v ss >/dev/null 2>&1 || return 1
        ss -H -lntu 2>/dev/null | awk \
            -v block_start="$block_start" \
            -v block_end="$block_end" '
                {{
                    local_address = $5
                    sub(/^.*:/, "", local_address)
                    if (local_address ~ /^[0-9]+$/ && local_address + 0 >= block_start && local_address + 0 <= block_end) {{
                        found = 1
                    }}
                }}
                END {{ exit(found ? 0 : 1) }}
            '
    }}
    _port_seed="${{RAY_PORT_SEED:-}}"
    if [[ -z "$_port_seed" ]]; then
        _port_seed="$(od -An -N4 -tu4 /dev/urandom 2>/dev/null | tr -d ' ')"
    fi
    if [[ -z "$_port_seed" ]]; then
        _port_seed="$(date +%s%N | cksum)"
        _port_seed="${{_port_seed%% *}}"
    fi
    if [[ ! "$_port_seed" =~ ^[0-9]+$ ]]; then
        _port_seed="$(printf '%s' "$_port_seed" | cksum)"
        _port_seed="${{_port_seed%% *}}"
    fi
    for ((_port_attempt = 0; _port_attempt < {block_count}; _port_attempt++)); do
        _port_slot=$(( (_port_seed + _port_attempt) % {block_count} ))
        _port_candidate=$(( {port_config.range_start} + (_port_slot * {port_config.block_size}) ))
        _port_candidate_end=$(( _port_candidate + {port_config.block_size - 1} ))
        # The directory is a stable inode for flock, not persisted claim state.
        # Never remove it: the kernel releases the claim when the FD closes.
        _candidate_lock="$_port_lock_root/block-${{_port_candidate}}-${{_port_candidate_end}}.lock.d"
        if [[ ! -d "$_candidate_lock" ]]; then
            if (umask 000; mkdir "$_candidate_lock") 2>/dev/null; then
                chmod 0555 "$_candidate_lock"
            elif [[ ! -d "$_candidate_lock" ]]; then
                continue
            fi
        fi
        [[ ! -L "$_candidate_lock" ]] || continue
        if ! exec {{_candidate_lock_fd}}<"$_candidate_lock"; then
            continue
        fi
        if flock -n "$_candidate_lock_fd"; then
            if ray_port_block_in_use "$_port_candidate" "$_port_candidate_end"; then
                flock -u "$_candidate_lock_fd"
                exec {{_candidate_lock_fd}}<&-
                continue
            fi
            _ray_port_lock_fd="$_candidate_lock_fd"
            _port_base="$_port_candidate"
            break
        fi
        exec {{_candidate_lock_fd}}<&-
    done
    if [[ -z "$_ray_port_lock_fd" ]]; then
        echo "ERROR: No free Ray port block in {port_config.range_start}-{port_config.range_end}" >&2
        exit 1
    fi
elif [[ "{port_strategy}" == "hash_jobid" ]]; then
    _port_seed="${{RAY_PORT_SEED:-${{SLURM_JOB_ID:-$$}}}}"
    if [[ ! "$_port_seed" =~ ^[0-9]+$ ]]; then
        _port_seed="$(printf '%s' "$_port_seed" | cksum)"
        _port_seed="${{_port_seed%% *}}"
    fi
    _port_slot=$(( _port_seed % {block_count} ))
    _port_base=$(( {port_config.range_start} + (_port_slot * {port_config.block_size}) ))
fi
if [[ "{port_strategy}" != "fixed" ]]; then
    port="$_port_base"
    node_manager_port=$(( _port_base + 1 ))
    object_manager_port=$(( _port_base + 2 ))
    ray_client_server_port=$(( _port_base + 3 ))
    redis_shard_port=$(( _port_base + 4 ))
    dash_port=$(( _port_base + 5 ))
    runtime_env_agent_port=$(( _port_base + 6 ))
    dashboard_agent_grpc_port=$(( _port_base + 7 ))
    dashboard_agent_listen_port=$(( _port_base + 8 ))
    metrics_export_port=$(( _port_base + 9 ))
    min_worker_port=$(( _port_base + 16 ))
    max_worker_port=$(( _port_base + {port_config.block_size - 1} ))
fi"""


def _render_ray_process_cleanup(grace_period: int) -> str:
    """Render cleanup that stops one ``ray start --block`` process tree."""
    return f"""stop_ray_process() {{
    local ray_pid="${{1:-}}"
    if [[ -n "$ray_pid" ]] && kill -0 "$ray_pid" 2>/dev/null; then
        kill -TERM "$ray_pid" 2>/dev/null || true
        for ((ray_wait = 0; ray_wait < {grace_period}; ray_wait++)); do
            if ! kill -0 "$ray_pid" 2>/dev/null; then break; fi
            sleep 1
        done
        if kill -0 "$ray_pid" 2>/dev/null; then
            kill -KILL "$ray_pid" 2>/dev/null || true
        fi
        wait "$ray_pid" 2>/dev/null || true
    fi
    if [[ -n "${{_ray_port_lock_fd:-}}" ]]; then
        flock -u "$_ray_port_lock_fd" 2>/dev/null || true
        exec {{_ray_port_lock_fd}}<&-
        _ray_port_lock_fd=""
    fi
}}"""


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
    dashboard_port: int = Field(
        default=8265,
        ge=1,
        le=65535,
        description="Ray dashboard port for the fixed strategy.",
    )
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
    ray_port: int = Field(
        default=6379,
        ge=1,
        le=65535,
        description="Ray GCS/head port for the fixed strategy.",
    )
    grace_period: int = Field(
        default=5,
        ge=0,
        description="Seconds to wait for graceful shutdown",
    )
    head_startup_timeout: int = Field(
        default=120,
        ge=1,
        description="Seconds to wait for head to be ready",
    )
    worker_startup_delay: int = Field(
        default=1, description="Seconds between worker starts"
    )
    pre_start_commands: list[str] = Field(
        default_factory=list,
        description="Optional shell commands to run before ray start (e.g., ulimit).",
    )
    worker_cpu_bind: Optional[str] = Field(
        default="none",
        description=(
            "Value for srun --cpu-bind when starting workers. "
            "Default 'none' disables CPU binding to avoid conflicts with job allocation. "
            "Other values: 'cores', 'threads', 'sockets', etc. "
            "Set to Python None (not the string) to omit the flag entirely."
        ),
    )
    use_head_ip: bool = Field(
        default=True, description="Use node IP instead of hostname for Ray head."
    )
    dashboard_host: str = Field(
        default="0.0.0.0",
        description="Bind host for Ray dashboard (e.g., 0.0.0.0 or 127.0.0.1).",
    )
    port_strategy: Literal["fixed", "hash_jobid", "random"] = Field(
        default="random",
        description=(
            "'random' claims an available random per-node port block; 'fixed' uses "
            "fixed ports; 'hash_jobid' deterministically selects a block from "
            "RAY_PORT_SEED, SLURM_JOB_ID, or the local PID."
        ),
    )
    port_config: RayPortConfig = Field(
        default_factory=RayPortConfig,
        description="Allowed port pool and fixed service ports.",
    )
    network_interface: Optional[str] = Field(
        default=None,
        description=(
            "Network interface to bind on every Ray node, such as 'ib0'. "
            "Ignored when node_ip_address_command is set."
        ),
    )
    node_ip_address_command: Optional[str] = Field(
        default=None,
        description=(
            "Shell command run on each Ray node to resolve its bind IP. "
            "Use for clusters whose interface names differ between nodes."
        ),
    )

    @model_validator(mode="after")
    def validate_fixed_ports(self):
        if self.port_strategy != "fixed":
            return self
        service_ports = {
            "ray_port": self.ray_port,
            "dashboard_port": self.dashboard_port,
            "node_manager_port": self.port_config.node_manager_port,
            "object_manager_port": self.port_config.object_manager_port,
            "redis_shard_port": self.port_config.redis_shard_port,
            "runtime_env_agent_port": self.port_config.runtime_env_agent_port,
            "dashboard_agent_grpc_port": self.port_config.dashboard_agent_grpc_port,
            "dashboard_agent_listen_port": (
                self.port_config.dashboard_agent_listen_port
            ),
            "metrics_export_port": self.port_config.metrics_export_port,
            "ray_client_server_port": self.port_config.ray_client_server_port,
        }
        if len(set(service_ports.values())) != len(service_ports):
            raise ValueError("Fixed Ray service ports must be unique")
        worker_range = range(
            self.port_config.min_worker_port,
            self.port_config.max_worker_port + 1,
        )
        overlapping_services = [
            name for name, port in service_ports.items() if port in worker_range
        ]
        if overlapping_services:
            raise ValueError(
                "Fixed Ray service ports overlap the worker range: "
                + ", ".join(overlapping_services)
            )
        return self

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

    @staticmethod
    def _library_path_export(python_executable: str) -> str:
        env_lib = Path(python_executable).parent.parent / "lib"
        return f"export LD_LIBRARY_PATH={shlex.quote(str(env_lib))}:${{LD_LIBRARY_PATH:-}}\n"

    def _render_node_ip_override(self, variable_name: str) -> str:
        if not variable_name.isidentifier():
            raise ValueError(f"Invalid shell variable name: {variable_name!r}")
        if self.node_ip_address_command:
            assignment = (
                f'{variable_name}="$({self.node_ip_address_command})"\n'
                f'{variable_name}="${{{variable_name}%%[[:space:]]*}}"'
            )
        elif self.network_interface:
            interface = shlex.quote(self.network_interface)
            assignment = (
                f'{variable_name}="$(ip -o -4 address show dev {interface} '
                '| awk \'NR==1 {sub(/\\\\/.*/, "", $4); print $4}\')"\n'
                f'if [[ -z "${{{variable_name}}}" ]]; then\n'
                f'  {variable_name}="$(ip -o -6 address show dev {interface} '
                '| awk \'NR==1 {sub(/\\\\/.*/, "", $4); print $4}\')"\n'
                "fi"
            )
        else:
            return ""
        return (
            f"{assignment}\n"
            f'if [[ -z "${{{variable_name}}}" ]]; then\n'
            '  echo "ERROR: Ray node IP resolution returned no address" >&2\n'
            "  exit 1\n"
            "fi"
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
            # Mode: Connect to existing cluster
            script += f"""# Connect to existing Ray cluster
    export RAY_ADDRESS={shlex.quote(self.ray_address)}
    echo "[$({date_fmt})] Connecting to Ray cluster: {self.ray_address}"
    echo "[$({date_fmt})] Executing payload..."
    """
            if activation_script:
                script += f"source {shlex.quote(activation_script)}\n"
                script += self._library_path_export(python_executable)
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
                python_executable,
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
        self,
        date_fmt: str,
        activation_script: Optional[str],
        working_dir: str,
        python_executable: str,
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
    {self._library_path_export(python_executable).strip()}
    echo "[$({date_fmt})] Environment activated."
    """
        port_assignments = _render_ray_port_assignments(
            ray_port=self.ray_port,
            dashboard_port=self.dashboard_port,
            port_strategy=self.port_strategy,
            port_config=self.port_config,
        )
        node_ip_override = self._render_node_ip_override("head_bind_addr")
        # The rest of the function remains the same
        return f"""{activation_block}
    {port_assignments}
    {_render_ray_process_cleanup(self.grace_period)}
    # Resolve head address for local mode.
    # Always attempt IP resolution when use_head_ip=true, not just under Slurm.
    # In Docker/CI, Ray detects the container IP internally but if we bind to
    # 127.0.0.1 the GCS client cannot connect (it tries the container IP).
    head_bind_addr="127.0.0.1"
    if [[ "{str(self.use_head_ip).lower()}" == "true" ]]; then
      head_node_name="$(hostname)"
      if command -v getent >/dev/null 2>&1; then
        ipv4=$(getent ahostsv4 "$head_node_name" | awk 'NR==1{{print $1}}' || true)
        if [[ -n "$ipv4" ]]; then
          head_bind_addr="$ipv4"
        else
          ipv6=$(getent ahostsv6 "$head_node_name" | awk 'NR==1{{print $1}}' || true)
          if [[ -n "$ipv6" ]]; then head_bind_addr="$ipv6"; fi
        fi
      elif command -v hostname >/dev/null 2>&1; then
        ipv4=$(hostname -I 2>/dev/null | awk '{{print $1}}' || true)
        if [[ -n "$ipv4" ]]; then head_bind_addr="$ipv4"; fi
      fi
    fi
    {node_ip_override}
    head_adv="$head_bind_addr"
    if [[ "$head_adv" == *:* ]]; then head_adv="[$head_adv]"; fi
    # Use short path for Ray sockets (Unix socket path limit: 107 bytes)
    # Must be node-local storage - Unix sockets don't work over NFS
    temp_dir_arg=""
    RAY_TMP_DIR=""  # Global for cleanup function
    _ray_instance="${{_port_base:-$port}}"
    if [[ -n "${{SLURM_JOB_ID:-}}" ]]; then
      # Use SLURM_TMPDIR if available (per-job temp dir)
      if [[ -n "${{SLURM_TMPDIR:-}}" ]]; then
        RAY_TMP_DIR="${{SLURM_TMPDIR}}/ray-${{_ray_instance}}"
        mkdir -p "$RAY_TMP_DIR"
        echo "[$({date_fmt})] Using SLURM_TMPDIR: $RAY_TMP_DIR (node-local)"
      elif mkdir -p "/tmp/r${{SLURM_JOB_ID}}-${{_ray_instance}}" 2>/dev/null; then
        RAY_TMP_DIR="/tmp/r${{SLURM_JOB_ID}}-${{_ray_instance}}"
        echo "[$({date_fmt})] Using /tmp: $RAY_TMP_DIR (node-local)"
      elif mkdir -p "/var/tmp/r${{SLURM_JOB_ID}}-${{_ray_instance}}" 2>/dev/null; then
        RAY_TMP_DIR="/var/tmp/r${{SLURM_JOB_ID}}-${{_ray_instance}}"
        echo "[$({date_fmt})] Using /var/tmp: $RAY_TMP_DIR (node-local)"
      else
        RAY_TMP_DIR="$HOME/.r${{SLURM_JOB_ID}}-${{_ray_instance}}"
        mkdir -p "$RAY_TMP_DIR"
        echo "[$({
            date_fmt
        })] WARNING: Using HOME: $RAY_TMP_DIR - may fail if shared filesystem"
      fi
    else
      _ray_local_instance="${{_ray_instance}}-${{UID:-$(id -u)}}-$$"
    if mkdir -p "/tmp/dsr${{_ray_local_instance}}" 2>/dev/null; then
      RAY_TMP_DIR="/tmp/dsr${{_ray_local_instance}}"
      echo "[$({date_fmt})] Using isolated local /tmp: $RAY_TMP_DIR"
    elif mkdir -p "/var/tmp/dsr${{_ray_local_instance}}" 2>/dev/null; then
      RAY_TMP_DIR="/var/tmp/dsr${{_ray_local_instance}}"
      echo "[$({date_fmt})] Using isolated local /var/tmp: $RAY_TMP_DIR"
    else
      RAY_TMP_DIR="$HOME/.dsr${{_ray_local_instance}}"
      mkdir -p "$RAY_TMP_DIR"
      echo "[$({date_fmt})] Using isolated local HOME: $RAY_TMP_DIR"
    fi
    fi
    export RAY_TMPDIR="$RAY_TMP_DIR"
    temp_dir_arg="--temp-dir=$RAY_TMP_DIR"
    echo "[$({
            date_fmt
        })] Ray temp directory: $RAY_TMP_DIR ($(echo -n "$RAY_TMP_DIR" | wc -c) chars)"
{override_block}    # Start local Ray cluster
    echo "[$({date_fmt})] Starting local Ray cluster"
    # Cleanup function - runs on exit, cancellation, or failure
    # This MUST succeed even if Ray never started or failed
    cleanup_ray() {{
      local exit_code=$?
      echo "[$({
            date_fmt
        })] Cleanup triggered (exit code: $exit_code, signal: ${{1:-none}})"

      # The --block parent owns only this Ray node's children. Signaling it
      # avoids `ray stop`, which would kill concurrent Ray runs by this user.
      echo "[$({date_fmt})] Stopping Ray..."
      stop_ray_process "${{RAY_HEAD_PID:-}}"
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
      trap - EXIT SIGINT SIGTERM
      exit "$exit_code"
    }}

    # Set trap for exit scenarios (not ERR - it causes double cleanup with set -e)
    trap cleanup_ray EXIT SIGINT SIGTERM

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
    # Set environment variables to ensure Ray uses the correct IP
    unset RAY_ADDRESS 2>/dev/null || true
    export RAY_IP="$head_bind_addr"
    export RAY_NODE_IP_ADDRESS="$head_bind_addr"
    export RAY_DASHBOARD_ADDRESS="http://$head_adv:$dash_port"
    echo "[$({date_fmt})] Starting Ray head on $head_bind_addr:$port..."
    ray start --head --port=$port --node-ip-address="$head_bind_addr" \
        --dashboard-host={self.dashboard_host} --dashboard-port=$dash_port \
        --node-manager-port=$node_manager_port \
        --object-manager-port=$object_manager_port \
        --ray-client-server-port=$ray_client_server_port \
        --redis-shard-ports=$redis_shard_port \
        --runtime-env-agent-port=$runtime_env_agent_port \
        --dashboard-agent-grpc-port=$dashboard_agent_grpc_port \
        --dashboard-agent-listen-port=$dashboard_agent_listen_port \
        --metrics-export-port=$metrics_export_port \
        --min-worker-port=$min_worker_port --max-worker-port=$max_worker_port \
        --num-gpus={self.num_gpus_per_node} {obj_store} {
            (" " + " ".join(self.ray_start_args)) if self.ray_start_args else ""
        } $temp_dir_arg --block &
    RAY_HEAD_PID=$!
    export RAY_ADDRESS="$head_adv:$port"
    echo "[$({date_fmt})] RAY_ADDRESS=$RAY_ADDRESS"
    # Give GCS a moment to initialize before checking
    sleep 2
    # Wait for Ray to be ready
    echo "[$({date_fmt})] Waiting for Ray to be ready..."
    for i in $(seq 1 {self.head_startup_timeout}); do
      if ray status --address "$RAY_ADDRESS" &>/dev/null; then
        echo "[$({date_fmt})] Ray is ready (local mode)"
        break
      fi
      if ! kill -0 "$RAY_HEAD_PID" 2>/dev/null; then
        echo "[$({date_fmt})] ERROR: Ray head exited during startup" >&2
        for log_dir in /tmp/ray/session_latest/logs "${{RAY_TMP_DIR:-}}"/session_*/logs; do
          if [[ -d "$log_dir" ]]; then
            for f in "$log_dir"/gcs_server.{{out,err}} "$log_dir"/raylet.{{out,err}}; do
              if [[ -f "$f" ]]; then
                echo "--- $f ---" >&2
                tail -50 "$f" >&2
              fi
            done
          fi
        done
        exit 1
      fi
      if [[ $i -eq {self.head_startup_timeout} ]]; then
        echo "[$({date_fmt})] ERROR: Ray failed to start within {
            self.head_startup_timeout
        } seconds" >&2
        exit 1
      fi
      sleep 1
    done
    echo "[$({date_fmt})] Ray cluster ready"
    echo "{RAY_DASHBOARD_URL_MARKER}http://$head_adv:$dash_port"
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
                "--node-manager-port=$node_manager_port",
                "--object-manager-port=$object_manager_port",
                "--ray-client-server-port=$ray_client_server_port",
                "--redis-shard-ports=$redis_shard_port",
                "--runtime-env-agent-port=$runtime_env_agent_port",
                "--dashboard-agent-grpc-port=$dashboard_agent_grpc_port",
                "--dashboard-agent-listen-port=$dashboard_agent_listen_port",
                "--metrics-export-port=$metrics_export_port",
                "--min-worker-port=$min_worker_port",
                "--max-worker-port=$max_worker_port",
                f"--num-gpus={self.num_gpus_per_node}",
                "--redis-password=$redis_password",
                "--temp-dir=$RAY_CLUSTER_TMP",
                "--block",
            ]
            + common_args
            + self.ray_start_args
        )

        worker_args = [
            "-v",
            "--address=$ip_head",
            "--redis-password=$redis_password",
            "--node-manager-port=$node_manager_port",
            "--object-manager-port=$object_manager_port",
            "--runtime-env-agent-port=$runtime_env_agent_port",
            "--dashboard-agent-grpc-port=$dashboard_agent_grpc_port",
            "--dashboard-agent-listen-port=$dashboard_agent_listen_port",
            "--metrics-export-port=$metrics_export_port",
            "--min-worker-port=$min_worker_port",
            "--max-worker-port=$max_worker_port",
            f"--num-gpus={self.num_gpus_per_node}",
            "--temp-dir=$RAY_CLUSTER_TMP",
        ] + common_args
        worker_node_ip_override = self._render_node_ip_override("worker_bind_addr")
        if worker_node_ip_override:
            worker_args.insert(2, "--node-ip-address=$worker_bind_addr")

        head_cmd_str = " \\\n    ".join(head_args)
        worker_cmd_str = " \\\n    ".join(worker_args)
        library_path_export = self._library_path_export(python_executable).strip()
        port_assignments = _render_ray_port_assignments(
            ray_port=self.ray_port,
            dashboard_port=self.dashboard_port,
            port_strategy=self.port_strategy,
            port_config=self.port_config,
        )
        process_cleanup = _render_ray_process_cleanup(self.grace_period)
        head_node_ip_override = self._render_node_ip_override("head_bind_addr")

        # --- Worker Script ---
        ray_worker_script = f"""#!/bin/bash
    set -e
    activation_script="$1"
    ip_head="$2"
    redis_password="$3"
    echo "Worker on $(hostname) activating environment: $activation_script"
    source "$activation_script"
    {library_path_export}
    {self._render_override_block(date_fmt)}
    {port_assignments}
    {process_cleanup}
    {worker_node_ip_override}
    # Determine Ray temp directory FIRST (short path to avoid 107-byte socket limit)
    # IMPORTANT: Must be node-local storage, NOT shared filesystem (NFS)!
    # Unix sockets don't work across NFS.
    # This MUST be defined before the cleanup function, trap, and background
    # log sync that all reference $RAY_CLUSTER_TMP.
    _ray_instance="${{_port_base:-$port}}"
    if [[ -n "${{SLURM_TMPDIR:-}}" ]]; then
        export RAY_CLUSTER_TMP="${{SLURM_TMPDIR}}/r$SLURM_JOB_ID-${{_ray_instance}}"
        echo "[$({date_fmt})] Using SLURM_TMPDIR for Ray (node-local)"
    elif mkdir -p "/tmp/r$SLURM_JOB_ID-${{_ray_instance}}" 2>/dev/null; then
        export RAY_CLUSTER_TMP="/tmp/r$SLURM_JOB_ID-${{_ray_instance}}"
        echo "[$({date_fmt})] Using /tmp for Ray (node-local)"
    elif mkdir -p "/var/tmp/r$SLURM_JOB_ID-${{_ray_instance}}" 2>/dev/null; then
        export RAY_CLUSTER_TMP="/var/tmp/r$SLURM_JOB_ID-${{_ray_instance}}"
        echo "[$({date_fmt})] Using /var/tmp for Ray (node-local)"
    else
        export RAY_CLUSTER_TMP="$HOME/.r$SLURM_JOB_ID-${{_ray_instance}}"
        echo "[$({
            date_fmt
        })] WARNING: Using HOME for Ray - if HOME is shared (NFS), this may cause socket conflicts!"
    fi
    echo "[$({
            date_fmt
        })] Ray temp directory: $RAY_CLUSTER_TMP ($(echo -n "$RAY_CLUSTER_TMP" | wc -c) chars)"
    mkdir -p "$RAY_CLUSTER_TMP"

    cleanup_node() {{
        local exit_code=$?
        echo "[$({date_fmt})] Worker on $(hostname) shutting down..."

        # Stop background log sync
        if [[ -n "${{WORKER_LOG_SYNC_PID:-}}" ]]; then
            echo "[$({
            date_fmt
        })] Stopping background log sync (PID: $WORKER_LOG_SYNC_PID)..."
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

        # Stop only this worker's --block parent and its owned children.
        stop_ray_process "${{RAY_WORKER_PID:-}}"

        # Cleanup temp dir
        rm -rf "$RAY_CLUSTER_TMP" 2>/dev/null || true
        echo "[$({date_fmt})] ✓ Worker cleanup complete"
        trap - EXIT INT TERM
        exit "$exit_code"
    }}
    trap cleanup_node TERM INT EXIT

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
    echo "[$({
            date_fmt
        })] Started background worker log sync (PID: $WORKER_LOG_SYNC_PID)"

    echo "Worker on $(hostname) starting and connecting to $ip_head..."
    ray start {worker_cmd_str} --block &
    RAY_WORKER_PID=$!
    wait "$RAY_WORKER_PID"
    """

        ray_driver_script = f"""#!/bin/bash
    set -e
    activation_script="$1"
    echo "======================================="
    echo "Ray Cluster Driver Script Started on $(hostname)"
    echo "Activating environment: $activation_script"
    echo "======================================="
    source "$activation_script"
    {library_path_export}
    {self._render_override_block(date_fmt)}
    # Define all variables first
    {port_assignments}
    {process_cleanup}
    
    # Choose head node (first host in allocation)
    head_node_name=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -n1)
    # Resolve what Ray should BIND to (and what workers should CONNECT to)
    head_bind_addr="$head_node_name"
    if [[ "{str(self.use_head_ip).lower()}" == "true" ]]; then
      # Prefer IPv4; fall back to IPv6; finally fall back to hostname
      ipv4=$(getent ahostsv4 "$head_node_name" | awk 'NR==1{{print $1}}' || true)
      if [[ -n "$ipv4" ]]; then
          head_bind_addr="$ipv4"
      else
          ipv6=$(getent ahostsv6 "$head_node_name" | awk 'NR==1{{print $1}}' || true)
          if [[ -n "$ipv6" ]]; then head_bind_addr="$ipv6"; fi
      fi
    fi
    {head_node_ip_override}
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

    # Determine Ray temp directory FIRST (short path to avoid 107-byte socket limit)
    # IMPORTANT: Must be node-local storage, NOT shared filesystem (NFS)!
    # Unix sockets don't work across NFS.
    # This MUST be defined before the cleanup function, trap, and background
    # log sync that all reference $RAY_CLUSTER_TMP.
    _ray_instance="${{_port_base:-$port}}"
    if [[ -n "${{SLURM_TMPDIR:-}}" ]]; then
        export RAY_CLUSTER_TMP="${{SLURM_TMPDIR}}/r$SLURM_JOB_ID-${{_ray_instance}}"
        echo "[$({date_fmt})] Using SLURM_TMPDIR for Ray (node-local)"
    elif mkdir -p "/tmp/r$SLURM_JOB_ID-${{_ray_instance}}" 2>/dev/null; then
        export RAY_CLUSTER_TMP="/tmp/r$SLURM_JOB_ID-${{_ray_instance}}"
        echo "[$({date_fmt})] Using /tmp for Ray (node-local)"
    elif mkdir -p "/var/tmp/r$SLURM_JOB_ID-${{_ray_instance}}" 2>/dev/null; then
        export RAY_CLUSTER_TMP="/var/tmp/r$SLURM_JOB_ID-${{_ray_instance}}"
        echo "[$({date_fmt})] Using /var/tmp for Ray (node-local)"
    else
        export RAY_CLUSTER_TMP="$HOME/.r$SLURM_JOB_ID-${{_ray_instance}}"
        echo "[$({
            date_fmt
        })] WARNING: Using HOME for Ray - if HOME is shared (NFS), this may cause socket conflicts!"
    fi
    echo "[$({
            date_fmt
        })] Ray temp directory: $RAY_CLUSTER_TMP ($(echo -n "$RAY_CLUSTER_TMP" | wc -c) chars)"
    mkdir -p "$RAY_CLUSTER_TMP"

    cleanup() {{
        exit_code=$?
        echo "======================================="
        echo "[$({
            date_fmt
        })] Initiating cluster shutdown (payload exit code: $exit_code, signal: ${{1:-none}})..."
        echo "======================================="

        # Capture error logs if job failed
        if [[ "$exit_code" -ne 0 ]]; then
            echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" >&2
            echo "PAYLOAD FAILED OR SCRIPT EXITED UNEXPECTEDLY! Capturing logs..." >&2
            for node in "${{worker_nodes[@]}}"; do
                echo "--- WORKER NODE ($node) RAYLET LOG ---" >&2
                srun --cpu-bind=none --nodes=1 --ntasks=1 -w "$node" bash -c 'tail -n 50 $(find $RAY_CLUSTER_TMP/session_*/logs/raylet.out -type f 2>/dev/null | sort | tail -n 1)' || echo "Worker log on $node not found." >&2
            done
            echo "--- HEAD NODE ($(hostname)) RAYLET LOG ---" >&2
            tail -n 50 $(find $RAY_CLUSTER_TMP/session_*/logs/raylet.out -type f 2>/dev/null | sort | tail -n 1) || echo "Head raylet log not found." >&2
            echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" >&2
        fi

        # Terminate worker processes
        if [ ${{#WORKER_PIDS[@]}} -gt 0 ]; then
            echo "[$({
            date_fmt
        })] Terminating ${{#WORKER_PIDS[@]}} worker srun process(es)..."
            kill -TERM "${{WORKER_PIDS[@]}}" 2>/dev/null || true
            sleep 2
            kill -9 "${{WORKER_PIDS[@]}}" 2>/dev/null || true
        fi

        # Stop only this head's --block parent and its owned children.
        echo "[$({date_fmt})] Stopping Ray head node..."
        stop_ray_process "${{RAY_HEAD_PID:-}}"

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
        rm -rf "$RAY_CLUSTER_TMP" 2>/dev/null || true
        echo "[$({date_fmt})] ✓ Shutdown complete"
        trap - EXIT SIGINT SIGTERM
        exit "$exit_code"
    }}
    trap cleanup EXIT SIGINT SIGTERM

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

    # ===== 1. Start Head Node =====
    echo "[$({date_fmt})] Starting Ray head on this node ($(hostname)) at $ip_head..."
    ray start {head_cmd_str} &
    RAY_HEAD_PID=$!
    export RAY_ADDRESS="$ip_head"

    # ===== 2. Wait for Head to be Ready =====
    echo "Waiting for Ray head to be ready..."
    for i in {{1..{self.head_startup_timeout}}}; do
        if ray status &>/dev/null; then echo "✓ Ray head is ready"; break; fi
        if ! kill -0 "$RAY_HEAD_PID" 2>/dev/null; then
            echo "ERROR: Ray head exited during startup" >&2
            exit 1
        fi
        if [[ $i -eq {
            self.head_startup_timeout
        } ]]; then echo "ERROR: Ray head failed to start" >&2; exit 1; fi
        sleep 1
    done
    echo "{RAY_DASHBOARD_URL_MARKER}http://$head_adv:$dash_port"

    # ===== 3. Start Worker Nodes =====
    all_nodes=($(scontrol show hostnames "$SLURM_JOB_NODELIST"))
    for node in "${{all_nodes[@]}}"; do
        if [[ "$node" != "$head_node_name" ]]; then worker_nodes+=("$node"); fi
    done
    echo "Head node: $head_node_name"; echo "Worker nodes: ${{worker_nodes[@]}}"
    export TMPDIR="{working_dir}"
    for node_i in "${{worker_nodes[@]}}"; do
        echo "Launching worker on $node_i..."
        srun {cpu_bind_option}--nodes=1 --ntasks=1 -w "$node_i" \\
            {
            working_dir
        }/ray_worker.sh "$activation_script" "$ip_head" "$redis_password" &
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

        # --- Main sbatch payload ---
        # Use --cpu-bind=none to avoid CPU binding conflicts with job allocation
        # Set TMPDIR to working dir in case /tmp is not writable
        main_sbatch_payload = f"""
    nodes=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
    nodes_array=($nodes)
    head_node="${{nodes_array[0]}}"
    echo "Designated head node: $head_node"
    export TMPDIR="{working_dir}"
    srun --cpu-bind=none --nodes=1 --ntasks=1 -w "$head_node" {working_dir}/ray_driver.sh "{activation_script}"
    """
        auxiliary_scripts = {
            "ray_driver.sh": ray_driver_script,
            "ray_worker.sh": ray_worker_script,
        }

        if allocation_context:
            raise NotImplementedError("This architecture is for standalone sbatch jobs")

        return main_sbatch_payload, auxiliary_scripts
