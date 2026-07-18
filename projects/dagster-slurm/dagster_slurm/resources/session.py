"""Slurm session management for operator fusion and run-scoped allocation."""

import hashlib
import json
import os
import posixpath
import shlex
import re
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Literal, Optional, Set
import uuid

from dagster import (
    Config,
    ConfigurableResource,
    InitResourceContext,
    get_dagster_logger,
)
from loguru import logger
from pydantic import Field, PrivateAttr

from ..helpers.ssh_helpers import TERMINAL_STATES, normalize_slurm_state
from ..helpers.ssh_pool import SSHConnectionPool
from ..launchers.base import ExecutionPlan
from ..resources.slurm import SlurmResource

_REMOTE_LOCK_WAIT_TIMEOUT_SECONDS = 180
_REMOTE_LOCK_POLL_SECONDS = 2
_SHARED_ALLOCATION_CLEANUP_GRACE_SECONDS = 10
_SAFE_AUXILIARY_SCRIPT_NAME_RE = re.compile(r"^[A-Za-z0-9_.=-]+$")


def _try_acquire_remote_lock(
    ssh_pool: SSHConnectionPool,
    *,
    lock_dir: str,
    owner: str,
) -> bool:
    """Acquire a remote filesystem lock with atomic mkdir."""
    parent_dir = posixpath.dirname(lock_dir.rstrip("/")) or "."
    quoted_parent = shlex.quote(parent_dir)
    quoted_lock = shlex.quote(lock_dir)
    quoted_owner = shlex.quote(owner)
    cmd = f"""
set -e
mkdir -p {quoted_parent}
if mkdir {quoted_lock} 2>/dev/null; then
  printf '%s\\n' {quoted_owner} > {quoted_lock}/owner
  printf acquired
  exit 0
fi
printf busy
"""
    return ssh_pool.run(cmd).strip().endswith("acquired")


def _safe_auxiliary_script_name(name: str) -> str:
    """Validate an auxiliary script name before using it as a remote path segment."""
    if not isinstance(name, str) or not name:
        raise ValueError("Auxiliary script names must be non-empty strings.")
    if "\x00" in name or "/" in name or "\\" in name:
        raise ValueError(
            f"Unsafe auxiliary script name {name!r}; use a safe basename only."
        )
    if name in {".", ".."} or posixpath.basename(name) != name:
        raise ValueError(
            f"Unsafe auxiliary script name {name!r}; use a safe basename only."
        )
    if not _SAFE_AUXILIARY_SCRIPT_NAME_RE.fullmatch(name):
        raise ValueError(
            f"Unsafe auxiliary script name {name!r}; use only letters, digits, '.', "
            "'_', '-', or '='."
        )
    return name


def _release_remote_lock(
    ssh_pool: SSHConnectionPool,
    *,
    lock_dir: str,
    owner: str,
) -> None:
    """Release a remote lock only when it is still owned by this process."""
    quoted_lock = shlex.quote(lock_dir)
    quoted_owner = shlex.quote(owner)
    ssh_pool.run(
        f"if [ -f {quoted_lock}/owner ] && "
        f'[ "$(cat {quoted_lock}/owner 2>/dev/null)" = {quoted_owner} ]; then '
        f"rm -rf {quoted_lock}; "
        "fi"
    )


def _describe_remote_lock(ssh_pool: SSHConnectionPool, *, lock_dir: str) -> str:
    quoted_lock = shlex.quote(lock_dir)
    quoted_owner_path = shlex.quote(f"{lock_dir}/owner")
    cmd = f"""
if [ -d {quoted_lock} ]; then
  printf 'lock_dir=%s\\n' {quoted_lock}
  if [ -f {quoted_owner_path} ]; then
    printf 'owner='
    cat {quoted_owner_path} 2>/dev/null || true
    printf '\\n'
  else
    printf 'owner=<missing>\\n'
  fi
  printf 'mtime='
  stat -c '%y' {quoted_lock} 2>/dev/null || stat -f '%Sm' {quoted_lock} 2>/dev/null || printf '<unknown>'
  printf '\\n'
else
  printf 'lock_dir=%s\\nstate=<missing>\\n' {quoted_lock}
fi
"""
    try:
        details = ssh_pool.run(cmd).strip()
    except Exception as exc:
        return f"lock_dir={lock_dir}\ndetails_unavailable={exc}"
    return details or f"lock_dir={lock_dir}\ndetails_unavailable=<empty>"


def _tail_remote_file_for_error(
    ssh_pool: SSHConnectionPool,
    path: str,
    *,
    byte_limit: int = 4000,
) -> str:
    try:
        output = ssh_pool.run(
            f"tail -c {byte_limit} {shlex.quote(path)} 2>/dev/null || true"
        )
    except Exception as exc:
        return f"<failed to read {path}: {exc}>"
    return output.rstrip() or "<empty>"


class SlurmAllocationScope(str, Enum):
    """Controls how Slurm allocations are scoped for ``mode="slurm"``."""

    ASSET = "asset"
    RUN = "run"


class SlurmRunAllocationConfig(Config):
    """Configuration for a run-owned Slurm allocation."""

    num_nodes: Optional[int] = Field(
        default=None,
        ge=1,
        description="Number of nodes for the run-owned allocation.",
    )
    gpus_per_node: Optional[int] = Field(
        default=None,
        ge=0,
        description="GPUs requested per node for the run-owned allocation.",
    )
    cpus_per_task: Optional[int] = Field(
        default=None,
        ge=1,
        description="CPUs per task requested for the allocation.",
    )
    mem: Optional[str] = Field(
        default=None,
        description="Memory allocation, for example '32G'.",
    )
    mem_per_cpu: Optional[str] = Field(
        default=None,
        description="Memory per CPU, for clusters that prefer --mem-per-cpu.",
    )
    time_limit: Optional[str] = Field(
        default=None,
        description="Maximum allocation time, for example '04:00:00'.",
    )
    partition: Optional[str] = Field(
        default=None,
        description="Slurm partition override for the allocation.",
    )
    qos: Optional[str] = Field(default=None, description="QoS override.")
    account: Optional[str] = Field(default=None, description="Account override.")
    reservation: Optional[str] = Field(
        default=None,
        description="Reservation override.",
    )
    constraint: Optional[str] = Field(
        default=None,
        description="Slurm constraint expression.",
    )
    cleanup_policy: Literal["after_run"] = Field(
        default="after_run",
        description="When to release the allocation. Only after_run is supported.",
    )


@dataclass(frozen=True)
class SlurmStepExecutionResult:
    """Result for one step executed inside a shared Slurm allocation."""

    job_id: int
    stdout_path: str
    stderr_path: str


class SlurmSessionResource(ConfigurableResource):
    """Slurm session resource for operator fusion.

    This is a proper Dagster resource that manages the lifecycle
    of a Slurm allocation across multiple assets in a run.

    Usage in definitions.py:

    .. code-block:: python

        session = SlurmSessionResource(
            slurm=slurm,
            num_nodes=4,
            time_limit="04:00:00",
        )
    """

    slurm: "SlurmResource" = Field(description="Slurm cluster configuration")
    num_nodes: int = Field(default=2, description="Nodes in allocation")
    time_limit: str = Field(default="04:00:00", description="Max allocation time")
    partition: Optional[str] = Field(default=None, description="Override partition")
    max_concurrent_jobs: int = Field(default=10, description="Max concurrent srun jobs")
    enable_health_checks: bool = Field(
        default=True, description="Enable node health checks"
    )
    enable_session: bool = Field(
        default=True, description="Enable session mode for operator fusion"
    )
    gpus_per_node: Optional[int] = Field(
        default=None,
        ge=0,
        description=(
            "GPUs per node requested for the allocation. If unset, inherits the "
            "Slurm queue default; set 0 explicitly to disable GPUs."
        ),
    )
    cpus_per_task: Optional[int] = Field(
        default=None, description="CPUs per task requested for the allocation"
    )
    mem: Optional[str] = Field(
        default=None, description="Memory requested for the allocation"
    )
    mem_per_cpu: Optional[str] = Field(
        default=None, description="Memory per CPU requested for the allocation"
    )
    qos: Optional[str] = Field(
        default=None, description="QoS override for the session allocation"
    )
    account: Optional[str] = Field(
        default=None, description="Account override for the session allocation"
    )
    reservation: Optional[str] = Field(
        default=None, description="Reservation override for the session allocation"
    )
    constraint: Optional[str] = Field(
        default=None, description="Constraint override for the session allocation"
    )

    # Private attributes for state management
    _allocation: Optional["SlurmAllocation"] = PrivateAttr(default=None)
    _ssh_pool: Optional[SSHConnectionPool] = PrivateAttr(default=None)
    _execution_semaphore: Optional[threading.Semaphore] = PrivateAttr(default=None)
    _initialized: bool = PrivateAttr(default=False)
    _lifecycle_lock: threading.RLock = PrivateAttr(default_factory=threading.RLock)
    _logger: Any = PrivateAttr(default=None)
    _context: Any = PrivateAttr(default=None)
    _owns_allocation: bool = PrivateAttr(default=False)
    _shared_lifecycle: bool = PrivateAttr(default=False)
    _allocation_lease_id: Optional[str] = PrivateAttr(default=None)

    @property
    def logger(self) -> Any:
        return self._logger or get_dagster_logger()

    def setup_for_execution(self, context: InitResourceContext) -> None:
        """Called by Dagster when resource is initialized for a run.
        This is the proper Dagster resource lifecycle hook.
        """
        with self._lifecycle_lock:
            if self._initialized:
                return

            self._logger = get_dagster_logger()
            self._context = context
            self._execution_semaphore = threading.Semaphore(self.max_concurrent_jobs)

            # Only create allocation if session mode is enabled
            if self.enable_session:
                # Start SSH pool
                self._ssh_pool = SSHConnectionPool(self.slurm.ssh)
                self._ssh_pool.__enter__()

                # Create or attach to the run allocation
                self._allocation = self._create_allocation(context)
                if self._shared_lifecycle:
                    self._register_allocation_lease()
                self.logger.info(
                    f"Session resource initialized with allocation {self._allocation.slurm_job_id}"
                )
            else:
                self.logger.info("Session mode disabled")

            self._initialized = True

    def teardown_after_execution(self, context: InitResourceContext) -> None:
        """Called by Dagster when resource is torn down after run completion.
        This is the proper Dagster resource lifecycle hook.
        """
        with self._lifecycle_lock:
            if not self._initialized:
                return

            self.logger.info("Tearing down session resource...")

            if self._shared_lifecycle:
                self._release_allocation_lease()

            # Cancel allocation
            if self._allocation:
                try:
                    if self._shared_lifecycle:
                        self._schedule_shared_allocation_cleanup()
                    elif self._owns_allocation:
                        self._allocation.cancel(self._ssh_pool)  # type: ignore
                        self.logger.info(
                            f"Allocation {self._allocation.slurm_job_id} canceled"
                        )
                    else:
                        self.logger.info(
                            "Attached session is not the allocation owner; skipping "
                            f"scancel for {self._allocation.slurm_job_id}"
                        )
                except Exception as e:
                    self.logger.warning(f"Error canceling allocation: {e}")

            # Close SSH pool
            if self._ssh_pool:
                try:
                    self._ssh_pool.__exit__(None, None, None)
                    self.logger.info("SSH connection pool closed")
                except Exception as e:
                    self.logger.warning(f"Error closing SSH pool: {e}")

            self._initialized = False

    def execute_in_session(
        self,
        execution_plan: ExecutionPlan,
        asset_key: str,
        run_dir: str,
    ) -> SlurmStepExecutionResult:
        """Execute workload in the shared allocation.
        Thread-safe for parallel asset execution.
        """
        if not self._initialized:
            raise RuntimeError(
                "Session not initialized. "
                "This resource must be setup by Dagster before use."
            )

        if not self.enable_session:
            raise RuntimeError("Session mode is disabled. Cannot execute in session.")

        # Rate limiting
        with self._execution_semaphore:  # type: ignore
            # Health check
            if self.enable_health_checks and not self._allocation.is_healthy(  # type: ignore
                self._ssh_pool  # type: ignore
            ):
                raise RuntimeError(
                    f"Allocation unhealthy. Failed nodes: {self._allocation.get_failed_nodes()}"  # type: ignore
                )

            # Execute
            return self._allocation.execute(  # type: ignore
                execution_plan=execution_plan,
                asset_key=asset_key,
                run_dir=run_dir,
                ssh_pool=self._ssh_pool,  # type: ignore
            )

    def _resolve_run_id(self, context) -> str:
        """Prefer DagsterRun.run_id to avoid deprecated InitResourceContext.run_id."""
        if context.run:
            run_id = context.run.run_id
        else:
            self.logger.warning(
                "Context is not part of a Dagster run, generating a temporary run_id."
            )
            run_id = uuid.uuid4().hex
        return run_id

    def _require_ssh_pool(self) -> SSHConnectionPool:
        if self._ssh_pool is None:
            raise RuntimeError("SSH pool is not initialized")
        return self._ssh_pool

    def _create_allocation(self, context) -> "SlurmAllocation":
        """Start or attach to the run's Slurm allocation."""
        allocation_id = f"dagster_{self._resolve_run_id(context)}"
        working_dir = f"{self.slurm.remote_base}/allocations/{allocation_id}"

        ssh_pool = self._require_ssh_pool()
        ssh_pool.run(f"mkdir -p {shlex.quote(working_dir)}")

        existing = self._read_allocation_metadata(working_dir)
        if existing is not None:
            object.__setattr__(self, "_owns_allocation", False)
            self.logger.info(f"Attached to existing allocation {existing.slurm_job_id}")
            return existing

        lock_dir = f"{working_dir}/.allocation.lock"
        lock_owner = f"{os.getpid()}-{threading.get_ident()}-{uuid.uuid4().hex}"
        deadline = time.time() + _REMOTE_LOCK_WAIT_TIMEOUT_SECONDS
        while time.time() < deadline:
            if _try_acquire_remote_lock(
                ssh_pool,
                lock_dir=lock_dir,
                owner=lock_owner,
            ):
                try:
                    existing = self._read_allocation_metadata(working_dir)
                    if existing is not None:
                        object.__setattr__(self, "_owns_allocation", False)
                        self.logger.info(
                            f"Attached to existing allocation {existing.slurm_job_id}"
                        )
                        return existing

                    allocation = self._submit_allocation(
                        allocation_id=allocation_id,
                        working_dir=working_dir,
                    )
                    self._write_allocation_metadata(allocation)
                    object.__setattr__(self, "_owns_allocation", True)
                    return allocation
                finally:
                    _release_remote_lock(
                        ssh_pool,
                        lock_dir=lock_dir,
                        owner=lock_owner,
                    )

            existing = self._read_allocation_metadata(working_dir)
            if existing is not None:
                object.__setattr__(self, "_owns_allocation", False)
                self.logger.info(
                    f"Attached to existing allocation {existing.slurm_job_id}"
                )
                return existing
            time.sleep(_REMOTE_LOCK_POLL_SECONDS)

        lock_details = _describe_remote_lock(ssh_pool, lock_dir=lock_dir)
        raise TimeoutError(
            "Timed out waiting for run-scoped Slurm allocation lock at "
            f"{lock_dir}. Existing lock details:\n{lock_details}"
        )

    def _submit_allocation(
        self,
        *,
        allocation_id: str,
        working_dir: str,
    ) -> "SlurmAllocation":
        """Submit a fresh Slurm allocation. Caller must hold the remote lock."""

        # Build allocation script
        partition = self.partition or self.slurm.queue.partition
        script_lines = [
            "#!/bin/bash",
            f"#SBATCH --job-name={allocation_id}",
            f"#SBATCH --time={self.time_limit}",
            "#SBATCH --output=allocation_%j.log",
        ]

        if partition:
            script_lines.append(f"#SBATCH --partition={partition}")

        def _normalize_optional(value):
            if value is None:
                return None
            if isinstance(value, str):
                cleaned = value.strip()
                return cleaned or None
            return str(value)

        qos = _normalize_optional(self.qos) or _normalize_optional(
            getattr(self.slurm.queue, "qos", None)
        )
        if qos:
            script_lines.append(f"#SBATCH --qos={qos}")

        account = _normalize_optional(self.account) or _normalize_optional(
            getattr(self.slurm.queue, "account", None)
        )
        if account:
            script_lines.append(f"#SBATCH --account={account}")

        reservation = _normalize_optional(self.reservation) or _normalize_optional(
            getattr(self.slurm.queue, "reservation", None)
        )
        if reservation:
            script_lines.append(f"#SBATCH --reservation={reservation}")

        constraint = _normalize_optional(self.constraint)
        if constraint:
            script_lines.append(f"#SBATCH --constraint={constraint}")

        cpus_per_task = self.cpus_per_task or getattr(self.slurm.queue, "cpus", None)
        if cpus_per_task:
            script_lines.append(f"#SBATCH --cpus-per-task={cpus_per_task}")

        mem = _normalize_optional(self.mem) or _normalize_optional(
            getattr(self.slurm.queue, "mem", None)
        )
        mem_per_cpu = _normalize_optional(self.mem_per_cpu) or _normalize_optional(
            getattr(self.slurm.queue, "mem_per_cpu", None)
        )
        if mem:
            script_lines.append(f"#SBATCH --mem={mem}")
        elif mem_per_cpu:
            script_lines.append(f"#SBATCH --mem-per-cpu={mem_per_cpu}")

        gpus_per_node = (
            self.gpus_per_node
            if self.gpus_per_node is not None
            else self.slurm.queue.gpus_per_node
        )

        final_num_nodes: Optional[int] = None
        if self.num_nodes and self.num_nodes > 0:
            final_num_nodes = self.num_nodes

        if gpus_per_node and final_num_nodes == 1 and gpus_per_node == 1:
            final_num_nodes = None

        if final_num_nodes:
            script_lines.append(f"#SBATCH --nodes={final_num_nodes}")

        if gpus_per_node:
            script_lines.append(f"#SBATCH --gres=gpu:{gpus_per_node}")

        quoted_working_dir = shlex.quote(working_dir)
        script_lines.extend(
            [
                "",
                "# Keep allocation alive for srun jobs",
                f"working_dir={quoted_working_dir}",
                'mkdir -p "$working_dir"',
                'echo "Allocation started"',
                'hostname > "${working_dir}/head_node.txt"',
                'scontrol show hostname $SLURM_JOB_NODELIST > "${working_dir}/nodes.txt"',
                "",
                "# Wait for cancellation",
                "sleep infinity",
            ]
        )

        # Submit allocation
        script_path = f"{working_dir}/allocation.sh"
        self._ssh_pool.write_file("\n".join(script_lines), script_path)  # type: ignore
        self._ssh_pool.run(f"chmod +x {shlex.quote(script_path)}")  # type: ignore

        submit_cmd = f"sbatch -D {shlex.quote(working_dir)} {shlex.quote(script_path)}"
        output = self._ssh_pool.run(submit_cmd)  # type: ignore

        match = re.search(r"Submitted batch job (\d+)", output)
        if not match:
            raise RuntimeError(f"Could not parse job ID from:\n{output}")

        job_id = int(match.group(1))
        self.logger.info(f"Allocation submitted: job {job_id}")

        # Query estimated start time for pending allocation
        self._log_estimated_start_time(job_id)

        # Wait for allocation to start
        self._wait_for_allocation_start(job_id, working_dir, timeout=120)

        # Read node list
        nodes_path = f"{working_dir}/nodes.txt"
        nodes_output = self._ssh_pool.run(f"cat {shlex.quote(nodes_path)}")  # type: ignore
        nodes = [n.strip() for n in nodes_output.strip().split("\n") if n.strip()]

        self.logger.info(f"Allocation ready: {len(nodes)} nodes: {nodes}")

        return SlurmAllocation(
            slurm_job_id=job_id,
            nodes=nodes,
            working_dir=working_dir,
            config=self,
        )

    def _allocation_metadata_path(self, working_dir: str) -> str:
        return f"{working_dir}/allocation.json"

    def _read_allocation_metadata(
        self,
        working_dir: str,
    ) -> "SlurmAllocation | None":
        ssh_pool = self._require_ssh_pool()
        metadata_path = self._allocation_metadata_path(working_dir)
        output = ssh_pool.run(f"cat {shlex.quote(metadata_path)} 2>/dev/null || true")
        if not output.strip():
            return None

        try:
            metadata = json.loads(output)
            job_id = int(metadata["slurm_job_id"])
            nodes = [str(node) for node in metadata["nodes"]]
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            self.logger.warning(
                f"Ignoring invalid allocation metadata at {metadata_path}: {exc}"
            )
            return None

        state = self._get_job_state(job_id)
        if state in TERMINAL_STATES:
            self.logger.warning(
                f"Ignoring stale allocation metadata for job {job_id} "
                f"with state {state}"
            )
            return None

        return SlurmAllocation(
            slurm_job_id=job_id,
            nodes=nodes,
            working_dir=working_dir,
            config=self,
        )

    def _write_allocation_metadata(self, allocation: "SlurmAllocation") -> None:
        metadata = {
            "slurm_job_id": allocation.slurm_job_id,
            "nodes": allocation.nodes,
            "working_dir": allocation.working_dir,
        }
        metadata_path = self._allocation_metadata_path(allocation.working_dir)
        tmp_path = f"{metadata_path}.tmp.{uuid.uuid4().hex}"
        ssh_pool = self._require_ssh_pool()
        ssh_pool.write_file(json.dumps(metadata, sort_keys=True), tmp_path)
        ssh_pool.run(f"mv {shlex.quote(tmp_path)} {shlex.quote(metadata_path)}")

    def _register_allocation_lease(self) -> None:
        if not self._allocation:
            return
        ssh_pool = self._require_ssh_pool()
        lease_id = f"{os.getpid()}-{threading.get_ident()}-{uuid.uuid4().hex}"
        lease_dir = f"{self._allocation.working_dir}/leases"
        lease_path = f"{lease_dir}/{lease_id}.lease"
        ssh_pool.run(
            f"mkdir -p {shlex.quote(lease_dir)} && "
            f"printf '%s\\n' {shlex.quote(lease_id)} > {shlex.quote(lease_path)}"
        )
        object.__setattr__(self, "_allocation_lease_id", lease_id)

    def _release_allocation_lease(self) -> None:
        if not self._allocation or not self._allocation_lease_id:
            return
        ssh_pool = self._require_ssh_pool()
        lease_path = (
            f"{self._allocation.working_dir}/leases/{self._allocation_lease_id}.lease"
        )
        ssh_pool.run(f"rm -f {shlex.quote(lease_path)}")
        object.__setattr__(self, "_allocation_lease_id", None)

    def _schedule_shared_allocation_cleanup(self) -> None:
        if not self._allocation:
            return
        ssh_pool = self._require_ssh_pool()

        lease_dir = f"{self._allocation.working_dir}/leases"
        job_id = self._allocation.slurm_job_id
        grace = _SHARED_ALLOCATION_CLEANUP_GRACE_SECONDS
        cmd = f"""
(
  sleep {grace}
  active=""
  if [ -d {shlex.quote(lease_dir)} ]; then
    active="$(find {shlex.quote(lease_dir)} -type f -name '*.lease' -print -quit 2>/dev/null || true)"
  fi
  if [ -z "$active" ]; then
    scancel {job_id} 2>/dev/null || true
  fi
) >/dev/null 2>&1 &
"""
        ssh_pool.run(cmd)
        self.logger.info(
            f"Scheduled shared allocation {job_id} cleanup after {grace}s grace"
        )

    def _log_estimated_start_time(self, job_id: int) -> None:
        """Log commands to check queue status for a pending allocation job."""
        # Just log the commands - don't try to parse output (Slurm versions vary too much)
        self.logger.info(
            f"Allocation job {job_id} submitted. Check queue status with:\n"
            f"  squeue --start -j {job_id}\n"
            f"  squeue --start -j {job_id} --json | jq '.jobs[0].start_time'"
        )

    def _wait_for_allocation_start(
        self,
        job_id: int,
        working_dir: str,
        timeout: int,
    ):
        """Poll until allocation is running."""
        start = time.time()

        while time.time() - start < timeout:
            state = self._get_job_state(job_id)

            if state == "RUNNING":
                # Verify marker file exists
                try:
                    head_node_path = f"{working_dir}/head_node.txt"
                    self._ssh_pool.run(f"test -f {shlex.quote(head_node_path)}")  # type: ignore
                    return
                except:  # noqa: E722
                    pass
            elif state in TERMINAL_STATES:
                raise RuntimeError(f"Allocation {job_id} failed with state: {state}")

            time.sleep(2)

        raise TimeoutError(f"Allocation {job_id} not ready after {timeout}s")

    def _get_job_state(self, job_id: int) -> str:
        """Query job state."""
        try:
            output = self._ssh_pool.run(  # type: ignore
                f"squeue -h -j {job_id} -o '%T' 2>/dev/null || true"
            )
            state = normalize_slurm_state(output)
            if state:
                return state

            output = self._ssh_pool.run(  # type: ignore
                f"sacct -X -n -j {job_id} -o State%20 2>/dev/null || true"
            )
            state = output.strip()
            return normalize_slurm_state(state.split()[0]) if state else ""
        except Exception:
            return ""


class SlurmAllocation:
    """Represents a running Slurm allocation."""

    def __init__(
        self,
        slurm_job_id: int,
        nodes: List[str],
        working_dir: str,
        config: SlurmSessionResource,
    ):
        self.slurm_job_id = slurm_job_id
        self.nodes = nodes
        self.working_dir = working_dir
        self.config = config
        self.logger = get_dagster_logger()
        self._failed_nodes: Set[str] = set()
        self._exec_count = 0
        self._exec_lock = threading.Lock()
        self._ray_cluster_lock = threading.Lock()
        self._ray_address: Optional[str] = None
        self._ray_fingerprint: Optional[tuple[Any, ...]] = None

    def execute(
        self,
        execution_plan: ExecutionPlan,
        asset_key: str,
        run_dir: str,
        ssh_pool: SSHConnectionPool,
    ) -> SlurmStepExecutionResult:
        """Execute plan in this allocation via srun."""
        with self._exec_lock:
            self._exec_count += 1
            exec_id = self._exec_count

        auxiliary_scripts = getattr(execution_plan, "auxiliary_scripts", {})
        safe_auxiliary_scripts = [
            (_safe_auxiliary_script_name(aux_name), aux_content)
            for aux_name, aux_content in auxiliary_scripts.items()
        ]

        script_lines = execution_plan.payload

        safe_asset_key = re.sub(r"[^A-Za-z0-9_.=-]+", "_", asset_key).strip("._-")
        if not safe_asset_key:
            safe_asset_key = "asset"
        script_name = f"asset_{exec_id}_{safe_asset_key}.sh"
        script_path = f"{run_dir}/{script_name}"
        ssh_pool.write_file("\n".join(script_lines), script_path)
        ssh_pool.run(f"chmod +x {shlex.quote(script_path)}")

        for safe_aux_name, aux_content in safe_auxiliary_scripts:
            aux_path = f"{run_dir}/{safe_aux_name}"
            ssh_pool.write_file(aux_content, aux_path)
            ssh_pool.run(f"chmod +x {shlex.quote(aux_path)}")

        log_name = f"slurm-{self.slurm_job_id}-step-{exec_id}_{safe_asset_key}"
        stdout_path = f"{run_dir}/{log_name}.out"
        stderr_path = f"{run_dir}/{log_name}.err"

        # Execute via srun
        srun_cmd = (
            f"srun --overlap --jobid={self.slurm_job_id} "
            f"--job-name=asset_{exec_id} {shlex.quote(script_path)} "
            f"> {shlex.quote(stdout_path)} 2> {shlex.quote(stderr_path)}"
        )

        self.logger.info(f"Executing in allocation {self.slurm_job_id}: {script_name}")
        try:
            ssh_pool.run(srun_cmd)
        except Exception as exc:
            stdout_tail = _tail_remote_file_for_error(ssh_pool, stdout_path)
            stderr_tail = _tail_remote_file_for_error(ssh_pool, stderr_path)
            raise RuntimeError(
                f"srun step {exec_id} failed in allocation {self.slurm_job_id}. "
                f"stdout_path={stdout_path}; stderr_path={stderr_path}\n"
                f"Original error: {exc}\n"
                f"--- stdout tail ---\n{stdout_tail}\n"
                f"--- stderr tail ---\n{stderr_tail}"
            ) from exc
        self.logger.info(
            f"Execution {exec_id} in allocation {self.slurm_job_id} completed"
        )

        return SlurmStepExecutionResult(
            job_id=self.slurm_job_id,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        )

    def ensure_ray_cluster(
        self,
        *,
        ssh_pool: SSHConnectionPool,
        launcher: Any,
        activation_script: str,
        startup_timeout: int,
    ) -> str:
        """Start one persistent Ray cluster inside the allocation and return its address."""
        if not self.nodes:
            raise RuntimeError(
                f"Allocation {self.slurm_job_id} has no nodes; cannot start Ray."
            )

        fingerprint = self._ray_launcher_fingerprint(
            launcher=launcher,
            activation_script=activation_script,
        )
        fingerprint_token = self._ray_fingerprint_token(fingerprint)
        with self._ray_cluster_lock:
            if self._ray_address:
                if self._ray_fingerprint != fingerprint:
                    raise ValueError(
                        "Run-scoped Ray allocation already exists with a different "
                        "launcher or environment. Use matching RayLauncher settings "
                        "and environment packaging for all assets in the run."
                    )
                return self._ray_address

            existing_address = self._read_existing_ray_cluster(
                ssh_pool=ssh_pool,
                fingerprint=fingerprint,
                fingerprint_token=fingerprint_token,
            )
            if existing_address:
                self._ray_address = existing_address
                self._ray_fingerprint = fingerprint
                return existing_address

            ray_dir = f"{self.working_dir}/ray_cluster"
            lock_dir = f"{ray_dir}/.start.lock"
            lock_owner = f"{os.getpid()}-{threading.get_ident()}-{uuid.uuid4().hex}"
            deadline = time.time() + max(
                startup_timeout, _REMOTE_LOCK_WAIT_TIMEOUT_SECONDS
            )
            while time.time() < deadline:
                if _try_acquire_remote_lock(
                    ssh_pool,
                    lock_dir=lock_dir,
                    owner=lock_owner,
                ):
                    try:
                        existing_address = self._read_existing_ray_cluster(
                            ssh_pool=ssh_pool,
                            fingerprint=fingerprint,
                            fingerprint_token=fingerprint_token,
                        )
                        if existing_address:
                            self._ray_address = existing_address
                            self._ray_fingerprint = fingerprint
                            return existing_address

                        self._ray_address = self._start_ray_cluster(
                            ssh_pool=ssh_pool,
                            launcher=launcher,
                            activation_script=activation_script,
                            startup_timeout=startup_timeout,
                        )
                        self._write_ray_fingerprint(
                            ssh_pool=ssh_pool,
                            fingerprint_token=fingerprint_token,
                        )
                        self._ray_fingerprint = fingerprint
                        return self._ray_address
                    finally:
                        _release_remote_lock(
                            ssh_pool,
                            lock_dir=lock_dir,
                            owner=lock_owner,
                        )

                existing_address = self._read_existing_ray_cluster(
                    ssh_pool=ssh_pool,
                    fingerprint=fingerprint,
                    fingerprint_token=fingerprint_token,
                )
                if existing_address:
                    self._ray_address = existing_address
                    self._ray_fingerprint = fingerprint
                    return existing_address
                time.sleep(_REMOTE_LOCK_POLL_SECONDS)

            lock_details = _describe_remote_lock(ssh_pool, lock_dir=lock_dir)
            raise TimeoutError(
                "Timed out waiting for persistent Ray cluster lock at "
                f"{lock_dir}. Existing lock details:\n{lock_details}"
            )

    def _ray_launcher_fingerprint(
        self,
        *,
        launcher: Any,
        activation_script: str,
    ) -> tuple[Any, ...]:
        return (
            activation_script,
            getattr(launcher, "num_gpus_per_node", 0),
            getattr(launcher, "dashboard_port", 8265),
            getattr(launcher, "object_store_memory_gb", None),
            tuple(getattr(launcher, "ray_start_args", [])),
            getattr(launcher, "redis_password", None),
            getattr(launcher, "ray_port", 6379),
            tuple(getattr(launcher, "pre_start_commands", [])),
            getattr(launcher, "worker_cpu_bind", "none"),
            getattr(launcher, "use_head_ip", True),
            getattr(launcher, "dashboard_host", "0.0.0.0"),
            getattr(launcher, "port_strategy", "hash_jobid"),
        )

    def _ray_fingerprint_token(self, fingerprint: tuple[Any, ...]) -> str:
        payload = json.dumps(fingerprint, default=str, sort_keys=True)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _ray_fingerprint_path(self) -> str:
        return f"{self.working_dir}/ray_cluster/ray_fingerprint.sha256"

    def _read_existing_ray_cluster(
        self,
        *,
        ssh_pool: SSHConnectionPool,
        fingerprint: tuple[Any, ...],
        fingerprint_token: str,
    ) -> str | None:
        ray_dir = f"{self.working_dir}/ray_cluster"
        ready_path = f"{ray_dir}/ray_ready"
        address_path = f"{ray_dir}/ray_address"
        fingerprint_path = self._ray_fingerprint_path()
        output = ssh_pool.run(
            f"if [ -f {shlex.quote(ready_path)} ] && "
            f"[ -f {shlex.quote(address_path)} ]; then "
            f"cat {shlex.quote(address_path)}; "
            "fi"
        ).strip()
        if not output:
            return None

        remote_fingerprint = ssh_pool.run(
            f"cat {shlex.quote(fingerprint_path)} 2>/dev/null || true"
        ).strip()
        if remote_fingerprint and remote_fingerprint != fingerprint_token:
            raise ValueError(
                "Run-scoped Ray allocation already exists with a different "
                "launcher or environment. Use matching RayLauncher settings "
                "and environment packaging for all assets in the run."
            )

        if not remote_fingerprint:
            return None

        self._ray_fingerprint = fingerprint
        return output

    def _write_ray_fingerprint(
        self,
        *,
        ssh_pool: SSHConnectionPool,
        fingerprint_token: str,
    ) -> None:
        fingerprint_path = self._ray_fingerprint_path()
        tmp_path = f"{fingerprint_path}.tmp.{uuid.uuid4().hex}"
        ssh_pool.write_file(fingerprint_token, tmp_path)
        ssh_pool.run(f"mv {shlex.quote(tmp_path)} {shlex.quote(fingerprint_path)}")

    def _start_ray_cluster(
        self,
        *,
        ssh_pool: SSHConnectionPool,
        launcher: Any,
        activation_script: str,
        startup_timeout: int,
    ) -> str:
        ray_dir = f"{self.working_dir}/ray_cluster"
        ssh_pool.run(f"mkdir -p {shlex.quote(ray_dir)}")

        head_script = self._render_ray_head_script(
            launcher=launcher,
            activation_script=activation_script,
            ray_dir=ray_dir,
        )
        worker_script = self._render_ray_worker_script(
            launcher=launcher,
            activation_script=activation_script,
        )
        head_script_path = f"{ray_dir}/ray_head.sh"
        worker_script_path = f"{ray_dir}/ray_worker.sh"
        ssh_pool.write_file(head_script, head_script_path)
        ssh_pool.write_file(worker_script, worker_script_path)
        ssh_pool.run(
            f"chmod +x {shlex.quote(head_script_path)} {shlex.quote(worker_script_path)}"
        )

        head_node = self.nodes[0]
        head_log = f"{ray_dir}/ray_head.log"
        head_cmd = (
            f"nohup srun --overlap --jobid={self.slurm_job_id} "
            f"--nodes=1 --ntasks=1 -w {shlex.quote(head_node)} "
            f"{shlex.quote(head_script_path)} "
            f"> {shlex.quote(head_log)} 2>&1 < /dev/null &"
        )
        self.logger.info(
            f"Starting persistent Ray head in allocation {self.slurm_job_id}"
        )
        ssh_pool.run(head_cmd)

        ray_address = self._wait_for_ray_address(
            ssh_pool=ssh_pool,
            startup_timeout=startup_timeout,
        )

        for index, node in enumerate(self.nodes[1:], start=1):
            worker_log = f"{ray_dir}/ray_worker_{index}.log"
            worker_cmd = (
                f"nohup srun --overlap --jobid={self.slurm_job_id} "
                f"--nodes=1 --ntasks=1 -w {shlex.quote(node)} "
                f"{shlex.quote(worker_script_path)} {shlex.quote(ray_address)} "
                f"> {shlex.quote(worker_log)} 2>&1 < /dev/null &"
            )
            self.logger.info(
                f"Starting persistent Ray worker on {node} in allocation {self.slurm_job_id}"
            )
            ssh_pool.run(worker_cmd)

        return ray_address

    def _render_ray_head_script(
        self,
        *,
        launcher: Any,
        activation_script: str,
        ray_dir: str,
    ) -> str:
        date_fmt = "date +%Y-%m-%dT%H:%M:%S%z"
        ray_port = int(getattr(launcher, "ray_port", 6379))
        dashboard_port = int(getattr(launcher, "dashboard_port", 8265))
        port_strategy = str(getattr(launcher, "port_strategy", "hash_jobid"))
        use_head_ip = str(getattr(launcher, "use_head_ip", True)).lower()
        dashboard_host = shlex.quote(
            str(getattr(launcher, "dashboard_host", "0.0.0.0"))
        )
        num_gpus = int(getattr(launcher, "num_gpus_per_node", 0))
        object_store_arg = ""
        if getattr(launcher, "object_store_memory_gb", None) is not None:
            bytes_value = int(launcher.object_store_memory_gb) * 1_000_000_000
            object_store_arg = f"--object-store-memory={bytes_value}"

        start_args = " ".join(
            shlex.quote(str(arg)) for arg in getattr(launcher, "ray_start_args", [])
        )
        pre_start = "\n".join(
            str(command) for command in getattr(launcher, "pre_start_commands", [])
        )
        redis_password = getattr(launcher, "redis_password", None)
        redis_arg = (
            f"--redis-password={shlex.quote(str(redis_password))}"
            if redis_password
            else ""
        )
        temp_dir_setup = self._render_ray_temp_dir_setup(
            variable_name="RAY_TMP_DIR",
            date_fmt=date_fmt,
        )
        ray_start_lines = [
            "ray start --head \\",
            '  --node-ip-address="$head_bind_addr" \\',
            '  --port="$port" \\',
            '  --dashboard-port="$dash_port" \\',
            f"  --dashboard-host={dashboard_host} \\",
            '  --temp-dir="$RAY_TMP_DIR" \\',
            f"  --num-gpus={num_gpus}",
        ]
        ray_start_lines.extend(
            f"  {arg}" for arg in (object_store_arg, redis_arg, start_args) if arg
        )
        for index in range(len(ray_start_lines) - 1):
            if not ray_start_lines[index].endswith("\\"):
                ray_start_lines[index] = f"{ray_start_lines[index]} \\"
        ray_start_command = "\n".join(ray_start_lines)

        return f"""#!/bin/bash
set -euo pipefail
source {shlex.quote(activation_script)}
{pre_start}

port="{ray_port}"
dash_port="{dashboard_port}"
if [[ "{port_strategy}" == "hash_jobid" && -n "${{SLURM_JOB_ID:-}}" ]]; then
    off=$(( SLURM_JOB_ID % 1000 ))
    port=$(( {ray_port} + off ))
	    dash_port=$(( {dashboard_port} + off ))
	fi

	head_node_name="$(hostname)"
	head_bind_addr="$head_node_name"
	if [[ -z "$head_bind_addr" ]]; then
	  head_bind_addr="127.0.0.1"
	fi
	if [[ "{use_head_ip}" == "true" ]]; then
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
	head_adv="$head_bind_addr"
	if [[ "$head_adv" == *:* ]]; then head_adv="[$head_adv]"; fi
	ray_address="$head_adv:$port"
	unset RAY_ADDRESS 2>/dev/null || true
	export RAY_IP="$head_bind_addr"
	export RAY_NODE_IP_ADDRESS="$head_bind_addr"
	export RAY_DASHBOARD_ADDRESS="http://$head_adv:$dash_port"
	{temp_dir_setup}

	cleanup_ray() {{
	  echo "[$({date_fmt})] Stopping persistent Ray head..."
	  ray stop --force 2>/dev/null || true
	  if [[ -n "${{RAY_TMP_DIR:-}}" && -d "$RAY_TMP_DIR" ]]; then
	    echo "[$({date_fmt})] Removing $RAY_TMP_DIR..."
	    rm -rf "$RAY_TMP_DIR" 2>/dev/null || true
	  fi
	}}
	trap cleanup_ray EXIT INT TERM

	echo "[$({date_fmt})] Starting persistent Ray head at $ray_address"
	{ray_start_command}

echo "$ray_address" > {shlex.quote(ray_dir)}/ray_address.tmp
mv {shlex.quote(ray_dir)}/ray_address.tmp {shlex.quote(ray_dir)}/ray_address
touch {shlex.quote(ray_dir)}/ray_ready

while true; do
  sleep 30
done
"""

    def _render_ray_worker_script(
        self,
        *,
        launcher: Any,
        activation_script: str,
    ) -> str:
        date_fmt = "date +%Y-%m-%dT%H:%M:%S%z"
        pre_start = "\n".join(
            str(command) for command in getattr(launcher, "pre_start_commands", [])
        )
        num_gpus = int(getattr(launcher, "num_gpus_per_node", 0))
        redis_password = getattr(launcher, "redis_password", None)
        redis_arg = (
            f"--redis-password={shlex.quote(str(redis_password))}"
            if redis_password
            else ""
        )
        temp_dir_setup = self._render_ray_temp_dir_setup(
            variable_name="RAY_TMP_DIR",
            date_fmt=date_fmt,
        )

        return f"""#!/bin/bash
	set -euo pipefail
	ray_address="$1"
	source {shlex.quote(activation_script)}
	{pre_start}
	{temp_dir_setup}

	cleanup_ray() {{
	  echo "[$({date_fmt})] Stopping persistent Ray worker..."
	  ray stop --force 2>/dev/null || true
	  if [[ -n "${{RAY_TMP_DIR:-}}" && -d "$RAY_TMP_DIR" ]]; then
	    echo "[$({date_fmt})] Removing $RAY_TMP_DIR..."
	    rm -rf "$RAY_TMP_DIR" 2>/dev/null || true
	  fi
	}}
	trap cleanup_ray EXIT INT TERM

	echo "[$({date_fmt})] Starting persistent Ray worker for $ray_address"
	ray start --address="$ray_address" --num-gpus={num_gpus} --temp-dir="$RAY_TMP_DIR" {redis_arg}

	while true; do
	  sleep 30
	done
	"""

    @staticmethod
    def _render_ray_temp_dir_setup(*, variable_name: str, date_fmt: str) -> str:
        if not variable_name.isidentifier():
            raise ValueError(f"Invalid shell variable name: {variable_name!r}")

        var_ref = f"${{{variable_name}}}"
        return f"""# Use a short, node-local Ray temp directory for sockets and runtime files.
	if [[ -n "${{SLURM_TMPDIR:-}}" ]]; then
	  {variable_name}="${{SLURM_TMPDIR}}/r${{SLURM_JOB_ID:-manual}}"
	  echo "[$({date_fmt})] Using SLURM_TMPDIR for Ray (node-local)"
	elif mkdir -p "/tmp/r${{SLURM_JOB_ID:-manual}}" 2>/dev/null; then
	  {variable_name}="/tmp/r${{SLURM_JOB_ID:-manual}}"
	  echo "[$({date_fmt})] Using /tmp for Ray (node-local)"
	elif mkdir -p "/var/tmp/r${{SLURM_JOB_ID:-manual}}" 2>/dev/null; then
	  {variable_name}="/var/tmp/r${{SLURM_JOB_ID:-manual}}"
	  echo "[$({date_fmt})] Using /var/tmp for Ray (node-local)"
	else
	  {variable_name}="$HOME/.r${{SLURM_JOB_ID:-manual}}"
	  echo "[$({date_fmt})] WARNING: Using HOME for Ray - shared filesystems can break Ray sockets"
	fi
	mkdir -p "{var_ref}"
	export RAY_TMPDIR="{var_ref}"
	echo "[$({date_fmt})] Ray temp directory: {var_ref} ($(echo -n "{var_ref}" | wc -c) chars)"
	"""

    def _wait_for_ray_address(
        self,
        *,
        ssh_pool: SSHConnectionPool,
        startup_timeout: int,
    ) -> str:
        ray_dir = f"{self.working_dir}/ray_cluster"
        address_path = f"{ray_dir}/ray_address"
        ready_path = f"{ray_dir}/ray_ready"
        deadline = time.time() + startup_timeout
        while time.time() < deadline:
            try:
                address = ssh_pool.run(
                    f"test -f {shlex.quote(ready_path)} && "
                    f"cat {shlex.quote(address_path)} 2>/dev/null || true"
                ).strip()
            except Exception:
                address = ""

            if address:
                self.logger.info(
                    f"Persistent Ray cluster ready in allocation {self.slurm_job_id}: {address}"
                )
                return address

            time.sleep(2)

        tail = ssh_pool.run(
            f"tail -n 80 {shlex.quote(ray_dir)}/ray_head.log 2>/dev/null || true"
        )
        raise TimeoutError(
            "Persistent Ray cluster did not become ready within "
            f"{startup_timeout}s. Ray head log tail:\n{tail}"
        )

    def is_healthy(self, ssh_pool: SSHConnectionPool) -> bool:
        """Check if allocation and nodes are healthy."""
        # Check allocation state
        try:
            output = ssh_pool.run(
                f"squeue -h -j {self.slurm_job_id} -o '%T' 2>/dev/null || true"
            )
            state = output.strip()
            if state not in {"RUNNING", ""}:
                return False
        except Exception:
            return False

        # Check node health
        for node in self.nodes:
            if node in self._failed_nodes:
                continue

            if not self._ping_node(node, ssh_pool):
                self._failed_nodes.add(node)
                self.logger.warning(f"Node {node} failed health check")

        # Allocation is healthy if at least one node is good
        return len(self._failed_nodes) < len(self.nodes)

    def _ping_node(self, node: str, ssh_pool: SSHConnectionPool) -> bool:
        """Verify node is responsive."""
        try:
            cmd = (
                f"srun --overlap --jobid={self.slurm_job_id} "
                f"--nodelist={node} "
                f"--time=00:00:10 "
                f"hostname"
            )
            ssh_pool.run(cmd, timeout=15)
            return True
        except Exception as e:
            self.logger.warning(f"Node {node} ping failed: {e}")
            return False

    def get_failed_nodes(self) -> List[str]:
        """Get list of failed nodes."""
        return list(self._failed_nodes)

    def cancel(self, ssh_pool: SSHConnectionPool):
        """Cancel the allocation."""
        ssh_pool.run(f"scancel {self.slurm_job_id}")


class SessionResourcePool:
    """Manages reusable Ray/Spark clusters in session mode."""

    def __init__(
        self,
        session: SlurmSessionResource,
        keep_alive: bool = True,
        resource_tolerance: float = 0.2,  # 20% tolerance for reuse
    ):
        self.session = session
        self.keep_alive = keep_alive
        self.resource_tolerance = resource_tolerance
        self._active_clusters = {}  # cluster_id -> cluster_info

    def get_or_create_ray_cluster(
        self,
        required_cpus: int,
        required_gpus: int,
        required_memory_gb: int,
    ):
        """Get existing Ray cluster if resources are close enough,
        otherwise create new one.
        """
        # Check if we have a compatible cluster
        for cluster_id, info in self._active_clusters.items():
            if info["type"] == "ray":
                # Check if resources are within tolerance
                cpu_match = (
                    abs(info["cpus"] - required_cpus) / required_cpus
                    < self.resource_tolerance
                )
                gpu_match = info["gpus"] == required_gpus  # GPUs must match exactly
                mem_match = (
                    abs(info["memory_gb"] - required_memory_gb) / required_memory_gb
                    < self.resource_tolerance
                )

                if cpu_match and gpu_match and mem_match:
                    logger.info(f"Reusing existing Ray cluster {cluster_id}")
                    return info["address"]

        # No compatible cluster - create new one
        logger.info("Creating new Ray cluster")
        cluster_address = self._start_ray_cluster(  # type: ignore
            required_cpus, required_gpus, required_memory_gb
        )

        cluster_id = f"ray_{uuid.uuid4().hex[:8]}"
        self._active_clusters[cluster_id] = {
            "type": "ray",
            "address": cluster_address,
            "cpus": required_cpus,
            "gpus": required_gpus,
            "memory_gb": required_memory_gb,
        }

        return cluster_address
