"""Slurm session management for operator fusion and run-scoped allocation."""

import hashlib
import json
import os
import posixpath
import shlex
import re
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, replace as dataclass_replace
from enum import Enum
from typing import Any, Callable, List, Literal, Optional, Set
import uuid

from dagster import (
    Config,
    ConfigurableResource,
    InitResourceContext,
    get_dagster_logger,
)
from loguru import logger
from pydantic import Field, PrivateAttr, model_validator

from ..helpers.ssh_helpers import TERMINAL_STATES, normalize_slurm_state
from ..helpers.ssh_pool import SSHConnectionPool
from ..helpers.signals import build_pre_timeout_supervisor_script
from ..launchers.base import ExecutionPlan
from ..helpers.ray_dashboard import RAY_DASHBOARD_URL_MARKER
from ..launchers.ray import (
    RayLauncher,
    RayPortConfig,
    _render_ray_port_assignments,
    _render_ray_process_cleanup,
)
from ..resources.slurm import (
    SlurmResource,
    normalize_signal_before_timeout,
    validate_signal_before_timeout,
    _slurm_time_limit_seconds,
)

_REMOTE_LOCK_WAIT_TIMEOUT_SECONDS = 180
_REMOTE_LOCK_POLL_SECONDS = 2
_SHARED_ALLOCATION_CLEANUP_GRACE_SECONDS = 10
_SAFE_AUXILIARY_SCRIPT_NAME_RE = re.compile(r"^[A-Za-z0-9_.=-]+$")
_SESSION_ALLOCATION_DIR_TAG = "dagster_slurm/session_allocation_dir"


def _validate_relay_options(
    time_min: str | None, time_limit: str | None, directives: list[str]
) -> None:
    if time_min is not None:
        minimum = _slurm_time_limit_seconds(time_min)
        if minimum <= 0 or (
            time_limit is not None and minimum > _slurm_time_limit_seconds(time_limit)
        ):
            raise ValueError("time_min must be positive and no greater than time_limit")
    # Only single, long-form scheduler options are accepted. Prevent overrides
    # of fields and options that would break our script/step lifecycle.
    reserved = {
        "job-name",
        "time",
        "time-min",
        "signal",
        "output",
        "error",
        "chdir",
        "wrap",
        "array",
        "wait",
        "parsable",
        "test-only",
        "hold",
        "requeue",
        "nodes",
        "ntasks",
        "ntasks-per-node",
        "cpus-per-task",
        "mem",
        "mem-per-cpu",
        "gres",
        "gpus",
        "gpus-per-node",
        "partition",
        "nodelist",
        "exclude",
        "qos",
        "account",
        "reservation",
        "constraint",
        "export",
        "export-file",
    }
    seen: set[str] = set()
    for directive in directives:
        match = re.fullmatch(
            r"--([a-z][a-z0-9-]*)(?:=([^\s\x00-\x1f\x7f]+))?", directive
        )
        if match is None:
            raise ValueError(
                "extra_sbatch_directives requires one --option[=value] per entry"
            )
        name = match[1]
        if any(option.startswith(name) for option in reserved) or name in seen:
            raise ValueError(f"Duplicate or managed sbatch directive: --{name}")
        seen.add(name)


_HOSTLIST_EXPANSION_LIMIT = 65536


def _validate_hostlist(field_name: str, hostlist: str | None) -> None:
    if hostlist is not None and (
        not hostlist or any(character.isspace() for character in hostlist)
    ):
        raise ValueError(f"{field_name} must be a non-empty Slurm host-list expression")


def _split_hostlist(hostlist: str) -> list[str]:
    """Split a host-list expression on commas outside brackets."""
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for character in hostlist:
        if character == "[":
            depth += 1
        elif character == "]":
            depth -= 1
        if character == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(character)
    parts.append("".join(current))
    return [part for part in parts if part]


def _expand_hostlist(hostlist: str) -> set[str] | None:
    """Expand a Slurm host-list expression such as ``gpu-[01-03,07]``.

    Returns None when the expression cannot be expanded locally (for example a
    file path or a malformed/oversized range), so callers can skip checks that
    depend on the concrete host names.
    """
    if "/" in hostlist:
        return None
    hosts: set[str] = set()
    for part in _split_hostlist(hostlist):
        expanded = [""]
        for literal, ranges in re.findall(r"([^\[\]]*)(?:\[([^\[\]]*)\])?", part):
            if not literal and not ranges:
                continue
            suffixes = [literal]
            if ranges:
                values: list[str] = []
                for item in ranges.split(","):
                    match = re.fullmatch(r"(\d+)(?:-(\d+))?", item)
                    if match is None:
                        return None
                    start, end = match.group(1), match.group(2) or match.group(1)
                    width = len(start)
                    if int(end) < int(start):
                        return None
                    values.extend(
                        f"{number:0{width}d}"
                        for number in range(int(start), int(end) + 1)
                    )
                suffixes = [literal + value for value in values]
            expanded = [prefix + suffix for prefix in expanded for suffix in suffixes]
            if len(expanded) > _HOSTLIST_EXPANSION_LIMIT:
                return None
        hosts.update(expanded)
        if len(hosts) > _HOSTLIST_EXPANSION_LIMIT:
            return None
    return hosts


def _validate_node_placement(nodelist: str | None, exclude: str | None) -> None:
    """Validate nodelist/exclude and reject nodes that appear in both."""
    _validate_hostlist("nodelist", nodelist)
    _validate_hostlist("exclude", exclude)
    if nodelist is None or exclude is None:
        return
    included = _expand_hostlist(nodelist)
    excluded = _expand_hostlist(exclude)
    if included is None or excluded is None:
        return
    overlap = included & excluded
    if overlap:
        raise ValueError(
            "nodelist and exclude must not name the same node(s): "
            + ", ".join(sorted(overlap))
        )


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
    time_min: Optional[str] = Field(
        default=None, description="Minimum backfill walltime."
    )
    extra_sbatch_directives: List[str] = Field(default_factory=list)
    signal_before_timeout: Optional[str] = Field(
        default=None,
        description="Signal sent to the allocation shell before walltime, e.g. TERM@120.",
    )
    partition: Optional[str] = Field(
        default=None,
        description="Slurm partition override for the allocation.",
    )
    nodelist: Optional[str] = Field(
        default=None,
        description=(
            "Slurm node list expression used to pin the run-owned allocation, "
            "for example 'gpu-01' or 'gpu-[01-04]'."
        ),
    )
    exclude: Optional[str] = Field(
        default=None,
        description=(
            "Slurm node list expression the run-owned allocation must avoid, "
            "for example to keep a known-bad node out of a retry without "
            "pinning placement via nodelist."
        ),
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

    @model_validator(mode="after")
    def _validate_signal_before_timeout(self) -> "SlurmRunAllocationConfig":
        _validate_relay_options(
            self.time_min, self.time_limit, self.extra_sbatch_directives
        )
        _validate_node_placement(self.nodelist, self.exclude)
        if self.time_limit is None and self.signal_before_timeout is not None:
            normalize_signal_before_timeout(self.signal_before_timeout)
        elif self.time_limit is not None:
            validate_signal_before_timeout(
                self.signal_before_timeout,
                self.time_limit,
            )
        return self


@dataclass(frozen=True)
class SlurmStepExecutionResult:
    """Result for one step executed inside a shared Slurm allocation."""

    job_id: int
    stdout_path: str
    stderr_path: str
    step_id: str | None = None
    step_id_path: str | None = None
    status_path: str | None = None
    drain_marker_path: str | None = None
    drain_signal: str | None = None
    exit_code: int | None = None
    allocation_state: str | None = None

    @property
    def drained(self) -> bool:
        """Whether a drain signal fired during this invocation."""
        return self.drain_signal is not None


class SlurmStepDrained(RuntimeError):
    """A Pipes invocation drained; callers can inspect the durable step result."""

    def __init__(self, result: SlurmStepExecutionResult):
        self.result = result
        super().__init__(
            f"Slurm step {result.step_id or result.job_id} drained ({result.drain_signal})"
        )


class SlurmAllocationEnded(RuntimeError):
    """The allocation ended before a step could publish its exit status."""

    def __init__(self, result: SlurmStepExecutionResult):
        self.result = result
        super().__init__(
            f"Slurm allocation {result.job_id} ended ({result.allocation_state})"
        )


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
    time_min: Optional[str] = Field(
        default=None, description="Minimum backfill walltime"
    )
    extra_sbatch_directives: List[str] = Field(default_factory=list)
    signal_before_timeout: Optional[str] = Field(
        default=None,
        description="Signal sent to the allocation shell before walltime, e.g. TERM@120.",
    )
    partition: Optional[str] = Field(default=None, description="Override partition")
    nodelist: Optional[str] = Field(
        default=None,
        description="Node list expression used to pin the session allocation",
    )
    exclude: Optional[str] = Field(
        default=None,
        description="Node list expression the session allocation must avoid",
    )
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

    @model_validator(mode="after")
    def _validate_signal_before_timeout(self) -> "SlurmSessionResource":
        _validate_relay_options(
            self.time_min, self.time_limit, self.extra_sbatch_directives
        )
        _validate_node_placement(self.nodelist, self.exclude)
        signal_spec = self._effective_signal_before_timeout()
        if (
            signal_spec
            and build_pre_timeout_supervisor_script("", signal_spec, "") is None
        ):
            raise ValueError(
                "Session drain requires a trappable signal (not KILL or STOP)"
            )
        if self.time_min is not None:
            validate_signal_before_timeout(signal_spec, self.time_min)
        return self

    def _effective_signal_before_timeout(self) -> str | None:
        return validate_signal_before_timeout(
            self.signal_before_timeout
            if self.signal_before_timeout is not None
            else self.slurm.queue.signal_before_timeout,
            self.time_limit,
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
    _preserve_allocation_on_teardown: bool = PrivateAttr(default=False)
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
                    if self._preserve_allocation_on_teardown:
                        self.logger.info(
                            "Preserving allocation %s for supervisor reattachment",
                            self._allocation.slurm_job_id,
                        )
                    elif self._shared_lifecycle:
                        self._schedule_shared_allocation_cleanup()
                    elif self._owns_allocation:
                        self._require_ssh_pool().run(
                            f"scancel --name={shlex.quote(posixpath.basename(self._allocation.session_dir))}"
                        )
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
        step_update_callback: Callable[[SlurmStepExecutionResult], None] | None = None,
        poll_callback: Callable[[SlurmStepExecutionResult], None] | None = None,
        timeout: int | None = None,
        allocation: "SlurmAllocation | None" = None,
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
            allocation = allocation or self.allocation
            # Health check
            if self.enable_health_checks and not allocation.is_healthy(
                self._ssh_pool  # type: ignore
            ):
                raise RuntimeError(
                    f"Allocation unhealthy. Failed nodes: {self._allocation.get_failed_nodes()}"  # type: ignore
                )

            # Execute
            return allocation.execute(
                execution_plan=execution_plan,
                asset_key=asset_key,
                run_dir=run_dir,
                ssh_pool=self._ssh_pool,  # type: ignore
                step_update_callback=step_update_callback,
                poll_callback=poll_callback,
                timeout=timeout,
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
        allocations_root = posixpath.normpath(f"{self.slurm.remote_base}/allocations")
        working_dir = f"{allocations_root}/{allocation_id}"
        if context.run:
            tagged_dir = getattr(context.run, "tags", {}).get(
                _SESSION_ALLOCATION_DIR_TAG
            )
            if tagged_dir:
                normalized_tagged_dir = posixpath.normpath(tagged_dir)
                if normalized_tagged_dir.startswith(f"{allocations_root}/"):
                    working_dir = normalized_tagged_dir
                    allocation_id = posixpath.basename(working_dir)
                else:
                    self.logger.warning(
                        "Ignoring session allocation directory outside %s: %s",
                        allocations_root,
                        tagged_dir,
                    )

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
        wait_for_start: bool = True,
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
        signal_before_timeout = self._effective_signal_before_timeout()
        if signal_before_timeout:
            script_lines.append(f"#SBATCH --signal=B:{signal_before_timeout}")
        if self.time_min:
            script_lines.append(f"#SBATCH --time-min={self.time_min}")
        script_lines.extend(
            f"#SBATCH {directive}" for directive in self.extra_sbatch_directives
        )

        if partition:
            script_lines.append(f"#SBATCH --partition={partition}")

        def _normalize_optional(value):
            if value is None:
                return None
            if isinstance(value, str):
                cleaned = value.strip()
                return cleaned or None
            return str(value)

        nodelist = _normalize_optional(self.nodelist)
        if nodelist:
            script_lines.append(f"#SBATCH --nodelist={nodelist}")

        exclude = _normalize_optional(self.exclude)
        if exclude:
            script_lines.append(f"#SBATCH --exclude={exclude}")

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
                'working_dir="$working_dir/jobs/${SLURM_JOB_ID:?}"',
                'mkdir -p "$working_dir"',
                self._render_allocation_drain_trap(),
                'echo "Allocation started"',
                'hostname > "${working_dir}/head_node.txt"',
                'scontrol show hostname $SLURM_JOB_NODELIST > "${working_dir}/nodes.txt"',
                "",
                "# Wait for cancellation",
                "sleep infinity &",
                "keeper=$!",
                'while kill -0 "$keeper" 2>/dev/null; do wait "$keeper" || true; done',
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
        allocation = SlurmAllocation(
            slurm_job_id=job_id,
            nodes=[],
            working_dir=f"{working_dir}/jobs/{job_id}",
            session_dir=working_dir,
            config=self,
        )
        if wait_for_start:
            try:
                self._wait_for_allocation_start(
                    job_id, allocation.working_dir, timeout=120
                )
                self._load_allocation_nodes(allocation)
            except Exception:
                allocation.cancel(self._require_ssh_pool())
                raise
        return allocation

    def _render_allocation_drain_trap(self) -> str:
        signal_name = (self._effective_signal_before_timeout() or "TERM@1").split("@")[
            0
        ]
        return f"""
drain_payloads() {{
  printf '%s\\n' {shlex.quote(signal_name)} > "$working_dir/drain_signal"
  # Only registered driver steps are signalled. Ray steps never register here.
  for marker in "$working_dir"/payloads/*/id; do
    [ -f "$marker" ] || continue
    [ ! -f "${{marker%/id}}/status" ] || continue
    step_id=$(cat "$marker")
    if [[ "$step_id" =~ ^${{SLURM_JOB_ID}}\\.[0-9]+$ ]]; then
      scancel --signal={shlex.quote(signal_name)} "$step_id" || true
    fi
  done
}}
trap drain_payloads {shlex.quote(signal_name)}
"""

    def _load_allocation_nodes(self, allocation: "SlurmAllocation") -> None:
        output = self._require_ssh_pool().run(
            f"cat {shlex.quote(allocation.working_dir + '/nodes.txt')}"
        )
        allocation.nodes = [
            node.strip() for node in output.splitlines() if node.strip()
        ]
        if not allocation.nodes:
            raise RuntimeError(
                f"Allocation {allocation.slurm_job_id} has no ready nodes"
            )

    def _allocation_metadata_path(self, working_dir: str) -> str:
        return f"{working_dir}/allocation.json"

    def _read_allocation_metadata(
        self,
        working_dir: str,
    ) -> "SlurmAllocation | None":
        metadata = self._read_session_metadata(working_dir)
        if metadata is None:
            return None
        try:
            allocation = self._allocation_from_record(metadata, working_dir)
        except (KeyError, TypeError, ValueError) as exc:
            self.logger.warning(f"Ignoring invalid allocation metadata: {exc}")
            return None
        state = self._get_job_state(allocation.slurm_job_id)
        if state in TERMINAL_STATES:
            successor = metadata.get("successor")
            if (
                not successor
                or self._get_job_state(int(successor["slurm_job_id"]))
                in TERMINAL_STATES
            ):
                return None
        return allocation

    def _read_session_metadata(self, working_dir: str) -> dict[str, Any] | None:
        ssh_pool = self._require_ssh_pool()
        metadata_path = self._allocation_metadata_path(working_dir)
        output = ssh_pool.run(f"cat {shlex.quote(metadata_path)} 2>/dev/null || true")
        if not output.strip():
            return None

        try:
            metadata = json.loads(output)
            if not isinstance(metadata, dict):
                raise ValueError("allocation metadata must be an object")
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            self.logger.warning(
                f"Ignoring invalid allocation metadata at {metadata_path}: {exc}"
            )
            return None

        return metadata

    def _allocation_from_record(
        self, record: dict[str, Any], session_dir: str
    ) -> "SlurmAllocation":
        config = self
        if record.get("config"):
            config = SlurmSessionResource(slurm=self.slurm, **record["config"])
            object.__setattr__(config, "_ssh_pool", self._require_ssh_pool())
        return SlurmAllocation(
            slurm_job_id=int(record["slurm_job_id"]),
            nodes=[str(node) for node in record["nodes"]],
            working_dir=record.get("working_dir", session_dir),
            session_dir=session_dir,
            config=config,
        )

    def _allocation_record(self, allocation: "SlurmAllocation") -> dict[str, Any]:
        return {
            "slurm_job_id": allocation.slurm_job_id,
            "nodes": allocation.nodes,
            "working_dir": allocation.working_dir,
            "config": {
                key: getattr(allocation.config, key)
                for key in SlurmRunAllocationConfig.model_fields
                if key != "cleanup_policy"
            },
        }

    def _write_allocation_metadata(self, allocation: "SlurmAllocation") -> None:
        self._write_session_metadata(
            allocation.session_dir, self._allocation_record(allocation)
        )

    def _write_session_metadata(
        self, session_dir: str, metadata: dict[str, Any]
    ) -> None:
        metadata_path = self._allocation_metadata_path(session_dir)
        tmp_path = f"{metadata_path}.tmp.{uuid.uuid4().hex}"
        ssh_pool = self._require_ssh_pool()
        ssh_pool.write_file(json.dumps(metadata, sort_keys=True), tmp_path)
        ssh_pool.run(f"mv {shlex.quote(tmp_path)} {shlex.quote(metadata_path)}")

    @contextmanager
    def _session_lock(
        self, session_dir: str, ssh_pool: SSHConnectionPool | None = None
    ):
        ssh_pool = ssh_pool or self._require_ssh_pool()
        lock_dir = f"{session_dir}/.allocation.lock"
        owner = uuid.uuid4().hex
        deadline = time.monotonic() + _REMOTE_LOCK_WAIT_TIMEOUT_SECONDS
        while not _try_acquire_remote_lock(ssh_pool, lock_dir=lock_dir, owner=owner):
            if time.monotonic() >= deadline:
                raise TimeoutError("Timed out waiting for the session lifecycle lock")
            time.sleep(0.1)
        try:
            yield
        finally:
            _release_remote_lock(ssh_pool, lock_dir=lock_dir, owner=owner)

    @property
    def allocation(self) -> "SlurmAllocation":
        """Current allocation, refreshed from the session's published identity."""
        with self._lifecycle_lock:
            if self._allocation is None:
                raise RuntimeError("Session is not initialized")
            metadata = self._read_session_metadata(self._allocation.session_dir)
            if (
                metadata
                and int(metadata["slurm_job_id"]) != self._allocation.slurm_job_id
            ):
                object.__setattr__(
                    self,
                    "_allocation",
                    self._allocation_from_record(
                        metadata, self._allocation.session_dir
                    ),
                )
            return self._allocation

    def submit_successor(
        self, config: SlurmRunAllocationConfig | None = None
    ) -> "SlurmAllocation":
        """Publish a queued successor immediately, inheriting unspecified fields.

        Only one successor may be outstanding. All allocations keep the session
        job name so terminal-run cleanup also cancels pending successors.
        """
        with self._lifecycle_lock:
            current = self.allocation
            with self._session_lock(current.session_dir):
                metadata = self._read_session_metadata(current.session_dir)
                if metadata is None:
                    raise RuntimeError("Session allocation metadata is missing")
                if metadata.get("successor"):
                    raise RuntimeError("A successor is already published")
                shape = dict(
                    metadata.get("config") or self._allocation_record(current)["config"]
                )
                if config is not None:
                    shape.update(
                        config.model_dump(
                            exclude_unset=True, exclude={"cleanup_policy"}
                        )
                    )
                successor_config = SlurmSessionResource(slurm=self.slurm, **shape)
                object.__setattr__(
                    successor_config, "_ssh_pool", self._require_ssh_pool()
                )
                successor = successor_config._submit_allocation(
                    allocation_id=posixpath.basename(current.session_dir),
                    working_dir=current.session_dir,
                    wait_for_start=False,
                )
                metadata["successor"] = self._allocation_record(successor)
                try:
                    self._write_session_metadata(current.session_dir, metadata)
                except Exception:
                    successor.cancel(self._require_ssh_pool())
                    raise
                return successor

    def promote_successor(
        self,
        *,
        launcher: Any = None,
        activation_script: str | None = None,
        startup_timeout: int = 120,
    ) -> "SlurmAllocation":
        """Promote a RUNNING successor after predecessor payloads have drained.

        Call ``allocation.request_drain()`` and wait for payload invocations to
        finish first. Ray is started before publication, using the predecessor's
        recorded launcher/environment unless explicitly supplied here.
        """
        with self._lifecycle_lock:
            current = self.allocation
            ssh_pool = self._require_ssh_pool()
            with self._session_lock(current.session_dir):
                metadata = self._read_session_metadata(current.session_dir)
                if (
                    metadata
                    and not metadata.get("successor")
                    and metadata.get("predecessors")
                ):
                    self._finish_promotion(current, metadata)
                    return current
                if not metadata or not metadata.get("successor"):
                    raise RuntimeError("No successor is published")
                if int(metadata["slurm_job_id"]) != current.slurm_job_id:
                    raise RuntimeError("Current allocation changed; retry promotion")
                successor = self._allocation_from_record(
                    metadata["successor"], current.session_dir
                )
                if self._get_job_state(successor.slurm_job_id) != "RUNNING":
                    raise RuntimeError("Successor allocation is not RUNNING")
                if current.has_active_payloads(ssh_pool):
                    raise RuntimeError(
                        "Predecessor payloads are still active; request_drain and wait before promotion"
                    )
                self._load_allocation_nodes(successor)
                ray_options = current._read_ray_start_options(ssh_pool)
                if launcher is not None:
                    ray_options = {
                        "launcher": launcher,
                        "activation_script": activation_script
                        if activation_script is not None
                        else (ray_options or {}).get("activation_script", ""),
                    }
                if ray_options:
                    successor.ensure_ray_cluster(
                        ssh_pool=ssh_pool,
                        startup_timeout=startup_timeout,
                        **ray_options,
                    )
                updated = self._allocation_record(successor)
                updated["predecessors"] = [
                    *metadata.get("predecessors", []),
                    self._allocation_record(current),
                ]
                self._write_session_metadata(current.session_dir, updated)
                object.__setattr__(self, "_allocation", successor)
                self._finish_promotion(successor, updated)
                return successor

    def _finish_promotion(
        self, allocation: "SlurmAllocation", metadata: dict[str, Any]
    ) -> None:
        self._publish_allocation_tags(allocation)
        for record in metadata.get("predecessors", []):
            job_id = int(record["slurm_job_id"])
            if self._get_job_state(job_id) not in TERMINAL_STATES:
                self._require_ssh_pool().run(f"scancel {job_id}")

    def _publish_allocation_tags(self, allocation: "SlurmAllocation") -> None:
        context = self._context
        if context is None or not context.run:
            return
        run = context.instance.get_run_by_id(context.run.run_id)
        tags = {
            "dagster_slurm/job_id": str(allocation.slurm_job_id),
            _SESSION_ALLOCATION_DIR_TAG: allocation.session_dir,
            "dagster_slurm/run_dir": "",
        }
        if run is not None:
            tags.update(
                {
                    key: ""
                    for key in run.tags
                    if key.startswith("dagster_slurm/session_step_")
                }
            )
        context.instance.add_run_tags(context.run.run_id, tags)

    def _register_allocation_lease(self) -> None:
        if not self._allocation:
            return
        ssh_pool = self._require_ssh_pool()
        lease_id = f"{os.getpid()}-{threading.get_ident()}-{uuid.uuid4().hex}"
        lease_dir = f"{self._allocation.session_dir}/leases"
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
            f"{self._allocation.session_dir}/leases/{self._allocation_lease_id}.lease"
        )
        ssh_pool.run(f"rm -f {shlex.quote(lease_path)}")
        object.__setattr__(self, "_allocation_lease_id", None)

    def _schedule_shared_allocation_cleanup(self) -> None:
        if not self._allocation:
            return
        ssh_pool = self._require_ssh_pool()

        lease_dir = f"{self._allocation.session_dir}/leases"
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
    scancel --name={shlex.quote(posixpath.basename(self._allocation.session_dir))} 2>/dev/null || true
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
        session_dir: str | None = None,
    ):
        self.slurm_job_id = slurm_job_id
        self.nodes = nodes
        self.working_dir = working_dir
        self.session_dir = session_dir or working_dir
        self.config = config
        self.logger = get_dagster_logger()
        self._failed_nodes: Set[str] = set()
        self._exec_count = 0
        self._exec_lock = threading.Lock()
        self._ray_cluster_lock = threading.Lock()
        self._ray_address: Optional[str] = None
        self._ray_dashboard_url: Optional[str] = None
        self._ray_fingerprint: Optional[tuple[Any, ...]] = None

    @property
    def end_time(self) -> str | None:
        """Current scheduler EndTime in the cluster's timezone (None if unknown).

        Read on every access: backfill and TimeLimit updates can change it.
        """
        output = (
            self.config._require_ssh_pool()
            .run(f"squeue -h -j {self.slurm_job_id} -o %e")
            .strip()
        )
        return (
            output if output and output not in {"N/A", "Unknown", "UNLIMITED"} else None
        )

    def request_drain(self) -> None:
        """Ask the allocation shell to drain payload steps, keeping Ray alive."""
        signal_name = (
            self.config._effective_signal_before_timeout() or "TERM@1"
        ).split("@")[0]
        self.config._require_ssh_pool().run(
            f"scancel --batch --signal={shlex.quote(signal_name)} {self.slurm_job_id}"
        )

    def has_active_payloads(self, ssh_pool: SSHConnectionPool) -> bool:
        if self.config._get_job_state(self.slurm_job_id) in TERMINAL_STATES:
            return False
        output = ssh_pool.run(f"""
for invocation in {shlex.quote(self.working_dir)}/payloads/*; do
  [ -d "$invocation" ] || continue
  if [ ! -f "$invocation/status" ]; then printf active; break; fi
done
""")
        return bool(output.strip())

    @property
    def ray_dashboard_url(self) -> str | None:
        """Dashboard URL reported by the persistent Ray head."""
        return self._ray_dashboard_url

    def execute(
        self,
        execution_plan: ExecutionPlan,
        asset_key: str,
        run_dir: str,
        ssh_pool: SSHConnectionPool,
        step_update_callback: Callable[[SlurmStepExecutionResult], None] | None = None,
        poll_callback: Callable[[SlurmStepExecutionResult], None] | None = None,
        timeout: int | None = None,
    ) -> SlurmStepExecutionResult:
        """Execute plan in this allocation via srun."""
        with self._exec_lock:
            self._exec_count += 1
            exec_id = f"{self._exec_count}-{uuid.uuid4().hex}"

        auxiliary_scripts = getattr(execution_plan, "auxiliary_scripts", {})
        safe_auxiliary_scripts = [
            (_safe_auxiliary_script_name(aux_name), aux_content)
            for aux_name, aux_content in auxiliary_scripts.items()
        ]

        safe_asset_key = re.sub(r"[^A-Za-z0-9_.=-]+", "_", asset_key).strip("._-")
        if not safe_asset_key:
            safe_asset_key = "asset"
        script_name = f"asset_{exec_id}_{safe_asset_key}.sh"
        script_path = f"{run_dir}/{script_name}"
        invocation_dir = f"{self.working_dir}/payloads/{exec_id}"
        step_id_path = f"{invocation_dir}/id"
        drain_marker_path = f"{invocation_dir}/drain_signal"
        step_id_capture = (
            'printf \'%s.%s\\n\' "${SLURM_JOB_ID:?}" "${SLURM_STEP_ID:?}" > '
            f"{shlex.quote(step_id_path)}"
        )
        workload_path = f"{run_dir}/workload_{exec_id}.sh"
        ssh_pool.write_file("\n".join(execution_plan.payload), workload_path)
        supervisor = build_pre_timeout_supervisor_script(
            workload_path,
            self.config._effective_signal_before_timeout() or "TERM@1",
            drain_marker_path,
            registration_script=step_id_capture,
            drain_request_path=f"{self.working_dir}/drain_signal",
        )
        assert supervisor is not None
        ssh_pool.write_file(supervisor, script_path)
        ssh_pool.run(f"chmod +x {shlex.quote(script_path)}")

        for safe_aux_name, aux_content in safe_auxiliary_scripts:
            aux_path = f"{run_dir}/{safe_aux_name}"
            ssh_pool.write_file(aux_content, aux_path)
            ssh_pool.run(f"chmod +x {shlex.quote(aux_path)}")

        log_name = f"slurm-{self.slurm_job_id}-step-{exec_id}_{safe_asset_key}"
        stdout_path = f"{run_dir}/{log_name}.out"
        stderr_path = f"{run_dir}/{log_name}.err"
        status_path = f"{invocation_dir}/status"

        # Launch the step from a detached remote wrapper. The wrapper and its
        # status marker survive a local Dagster supervisor restart, allowing a
        # retry to reattach to this exact invocation instead of resubmitting it.
        srun_cmd = (
            f"srun --overlap --jobid={self.slurm_job_id} --nodes=1 --ntasks=1 "
            f"--job-name=asset_{exec_id} {shlex.quote(script_path)} "
            f"> {shlex.quote(stdout_path)} 2> {shlex.quote(stderr_path)}"
        )
        status_tmp_path = f"{status_path}.tmp"
        wrapper = (
            "set +e\n"
            f"{srun_cmd}\n"
            "status=$?\n"
            f"printf '%s\\n' \"$status\" > {shlex.quote(status_tmp_path)}\n"
            f"mv {shlex.quote(status_tmp_path)} {shlex.quote(status_path)}"
        )
        launch_cmd = (
            f"rm -f {shlex.quote(step_id_path)} {shlex.quote(status_path)} "
            f"{shlex.quote(status_tmp_path)}; "
            "nohup bash --noprofile --norc -c "
            f"{shlex.quote(wrapper)} </dev/null >/dev/null 2>&1 &"
        )

        self.logger.info(f"Executing in allocation {self.slurm_job_id}: {script_name}")
        # Registration and promotion share a lock, including steps queued in
        # srun that have not published their Slurm step ID yet.
        with self.config._session_lock(self.session_dir, ssh_pool=ssh_pool):
            metadata = ssh_pool.run(
                f"cat {shlex.quote(self.session_dir + '/allocation.json')} 2>/dev/null || true"
            ).strip()
            if (
                metadata
                and int(json.loads(metadata)["slurm_job_id"]) != self.slurm_job_id
            ):
                raise RuntimeError(
                    "Allocation was replaced; prepare the payload in the current allocation"
                )
            ssh_pool.run(f"mkdir -p {shlex.quote(invocation_dir)}")
            # Keep registration if SSH disconnects after launching srun: the
            # payload may still be active, so promotion must not discard it.
            ssh_pool.run(launch_cmd)
        result = SlurmStepExecutionResult(
            job_id=self.slurm_job_id,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            step_id_path=step_id_path,
            status_path=status_path,
            drain_marker_path=drain_marker_path,
        )
        if step_update_callback is not None:
            step_update_callback(result)

        return self.wait_for_step(
            result,
            ssh_pool=ssh_pool,
            step_update_callback=step_update_callback,
            poll_callback=poll_callback,
            timeout=timeout,
        )

    def wait_for_step(
        self,
        result: SlurmStepExecutionResult,
        *,
        ssh_pool: SSHConnectionPool,
        step_update_callback: Callable[[SlurmStepExecutionResult], None] | None = None,
        poll_callback: Callable[[SlurmStepExecutionResult], None] | None = None,
        timeout: int | None = None,
    ) -> SlurmStepExecutionResult:
        """Wait for a launched allocation step, including after supervisor restart."""
        if result.step_id_path is None or result.status_path is None:
            raise ValueError("Session step reattachment requires id and status paths")

        started_at = time.monotonic()
        current = result
        while True:
            step_id = ssh_pool.run(
                f"cat {shlex.quote(result.step_id_path)} 2>/dev/null || true"
            ).strip()
            if current.step_id is None and re.fullmatch(
                rf"{result.job_id}\.[A-Za-z0-9_+-]+",
                step_id,
            ):
                current = dataclass_replace(current, step_id=step_id)
                if step_update_callback is not None:
                    step_update_callback(current)

            status = ssh_pool.run(
                f"cat {shlex.quote(result.status_path)} 2>/dev/null || true"
            ).strip()
            drain_marker_path = result.drain_marker_path or (
                f"{posixpath.dirname(result.status_path)}/drain_signal"
            )
            drain_signal = (
                ssh_pool.run(
                    f"cat {shlex.quote(drain_marker_path)} 2>/dev/null || true"
                ).strip()
                or None
            )
            current = dataclass_replace(current, drain_signal=drain_signal)
            if status:
                if not re.fullmatch(r"\d+", status):
                    raise RuntimeError(
                        f"Invalid session step status {status!r} at {result.status_path}"
                    )
                return_code = int(status)
                current = dataclass_replace(current, exit_code=return_code)
                if return_code != 0 and not current.drained:
                    stdout_tail = _tail_remote_file_for_error(
                        ssh_pool, result.stdout_path
                    )
                    stderr_tail = _tail_remote_file_for_error(
                        ssh_pool, result.stderr_path
                    )
                    raise RuntimeError(
                        f"srun step failed in allocation {result.job_id} "
                        f"with exit code {return_code}. "
                        f"stdout_path={result.stdout_path}; "
                        f"stderr_path={result.stderr_path}\n"
                        f"--- stdout tail ---\n{stdout_tail}\n"
                        f"--- stderr tail ---\n{stderr_tail}"
                    )
                self.logger.info(
                    "Execution %s in allocation %s completed",
                    current.step_id or "<pending-step-id>",
                    result.job_id,
                )
                return current

            state_output = ssh_pool.run(
                f"squeue -h -j {result.job_id} -o '%T' 2>/dev/null || true"
            ).strip()
            if not state_output:
                state_output = ssh_pool.run(
                    f"sacct -X -n -j {result.job_id} -o State%20 2>/dev/null || true"
                ).strip()
            state = (
                normalize_slurm_state(state_output.split()[0]) if state_output else ""
            )
            if state in TERMINAL_STATES:
                raise SlurmAllocationEnded(
                    dataclass_replace(current, allocation_state=state)
                )

            if poll_callback is not None:
                poll_callback(current)
            if timeout is not None and time.monotonic() - started_at > timeout:
                raise TimeoutError(
                    f"Timed out after {timeout}s waiting for allocation step "
                    f"in job {result.job_id}"
                )
            time.sleep(1)

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

                        ray_dir = f"{self.working_dir}/ray_cluster"
                        ssh_pool.run(f"mkdir -p {shlex.quote(ray_dir)}")
                        options_path = f"{ray_dir}/launch.json"
                        options_tmp_path = f"{options_path}.{uuid.uuid4().hex}.tmp"
                        ssh_pool.write_file(
                            json.dumps(
                                {
                                    "launcher": launcher.model_dump(mode="json"),
                                    "activation_script": activation_script,
                                }
                            ),
                            options_tmp_path,
                        )
                        ssh_pool.run(
                            f"mv {shlex.quote(options_tmp_path)} {shlex.quote(options_path)}"
                        )
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

    def _read_ray_start_options(
        self, ssh_pool: SSHConnectionPool
    ) -> dict[str, Any] | None:
        output = ssh_pool.run(
            f"cat {shlex.quote(self.working_dir + '/ray_cluster/launch.json')} 2>/dev/null || true"
        ).strip()
        if not output:
            return None
        options = json.loads(output)
        return {
            "launcher": RayLauncher(**options["launcher"]),
            "activation_script": options["activation_script"],
        }

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
            getattr(launcher, "port_strategy", "random"),
            getattr(launcher, "port_config", RayPortConfig()).model_dump(mode="json"),
            getattr(launcher, "network_interface", None),
            getattr(launcher, "node_ip_address_command", None),
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
        self._ray_dashboard_url = (
            ssh_pool.run(
                f"cat {shlex.quote(ray_dir)}/dashboard_url 2>/dev/null || true"
            ).strip()
            or None
        )
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
        port_strategy = str(getattr(launcher, "port_strategy", "random"))
        port_config = getattr(launcher, "port_config", RayPortConfig())
        grace_period = int(getattr(launcher, "grace_period", 5))
        use_head_ip = str(getattr(launcher, "use_head_ip", True)).lower()
        dashboard_host = shlex.quote(
            str(getattr(launcher, "dashboard_host", "0.0.0.0"))
        )
        num_gpus = int(getattr(launcher, "num_gpus_per_node", 0))
        head_startup_timeout = int(getattr(launcher, "head_startup_timeout", 120))
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
            '  --node-manager-port="$node_manager_port" \\',
            '  --object-manager-port="$object_manager_port" \\',
            '  --ray-client-server-port="$ray_client_server_port" \\',
            '  --redis-shard-ports="$redis_shard_port" \\',
            '  --runtime-env-agent-port="$runtime_env_agent_port" \\',
            '  --dashboard-agent-grpc-port="$dashboard_agent_grpc_port" \\',
            '  --dashboard-agent-listen-port="$dashboard_agent_listen_port" \\',
            '  --metrics-export-port="$metrics_export_port" \\',
            '  --min-worker-port="$min_worker_port" \\',
            '  --max-worker-port="$max_worker_port" \\',
            '  --temp-dir="$RAY_TMP_DIR" \\',
            f"  --num-gpus={num_gpus} \\",
            "  --block",
        ]
        ray_start_lines.extend(
            f"  {arg}" for arg in (object_store_arg, redis_arg, start_args) if arg
        )
        for index in range(len(ray_start_lines) - 1):
            if not ray_start_lines[index].endswith("\\"):
                ray_start_lines[index] = f"{ray_start_lines[index]} \\"
        ray_start_command = "\n".join(ray_start_lines)
        port_assignments = _render_ray_port_assignments(
            ray_port=ray_port,
            dashboard_port=dashboard_port,
            port_strategy=port_strategy,
            port_config=port_config,
        )
        node_ip_override = launcher._render_node_ip_override("head_bind_addr")

        return f"""#!/bin/bash
set -euo pipefail
source {shlex.quote(activation_script)}
{pre_start}

{port_assignments}
{_render_ray_process_cleanup(grace_period)}

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
	{node_ip_override}
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
	  stop_ray_process "${{RAY_HEAD_PID:-}}"
	  if [[ -n "${{RAY_TMP_DIR:-}}" && -d "$RAY_TMP_DIR" ]]; then
	    echo "[$({date_fmt})] Removing $RAY_TMP_DIR..."
	    rm -rf "$RAY_TMP_DIR" 2>/dev/null || true
	  fi
	}}
	trap cleanup_ray EXIT INT TERM

	echo "[$({date_fmt})] Starting persistent Ray head at $ray_address"
	{ray_start_command} &
	RAY_HEAD_PID=$!

for i in $(seq 1 {head_startup_timeout}); do
  if ray status --address "$ray_address" &>/dev/null; then break; fi
  if ! kill -0 "$RAY_HEAD_PID" 2>/dev/null; then
    echo "ERROR: Persistent Ray head exited during startup" >&2
    exit 1
  fi
  if [[ "$i" -eq {head_startup_timeout} ]]; then
    echo "ERROR: Persistent Ray head failed to start" >&2
    exit 1
  fi
  sleep 1
done

echo "$ray_address" > {shlex.quote(ray_dir)}/ray_address.tmp
mv {shlex.quote(ray_dir)}/ray_address.tmp {shlex.quote(ray_dir)}/ray_address
echo "http://$head_adv:$dash_port" > {shlex.quote(ray_dir)}/dashboard_url.tmp
mv {shlex.quote(ray_dir)}/dashboard_url.tmp {shlex.quote(ray_dir)}/dashboard_url
echo "{RAY_DASHBOARD_URL_MARKER}http://$head_adv:$dash_port"
touch {shlex.quote(ray_dir)}/ray_ready
wait "$RAY_HEAD_PID"
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
        ray_port = int(getattr(launcher, "ray_port", 6379))
        dashboard_port = int(getattr(launcher, "dashboard_port", 8265))
        port_strategy = str(getattr(launcher, "port_strategy", "random"))
        port_config = getattr(launcher, "port_config", RayPortConfig())
        grace_period = int(getattr(launcher, "grace_period", 5))
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
        port_assignments = _render_ray_port_assignments(
            ray_port=ray_port,
            dashboard_port=dashboard_port,
            port_strategy=port_strategy,
            port_config=port_config,
        )
        node_ip_override = launcher._render_node_ip_override("worker_bind_addr")
        worker_node_ip_arg = (
            '  --node-ip-address="$worker_bind_addr" \\\n' if node_ip_override else ""
        )

        return f"""#!/bin/bash
	set -euo pipefail
	ray_address="$1"
	source {shlex.quote(activation_script)}
	{pre_start}
	{port_assignments}
	{_render_ray_process_cleanup(grace_period)}
	{node_ip_override}
	{temp_dir_setup}

	cleanup_ray() {{
	  echo "[$({date_fmt})] Stopping persistent Ray worker..."
	  stop_ray_process "${{RAY_WORKER_PID:-}}"
	  if [[ -n "${{RAY_TMP_DIR:-}}" && -d "$RAY_TMP_DIR" ]]; then
	    echo "[$({date_fmt})] Removing $RAY_TMP_DIR..."
	    rm -rf "$RAY_TMP_DIR" 2>/dev/null || true
	  fi
	}}
	trap cleanup_ray EXIT INT TERM

	echo "[$({date_fmt})] Starting persistent Ray worker for $ray_address"
	ray start --address="$ray_address" --num-gpus={num_gpus} \
{worker_node_ip_arg}\
	  --node-manager-port="$node_manager_port" \
	  --object-manager-port="$object_manager_port" \
	  --ray-client-server-port="$ray_client_server_port" \
	  --runtime-env-agent-port="$runtime_env_agent_port" \
	  --dashboard-agent-grpc-port="$dashboard_agent_grpc_port" \
	  --dashboard-agent-listen-port="$dashboard_agent_listen_port" \
	  --metrics-export-port="$metrics_export_port" \
	  --min-worker-port="$min_worker_port" \
	  --max-worker-port="$max_worker_port" \
	  --temp-dir="$RAY_TMP_DIR" {redis_arg} --block &
	RAY_WORKER_PID=$!
	wait "$RAY_WORKER_PID"
	"""

    @staticmethod
    def _render_ray_temp_dir_setup(*, variable_name: str, date_fmt: str) -> str:
        if not variable_name.isidentifier():
            raise ValueError(f"Invalid shell variable name: {variable_name!r}")

        var_ref = f"${{{variable_name}}}"
        return f"""# Use a short, node-local Ray temp directory for sockets and runtime files.
	_ray_instance="${{_port_base:-${{port:-manual}}}}"
	if [[ -n "${{SLURM_TMPDIR:-}}" ]]; then
	  {variable_name}="${{SLURM_TMPDIR}}/r${{SLURM_JOB_ID:-manual}}-${{_ray_instance}}"
	  echo "[$({date_fmt})] Using SLURM_TMPDIR for Ray (node-local)"
	elif mkdir -p "/tmp/r${{SLURM_JOB_ID:-manual}}-${{_ray_instance}}" 2>/dev/null; then
	  {variable_name}="/tmp/r${{SLURM_JOB_ID:-manual}}-${{_ray_instance}}"
	  echo "[$({date_fmt})] Using /tmp for Ray (node-local)"
	elif mkdir -p "/var/tmp/r${{SLURM_JOB_ID:-manual}}-${{_ray_instance}}" 2>/dev/null; then
	  {variable_name}="/var/tmp/r${{SLURM_JOB_ID:-manual}}-${{_ray_instance}}"
	  echo "[$({date_fmt})] Using /var/tmp for Ray (node-local)"
	else
	  {variable_name}="$HOME/.r${{SLURM_JOB_ID:-manual}}-${{_ray_instance}}"
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
                self._ray_dashboard_url = (
                    ssh_pool.run(
                        f"cat {shlex.quote(ray_dir)}/dashboard_url 2>/dev/null || true"
                    ).strip()
                    or None
                )
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
