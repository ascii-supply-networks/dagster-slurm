"""Slurm session management for operator fusion."""

import re
import shlex
import time
import threading
from typing import Dict, List, Optional, Any, Set
from dagster import ConfigurableResource, InitResourceContext, get_dagster_logger
from ..helpers.ssh_pool import SSHConnectionPool
from ..helpers.ssh_helpers import TERMINAL_STATES
from ..launchers.base import ExecutionPlan
from ..resources.slurm import SlurmResource
from pydantic import Field, PrivateAttr


class SlurmSessionResource(ConfigurableResource):
    """
    Slurm session resource for operator fusion.

    This is a proper Dagster resource that manages the lifecycle
    of a Slurm allocation across multiple assets in a run.

    Usage in definitions.py:
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

    # Private attributes for state management
    _allocation: Optional["SlurmAllocation"] = PrivateAttr(default=None)
    _ssh_pool: Optional[SSHConnectionPool] = PrivateAttr(default=None)
    _execution_semaphore: Optional[threading.Semaphore] = PrivateAttr(default=None)
    _initialized: bool = PrivateAttr(default=False)

    def setup_for_execution(
        self, context: InitResourceContext
    ) -> "SlurmSessionResource":
        """
        Called by Dagster when resource is initialized for a run.
        This is the proper Dagster resource lifecycle hook.
        """
        if self._initialized:
            return self

        self.logger = get_dagster_logger()
        self.context = context
        self._execution_semaphore = threading.Semaphore(self.max_concurrent_jobs)

        # Only create allocation if session mode is enabled
        if self.enable_session:
            # Start SSH pool
            self._ssh_pool = SSHConnectionPool(self.slurm.ssh)
            self._ssh_pool.__enter__()

            # Create allocation
            self._allocation = self._create_allocation()
            self.logger.info(
                f"Session resource initialized with allocation {self._allocation.slurm_job_id}"
            )
        else:
            self.logger.info("Session mode disabled")

        self._initialized = True

        return self

    def teardown_after_execution(self, context: InitResourceContext) -> None:
        """
        Called by Dagster when resource is torn down after run completion.
        This is the proper Dagster resource lifecycle hook.
        """
        if not self._initialized:
            return

        self.logger.info("Tearing down session resource...")

        # Cancel allocation
        if self._allocation:
            try:
                self._allocation.cancel(self._ssh_pool)
                self.logger.info(f"Allocation {self._allocation.slurm_job_id} canceled")
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
    ) -> int:
        """
        Execute workload in the shared allocation.
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
        with self._execution_semaphore:
            # Health check
            if self.enable_health_checks and not self._allocation.is_healthy(
                self._ssh_pool
            ):
                raise RuntimeError(
                    f"Allocation unhealthy. Failed nodes: {self._allocation.get_failed_nodes()}"
                )

            # Execute
            return self._allocation.execute(
                execution_plan=execution_plan,
                asset_key=asset_key,
                ssh_pool=self._ssh_pool,
            )

    def _create_allocation(self) -> "SlurmAllocation":
        """Start new Slurm allocation."""
        allocation_id = f"dagster_{self.context.run_id}"
        working_dir = f"{self.slurm.remote_base}/allocations/{allocation_id}"

        # Create working directory
        self._ssh_pool.run(f"mkdir -p {working_dir}")

        # Build allocation script
        partition = self.partition or self.slurm.queue.partition
        script_lines = [
            "#!/bin/bash",
            f"#SBATCH --job-name={allocation_id}",
            f"#SBATCH --nodes={self.num_nodes}",
            f"#SBATCH --time={self.time_limit}",
            f"#SBATCH --output={working_dir}/allocation_%j.log",
        ]

        if partition:
            script_lines.append(f"#SBATCH --partition={partition}")

        script_lines.extend(
            [
                "",
                "# Keep allocation alive for srun jobs",
                "echo 'Allocation started'",
                f"hostname > {working_dir}/head_node.txt",
                f"scontrol show hostname $SLURM_JOB_NODELIST > {working_dir}/nodes.txt",
                "",
                "# Wait for cancellation",
                "sleep infinity",
            ]
        )

        # Submit allocation
        script_path = f"{working_dir}/allocation.sh"
        self._ssh_pool.write_file("\n".join(script_lines), script_path)
        self._ssh_pool.run(f"chmod +x {script_path}")

        submit_cmd = f"sbatch {script_path}"
        output = self._ssh_pool.run(submit_cmd)

        match = re.search(r"Submitted batch job (\d+)", output)
        if not match:
            raise RuntimeError(f"Could not parse job ID from:\n{output}")

        job_id = int(match.group(1))
        self.logger.info(f"Allocation submitted: job {job_id}")

        # Wait for allocation to start
        self._wait_for_allocation_start(job_id, working_dir, timeout=120)

        # Read node list
        nodes_output = self._ssh_pool.run(f"cat {working_dir}/nodes.txt")
        nodes = [n.strip() for n in nodes_output.strip().split("\n") if n.strip()]

        self.logger.info(f"Allocation ready: {len(nodes)} nodes: {nodes}")

        return SlurmAllocation(
            slurm_job_id=job_id,
            nodes=nodes,
            working_dir=working_dir,
            config=self,
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
                    self._ssh_pool.run(f"test -f {working_dir}/head_node.txt")
                    return
                except:
                    pass
            elif state in TERMINAL_STATES:
                raise RuntimeError(f"Allocation {job_id} failed with state: {state}")

            time.sleep(2)

        raise TimeoutError(f"Allocation {job_id} not ready after {timeout}s")

    def _get_job_state(self, job_id: int) -> str:
        """Query job state."""
        try:
            output = self._ssh_pool.run(
                f"squeue -h -j {job_id} -o '%T' 2>/dev/null || true"
            )
            state = output.strip()
            if state:
                return state

            output = self._ssh_pool.run(
                f"sacct -X -n -j {job_id} -o State 2>/dev/null || true"
            )
            state = output.strip()
            return state.split()[0] if state else ""
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

    def execute(
        self,
        execution_plan: ExecutionPlan,
        asset_key: str,
        ssh_pool: SSHConnectionPool,
    ) -> int:
        """Execute plan in this allocation via srun."""

        if execution_plan.kind != "shell_script":
            raise ValueError(
                f"Session mode only supports shell_script plans, got {execution_plan.kind}"
            )

        with self._exec_lock:
            self._exec_count += 1
            exec_id = self._exec_count

        script_lines = execution_plan.payload

        # Write script
        script_name = f"asset_{exec_id}_{asset_key.replace('/', '_')}.sh"
        script_path = f"{self.working_dir}/{script_name}"
        ssh_pool.write_file("\n".join(script_lines), script_path)
        ssh_pool.run(f"chmod +x {script_path}")

        # Execute via srun
        srun_cmd = (
            f"srun --jobid={self.slurm_job_id} --job-name=asset_{exec_id} {script_path}"
        )

        self.logger.info(f"Executing in allocation {self.slurm_job_id}: {script_name}")
        ssh_pool.run(srun_cmd)
        self.logger.info(
            f"Execution {exec_id} in allocation {self.slurm_job_id} completed"
        )

        return exec_id

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
                f"srun --jobid={self.slurm_job_id} "
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
