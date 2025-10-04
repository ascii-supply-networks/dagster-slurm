"""Heterogeneous job manager for dynamic resource allocation."""

import re
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from dagster import get_dagster_logger

from ..helpers.ssh_helpers import TERMINAL_STATES
from ..helpers.ssh_pool import SSHConnectionPool
from ..resources.slurm import SlurmResource


@dataclass
class HetJobComponent:
    """Single component of heterogeneous job."""

    asset_key: str
    nodes: int
    cpus_per_task: int
    mem: str
    gpus_per_node: int = 0
    time_limit: str = "01:00:00"
    script_path: str = ""
    partition: Optional[str] = None


class HeterogeneousJobManager:
    """Manages Slurm heterogeneous jobs.

    Submits multiple resource configurations in ONE sbatch,
    avoiding multiple queue waits.

    Example:
        manager = HeterogeneousJobManager(slurm, ssh_pool, "/path/to/work")

        components = [
            HetJobComponent("prep", nodes=1, cpus_per_task=8, mem="32G", ...),
            HetJobComponent("train", nodes=4, cpus_per_task=32, mem="128G", gpus_per_node=2, ...),
            HetJobComponent("infer", nodes=8, cpus_per_task=16, mem="64G", gpus_per_node=1, ...),
        ]

        job_id = manager.submit_heterogeneous_job(components, "ml_pipeline")
        manager.wait_for_completion(job_id)

    """

    def __init__(
        self,
        slurm_resource: SlurmResource,
        ssh_pool: SSHConnectionPool,
        working_dir: str,
    ):
        """Args:
        slurm_resource: Slurm configuration
        ssh_pool: Active SSH connection pool
        working_dir: Remote working directory.

        """
        self.slurm = slurm_resource
        self.ssh_pool = ssh_pool
        self.working_dir = working_dir
        self.logger = get_dagster_logger()

    def submit_heterogeneous_job(
        self,
        components: List[HetJobComponent],
        job_name: str,
    ) -> int:
        """Submit heterogeneous job with multiple resource configurations.
        
        Args:
            components: List of job components with different resources
            job_name: Name for the Slurm job
            
        Returns:
            Slurm job ID
            
        Example Slurm command generated:
            sbatch --het-group=0 --nodes=4 --gres=gpu:2 --mem=128G : \
                   --het-group=1 --nodes=1 --mem=32G : \
                   --het-group=2 --nodes=8 --gres=gpu:1 --mem=64G \
                   wrapper_script.sh

        """
        if not components:
            raise ValueError("Must provide at least one component")

        self.logger.info(
            f"Building heterogeneous job with {len(components)} components"
        )

        # Build heterogeneous sbatch command
        sbatch_parts = ["sbatch"]

        for i, comp in enumerate(components):
            if i > 0:
                sbatch_parts.append(":")  # Separator between het groups

            # Resource specification for this component
            sbatch_parts.extend(
                [
                    f"--het-group={i}",
                    f"--nodes={comp.nodes}",
                    f"--cpus-per-task={comp.cpus_per_task}",
                    f"--mem={comp.mem}",
                    f"--time={comp.time_limit}",
                ]
            )

            # Add GPU resources if requested
            if comp.gpus_per_node > 0:
                sbatch_parts.append(f"--gres=gpu:{comp.gpus_per_node}")

            # Add partition (component-specific or global)
            partition = comp.partition or self.slurm.queue.partition
            if partition:
                sbatch_parts.append(f"--partition={partition}")

        # Add common job options (after all het-group specs)
        sbatch_parts.extend(
            [
                f"--job-name={job_name}",
                f"--output={self.working_dir}/hetjob-%j.out",
                f"--error={self.working_dir}/hetjob-%j.err",
            ]
        )

        # Generate wrapper script that dispatches to components
        wrapper_script = self._generate_wrapper_script(components)
        wrapper_path = f"{self.working_dir}/hetjob_wrapper.sh"

        self.logger.debug(f"Writing wrapper script to {wrapper_path}")
        self.ssh_pool.write_file(wrapper_script, wrapper_path)
        self.ssh_pool.run(f"chmod +x {wrapper_path}")

        # Add wrapper script to command
        sbatch_parts.append(wrapper_path)
        sbatch_cmd = " ".join(sbatch_parts)

        self.logger.info(f"Submitting heterogeneous job: {sbatch_cmd}")

        # Submit job
        output = self.ssh_pool.run(sbatch_cmd)

        # Parse job ID
        match = re.search(r"Submitted batch job (\d+)", output)
        if not match:
            raise RuntimeError(f"Could not parse job ID from sbatch output:\n{output}")

        job_id = int(match.group(1))
        self.logger.info(f"✅ Heterogeneous job {job_id} submitted successfully")

        # Log component details
        for i, comp in enumerate(components):
            self.logger.info(
                f"  Component {i} ({comp.asset_key}): "
                f"{comp.nodes} nodes × {comp.cpus_per_task} CPUs × {comp.mem}"
                + (f" × {comp.gpus_per_node} GPUs" if comp.gpus_per_node > 0 else "")
            )

        return job_id

    def _generate_wrapper_script(self, components: List[HetJobComponent]) -> str:
        """Generate wrapper script that runs each component on its allocated resources.

        Uses SLURM_HET_SIZE and SLURM_PROCID to dispatch to correct component.

        Slurm automatically sets these environment variables:
        - SLURM_HET_SIZE: Total number of heterogeneous components
        - SLURM_PROCID: Current component ID (0, 1, 2, ...)
        - SLURM_JOB_ID: Overall job ID
        - SLURM_JOB_NODELIST: Nodes allocated to this component
        - SLURM_JOB_NUM_NODES: Number of nodes in this component
        """
        date_fmt = "date +%Y-%m-%dT%H:%M:%S%z"

        script = f"""#!/bin/bash
set -euo pipefail

echo "========================================"
echo "[$({date_fmt})] Heterogeneous Job Wrapper"
echo "========================================"
echo "Overall Job ID: $SLURM_JOB_ID"
echo "Het Component: $SLURM_PROCID / $SLURM_HET_SIZE"
echo "Nodes in this component: $SLURM_JOB_NUM_NODES"
echo "Node list: $SLURM_JOB_NODELIST"
echo "CPUs per task: $SLURM_CPUS_PER_TASK"
echo "Memory: $SLURM_MEM_PER_NODE"
echo "========================================"

# Component-specific log file
COMPONENT_LOG="{self.working_dir}/component_${{SLURM_PROCID}}.log"
exec > >(tee -a "$COMPONENT_LOG") 2>&1

# Dispatch based on heterogeneous group ID
case $SLURM_PROCID in
"""

        for i, comp in enumerate(components):
            script += f"""
  {i})
    echo "[$({date_fmt})] Running component {i}: {comp.asset_key}"
    echo "Resources: {comp.nodes} nodes, {comp.cpus_per_task} CPUs, {comp.mem} mem"
    """

            if comp.gpus_per_node > 0:
                script += f"""echo "GPUs: {comp.gpus_per_node} per node"
    """

            script += f"""
    # Verify script exists
    if [ ! -f {comp.script_path} ]; then
      echo "[$({date_fmt})] ERROR: Script not found: {comp.script_path}"
      exit 1
    fi
    
    # Run the component script
    echo "[$({date_fmt})] Executing: {comp.script_path}"
    bash {comp.script_path}
    exit_code=$?
    
    echo "[$({date_fmt})] Component {i} ({comp.asset_key}) finished with exit code $exit_code"
    exit $exit_code
    ;;
"""

        script += f"""
  *)
    echo "[$({date_fmt})] ERROR: Unknown het group $SLURM_PROCID"
    echo "Expected SLURM_PROCID in range 0-{len(components) - 1}"
    exit 1
    ;;
esac

echo "[$({date_fmt})] Component $SLURM_PROCID completed successfully"
"""

        return script

    def wait_for_completion(
        self,
        job_id: int,
        poll_interval: int = 5,
        timeout: Optional[int] = None,
    ):
        """Wait for heterogeneous job to complete.

        Args:
            job_id: Slurm job ID
            poll_interval: Seconds between status checks
            timeout: Maximum time to wait (None = infinite)

        Raises:
            RuntimeError: If job fails or times out

        """
        self.logger.info(f"Waiting for heterogeneous job {job_id} to complete...")

        start_time = time.time()

        while True:
            # Check timeout
            if timeout and (time.time() - start_time) > timeout:
                raise RuntimeError(
                    f"Heterogeneous job {job_id} timed out after {timeout} seconds"
                )

            # Get job state
            state = self._get_job_state(job_id)

            if not state:
                self.logger.warning(f"Job {job_id} state unknown, assuming complete")
                break

            if state in TERMINAL_STATES:
                elapsed = time.time() - start_time
                self.logger.info(
                    f"Job {job_id} finished: {state} (elapsed: {elapsed:.1f}s)"
                )

                if state != "COMPLETED":
                    # Get detailed failure info
                    self._log_failure_details(job_id)
                    raise RuntimeError(
                        f"Heterogeneous job {job_id} failed with state: {state}"
                    )

                break

            # Still running
            if time.time() - start_time > 60:  # Log every minute after first minute
                self.logger.debug(f"Job {job_id} still {state}...")

            time.sleep(poll_interval)

    def _get_job_state(self, job_id: int) -> str:
        """Query job state from Slurm.

        Args:
            job_id: Slurm job ID

        Returns:
            Job state string (e.g., "RUNNING", "COMPLETED", "FAILED")

        """
        try:
            # Try squeue first (for running jobs)
            output = self.ssh_pool.run(
                f"squeue -h -j {job_id} -o '%T' 2>/dev/null || true"
            )
            state = output.strip()
            if state:
                return state

            # Fall back to sacct (for completed jobs)
            output = self.ssh_pool.run(
                f"sacct -X -n -j {job_id} -o State 2>/dev/null || true"
            )
            state = output.strip()
            return state.split()[0] if state else ""

        except Exception as e:
            self.logger.warning(f"Error querying job state: {e}")
            return ""

    def _log_failure_details(self, job_id: int):
        """Log detailed information about failed job.

        Args:
            job_id: Slurm job ID

        """
        try:
            # Get job details from sacct
            output = self.ssh_pool.run(
                f"sacct -j {job_id} --format=JobID,State,ExitCode,Elapsed,NodeList -P"
            )
            self.logger.error(f"Job {job_id} details:\n{output}")

            # Try to get component logs
            for i in range(10):  # Check up to 10 components
                log_path = f"{self.working_dir}/component_{i}.log"
                try:
                    log_content = self.ssh_pool.run(
                        f"tail -50 {log_path} 2>/dev/null || true"
                    )
                    if log_content.strip():
                        self.logger.error(
                            f"Component {i} log (last 50 lines):\n{log_content}"
                        )
                except:  # noqa: E722
                    break

            # Get main job output
            try:
                job_out = self.ssh_pool.run(
                    f"tail -100 {self.working_dir}/hetjob-{job_id}.out 2>/dev/null || true"
                )
                if job_out.strip():
                    self.logger.error(f"Job output (last 100 lines):\n{job_out}")
            except:  # noqa: E722
                pass

            # Get main job errors
            try:
                job_err = self.ssh_pool.run(
                    f"tail -100 {self.working_dir}/hetjob-{job_id}.err 2>/dev/null || true"
                )
                if job_err.strip():
                    self.logger.error(f"Job errors (last 100 lines):\n{job_err}")
            except:  # noqa: E722
                pass

        except Exception as e:
            self.logger.warning(f"Could not retrieve failure details: {e}")

    def get_component_logs(self, job_id: int) -> Dict[int, str]:
        """Retrieve logs from all components.

        Args:
            job_id: Slurm job ID

        Returns:
            Dict mapping component ID to log content

        """
        logs = {}

        for i in range(20):  # Check up to 20 components
            log_path = f"{self.working_dir}/component_{i}.log"
            try:
                log_content = self.ssh_pool.run(f"cat {log_path} 2>/dev/null || true")
                if log_content.strip():
                    logs[i] = log_content
                else:
                    break  # No more components
            except:  # noqa: E722
                break

        return logs

    def cancel_job(self, job_id: int):
        """Cancel a running heterogeneous job.

        Args:
            job_id: Slurm job ID

        """
        self.logger.info(f"Canceling heterogeneous job {job_id}")
        try:
            self.ssh_pool.run(f"scancel {job_id}")
            self.logger.info(f"Job {job_id} canceled")
        except Exception as e:
            self.logger.error(f"Failed to cancel job {job_id}: {e}")
            raise
