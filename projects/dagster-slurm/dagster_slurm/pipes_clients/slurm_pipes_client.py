"""Slurm Pipes client for remote execution."""

import uuid
import shutil
from pathlib import Path
from typing import Dict, Optional, Any, Iterator
from dagster import (
    AssetExecutionContext,
    PipesClient,
    PipesEnvContextInjector,
    open_pipes_session,
    get_dagster_logger,
)
from ..launchers.base import ComputeLauncher
from ..helpers.message_readers import SSHMessageReader
from ..helpers.env_packaging import (
    pack_environment_with_pixi,
)
from ..helpers.metrics import SlurmMetricsCollector
from ..helpers.ssh_pool import SSHConnectionPool
from ..helpers.ssh_helpers import TERMINAL_STATES
from ..resources.slurm import SlurmResource
from ..resources.session import SlurmSessionResource


class SlurmPipesClient(PipesClient):
    """
    Pipes client for Slurm execution.

    Handles:
    - Packaging environment with pixi pack
    - Uploading payload via SSH
    - Submitting to Slurm via sbatch (or srun in session mode)
    - Streaming results via SSH tail
    - Collecting metrics

    Works in two modes:
    1. Standalone: Each asset = separate sbatch job
    2. Session: Multiple assets share a Slurm allocation (operator fusion)
    """

    def __init__(
        self,
        slurm_resource: "SlurmResource",
        launcher: ComputeLauncher,
        session_resource: Optional["SlurmSessionResource"] = None,
        cleanup_on_failure: bool = True,
    ):
        """
        Args:
            slurm_resource: Slurm cluster configuration
            launcher: Launcher to generate execution plans
            session_resource: Optional session resource for operator fusion (changed name)
            cleanup_on_failure: Whether to cleanup remote files on failure
        """
        super().__init__()
        self.slurm = slurm_resource
        self.launcher = launcher
        self.session = session_resource  # Now holds SlurmSessionResource, not Manager
        self.cleanup_on_failure = cleanup_on_failure
        self.logger = get_dagster_logger()
        self.metrics_collector = SlurmMetricsCollector()

    def run(
        self,
        context: AssetExecutionContext,
        *,
        payload_path: str,
        python_executable: Optional[str] = None,
        extra_env: Optional[Dict[str, str]] = None,
        extras: Optional[Dict[str, Any]] = None,
        use_session: bool = False,
    ) -> Iterator:
        """
        Execute payload on Slurm cluster.

        Args:
            context: Dagster execution context
            payload_path: Local path to Python script
            python_executable: Python on cluster (default: from slurm_resource)
            extra_env: Additional environment variables
            extras: Extra data to pass via Pipes
            use_session: If True and session_resource provided, use shared allocation

        Yields:
            Dagster events
        """
        python_executable = python_executable or self.slurm.remote_python
        run_id = context.run_id or uuid.uuid4().hex

        # Setup SSH connection pool
        ssh_pool = SSHConnectionPool(self.slurm.ssh)

        run_dir = None
        temp_files = []
        job_id = None

        try:
            with ssh_pool:
                # Pack environment using pixi
                self.logger.info("Packing environment with pixi...")
                pack_file = pack_environment_with_pixi()

                # Setup remote directories
                remote_base = self.slurm.remote_base
                run_dir = f"{remote_base}/runs/{run_id}"
                messages_path = f"{run_dir}/messages.jsonl"

                ssh_pool.run(f"mkdir -p {run_dir}")

                # Upload packed environment
                self.logger.info(f"Uploading environment to {run_dir}...")
                ssh_pool.upload_file(str(pack_file), f"{run_dir}/{pack_file.name}")
                temp_files.append(f"{run_dir}/{pack_file.name}")

                # Generate and upload installer
                temp_dir = Path("/tmp") / f"dagster_{run_id}"
                temp_dir.mkdir(parents=True, exist_ok=True)

                # TODO: properly implement this with pack. we have the auto unpacker already
                raise NotImplementedError("Environment unpacking not implemented yet")
                # installer = generate_unpack_installer(pack_file, temp_dir)
                # ssh_pool.upload_file(str(installer), f"{run_dir}/environment.sh")
                temp_files.append(str(temp_dir))

                # Upload payload
                payload_name = Path(payload_path).name
                remote_payload = f"{run_dir}/{payload_name}"
                ssh_pool.upload_file(payload_path, remote_payload)

                # Setup Pipes communication
                context_injector = PipesEnvContextInjector()
                message_reader = SSHMessageReader(messages_path, self.slurm.ssh)

                with open_pipes_session(
                    context=context,
                    context_injector=context_injector,
                    message_reader=message_reader,
                    extras=extras,
                ) as session:
                    pipes_env = session.get_bootstrap_env_vars()

                    # Generate execution plan
                    allocation_context = None

                    # CHANGED: Access allocation through session resource
                    if use_session and self.session and self.session._initialized:
                        allocation = self.session._allocation
                        if allocation:
                            allocation_context = {
                                "nodes": allocation.nodes,
                                "num_nodes": len(allocation.nodes),
                                "head_node": allocation.nodes[0]
                                if allocation.nodes
                                else None,
                                "slurm_job_id": allocation.slurm_job_id,
                            }

                    execution_plan = self.launcher.prepare_execution(
                        payload_path=remote_payload,
                        python_executable=python_executable,
                        working_dir=run_dir,
                        pipes_context=pipes_env,
                        extra_env=extra_env,
                        allocation_context=allocation_context,
                    )

                    # Execute
                    if use_session and self.session:
                        self.logger.info("Executing in Slurm session")
                        job_id = self._execute_in_session(
                            execution_plan=execution_plan,
                            context=context,
                        )
                    else:
                        self.logger.info("Executing as standalone Slurm job")
                        job_id = self._execute_standalone(
                            execution_plan=execution_plan,
                            run_dir=run_dir,
                            ssh_pool=ssh_pool,
                        )

                    self.logger.info(f"Job {job_id} completed")

                    # Collect metrics
                    try:
                        metrics = self.metrics_collector.collect_job_metrics(
                            job_id, ssh_pool
                        )
                        context.add_output_metadata(
                            {
                                "slurm_job_id": job_id,
                                "node_hours": metrics.node_hours,
                                "cpu_efficiency_pct": round(
                                    metrics.cpu_efficiency * 100, 2
                                ),
                                "max_memory_mb": round(metrics.max_rss_mb, 2),
                                "elapsed_seconds": round(metrics.elapsed_seconds, 2),
                            }
                        )
                    except Exception as e:
                        self.logger.warning(f"Failed to collect metrics: {e}")

                    # Yield results
                    for event in session.get_results():
                        yield event

                # Cleanup temp files (keep run_dir for debugging)
                self._cleanup_temp_files(temp_files)

        except Exception as e:
            self.logger.error(f"Execution failed: {e}")

            # Cleanup on failure
            if self.cleanup_on_failure and run_dir:
                try:
                    with ssh_pool:
                        ssh_pool.run(f"rm -rf {run_dir}")
                except Exception as cleanup_error:
                    self.logger.warning(f"Cleanup failed: {cleanup_error}")

            raise

    def _execute_in_session(
        self,
        execution_plan,
        context: AssetExecutionContext,
    ) -> int:
        """
        Execute in shared Slurm allocation.

        CHANGED: Now calls session.execute_in_session() directly
        """
        if not self.session:
            raise RuntimeError("Session resource not provided")

        if not self.session._initialized:
            raise RuntimeError(
                "Session not initialized. "
                "Ensure ComputeResource properly initializes the session."
            )

        # Delegate to session resource
        return self.session.execute_in_session(
            execution_plan=execution_plan,
            asset_key=str(context.asset_key),
        )

    def _execute_standalone(
        self,
        execution_plan,
        run_dir: str,
        ssh_pool: SSHConnectionPool,
    ) -> int:
        """Execute as standalone sbatch job."""

        if execution_plan.kind != "shell_script":
            raise ValueError(
                f"Standalone mode only supports shell_script, got {execution_plan.kind}"
            )

        # Write script
        script_path = f"{run_dir}/job.sh"
        script_content = "\n".join(execution_plan.payload)
        ssh_pool.write_file(script_content, script_path)
        ssh_pool.run(f"chmod +x {script_path}")

        # Build sbatch command
        sbatch_opts = [
            f"-J dagster_{self.slurm.remote_base.split('/')[-1]}",
            f"-D {run_dir}",
            f"-o {run_dir}/slurm-%j.out",
            f"-e {run_dir}/slurm-%j.err",
            f"-t {self.slurm.queue.time_limit}",
            f"-c {self.slurm.queue.cpus}",
            f"--mem={self.slurm.queue.mem}",
        ]

        if self.slurm.queue.partition:
            sbatch_opts.append(f"-p {self.slurm.queue.partition}")

        sbatch_cmd = f"sbatch {' '.join(sbatch_opts)} {script_path}"

        # Submit
        output = ssh_pool.run(sbatch_cmd)

        # Parse job ID
        import re

        match = re.search(r"Submitted batch job (\d+)", output)
        if not match:
            raise RuntimeError(f"Could not parse job ID from:\n{output}")

        job_id = int(match.group(1))
        self.logger.info(f"Submitted job {job_id}")

        # Wait for completion
        self._wait_for_job(job_id, ssh_pool)

        return job_id

    def _wait_for_job(self, job_id: int, ssh_pool: SSHConnectionPool):
        """Poll Slurm until job completes."""
        import time

        self.logger.info(f"Waiting for job {job_id}...")

        while True:
            state = self._get_job_state(job_id, ssh_pool)

            if not state:
                self.logger.warning("Job state unknown, assuming complete")
                break

            if state in TERMINAL_STATES:
                self.logger.info(f"Job {job_id} finished: {state}")
                if state != "COMPLETED":
                    raise RuntimeError(f"Job {job_id} failed with state: {state}")
                break

            time.sleep(5)

    def _get_job_state(self, job_id: int, ssh_pool: SSHConnectionPool) -> str:
        """Query job state from Slurm."""
        try:
            # Try squeue first
            output = ssh_pool.run(f"squeue -h -j {job_id} -o '%T' 2>/dev/null || true")
            state = output.strip()
            if state:
                return state

            # Fall back to sacct
            output = ssh_pool.run(
                f"sacct -X -n -j {job_id} -o State 2>/dev/null || true"
            )
            state = output.strip()
            return state.split()[0] if state else ""

        except Exception as e:
            self.logger.warning(f"Error querying job state: {e}")
            return ""

    def _cleanup_temp_files(self, temp_files: list):
        """Remove temporary files."""
        for path in temp_files:
            try:
                if Path(path).is_dir():
                    shutil.rmtree(path)
                elif Path(path).exists():
                    Path(path).unlink()
            except Exception as e:
                self.logger.warning(f"Failed to cleanup {path}: {e}")
