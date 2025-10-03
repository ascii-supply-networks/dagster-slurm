"""Slurm Pipes client for remote execution."""

import uuid
import shutil
import platform
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
from ..helpers.env_packaging import pack_environment_with_pixi
from ..helpers.metrics import SlurmMetricsCollector
from ..helpers.ssh_pool import SSHConnectionPool
from ..helpers.ssh_helpers import TERMINAL_STATES
from ..resources.slurm import SlurmResource
from ..resources.session import SlurmSessionResource


class SlurmPipesClient(PipesClient):
    """
    Pipes client for Slurm execution.

    Handles:
    - Packaging environment with pixi pack (self-extracting executable)
    - Uploading and extracting environment via SSH
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
        debug_mode: bool = False,
        auto_detect_platform: bool = True,
        pack_platform: Optional[str] = None,
    ):
        """
        Args:
            slurm_resource: Slurm cluster configuration
            launcher: Launcher to generate execution plans
            session_resource: Optional session resource for operator fusion
            cleanup_on_failure: Whether to cleanup remote files on failure (ignored if debug_mode=True)
            debug_mode: If True, NEVER cleanup files (for debugging)
            auto_detect_platform: Auto-detect platform for pack command
            pack_platform: Override platform ('linux-64', 'linux-aarch64', 'osx-arm64')
        """
        super().__init__()
        self.slurm = slurm_resource
        self.launcher = launcher
        self.session = session_resource
        self.cleanup_on_failure = cleanup_on_failure
        self.debug_mode = debug_mode
        self.auto_detect_platform = auto_detect_platform
        self.pack_platform = pack_platform
        self.logger = get_dagster_logger()
        self.metrics_collector = SlurmMetricsCollector()

    def run(
        self,
        context: AssetExecutionContext,
        *,
        payload_path: str,
        extra_env: Optional[Dict[str, str]] = None,
        extras: Optional[Dict[str, Any]] = None,
        use_session: bool = False,
    ) -> Iterator:
        """
        Execute payload on Slurm cluster.

        Args:
            context: Dagster execution context
            payload_path: Local path to Python script
            extra_env: Additional environment variables
            extras: Extra data to pass via Pipes
            use_session: If True and session_resource provided, use shared allocation

        Yields:
            Dagster events
        """
        run_id = context.run_id or uuid.uuid4().hex

        # Setup SSH connection pool
        ssh_pool = SSHConnectionPool(self.slurm.ssh)
        run_dir = None
        job_id = None

        try:
            with ssh_pool:
                # Detect platform for packing
                pack_cmd = self._get_pack_command()

                # Pack environment using pixi
                self.logger.info(
                    f"Packing environment with command: {' '.join(pack_cmd)}"
                )
                pack_file = pack_environment_with_pixi(pack_cmd=pack_cmd)

                # Setup remote directories - expand $HOME properly
                remote_base = self._get_remote_base(run_id, ssh_pool)
                run_dir = f"{remote_base}/runs/{run_id}"
                messages_path = f"{run_dir}/messages.jsonl"
                env_dir = f"{run_dir}/env"

                self.logger.info(f"Creating remote directory: {run_dir}")
                ssh_pool.run(f"mkdir -p {run_dir}")
                ssh_pool.run(f"mkdir -p {env_dir}")

                # Verify directory creation
                verify_result = ssh_pool.run(f"ls -la {run_dir}")
                self.logger.debug(f"Remote directory contents:\n{verify_result}")

                # Upload packed environment (self-extracting executable)
                self.logger.info(
                    f"Uploading environment ({pack_file.name}) to {run_dir}..."
                )
                remote_pack_file = f"{run_dir}/{pack_file.name}"

                # Use absolute path for upload
                ssh_pool.upload_file(str(pack_file.absolute()), remote_pack_file)

                # Verify upload
                verify_upload = ssh_pool.run(f"ls -lh {remote_pack_file}")
                self.logger.info(f"Upload verified:\n{verify_upload}")

                # Extract environment
                self.logger.info("Extracting environment on remote host...")
                activation_script = self._extract_environment(
                    ssh_pool=ssh_pool,
                    pack_file_path=remote_pack_file,
                    extract_dir=env_dir,
                )

                # Python executable from extracted environment
                python_executable = f"{env_dir}/bin/python"

                # Upload payload
                payload_name = Path(payload_path).name
                remote_payload = f"{run_dir}/{payload_name}"
                ssh_pool.upload_file(payload_path, remote_payload)

                # Setup Pipes communication
                context_injector = PipesEnvContextInjector()
                message_reader = SSHMessageReader(
                    messages_path,
                    self.slurm.ssh,
                    control_path=ssh_pool.control_path,
                    reconnect_interval=2.0,  # Optional: customize
                    max_reconnect_attempts=10,  # Optional: customize
                )

                with open_pipes_session(
                    context=context,
                    context_injector=context_injector,
                    message_reader=message_reader,
                    extras=extras,
                ) as session:
                    pipes_env = session.get_bootstrap_env_vars()

                    # Generate execution plan
                    allocation_context = None
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
                        activation_script=activation_script,  # Pass to launcher
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

                # Don't cleanup in debug mode
                if self.debug_mode:
                    self.logger.warning(
                        f"🐛 DEBUG MODE: Keeping remote directory for inspection: {run_dir}"
                    )
                    self.logger.warning(
                        f"   To inspect: ssh {self.slurm.ssh.user}@{self.slurm.ssh.host} -p {self.slurm.ssh.port}"
                    )
                    self.logger.warning(f"   Directory: {run_dir}")

        except Exception as e:
            self.logger.error(f"Execution failed: {e}")

            # Cleanup on failure (unless debug mode)
            if self.cleanup_on_failure and not self.debug_mode and run_dir:
                try:
                    with ssh_pool:
                        ssh_pool.run(f"rm -rf {run_dir}")
                        self.logger.info(f"Cleaned up remote directory: {run_dir}")
                except Exception as cleanup_error:
                    self.logger.warning(f"Cleanup failed: {cleanup_error}")
            elif self.debug_mode:
                self.logger.warning(
                    f"🐛 DEBUG MODE: Keeping failed run directory for inspection: {run_dir}"
                )
                self.logger.warning(
                    f"   To inspect: ssh {self.slurm.ssh.user}@{self.slurm.ssh.host} -p {self.slurm.ssh.port}"
                )
                self.logger.warning(f"   Directory: {run_dir}")

            raise

    def _get_pack_command(self) -> list[str]:
        """
        Determine the appropriate pack command based on platform.

        Returns:
            List of command arguments for pixi pack
        """
        # If explicitly specified, use that
        if self.pack_platform:
            platform_map = {
                "linux-64": ["pixi", "run", "--frozen", "pack"],
                "linux-aarch64": ["pixi", "run", "--frozen", "pack-aarch"],
                "osx-arm64": ["pixi", "run", "--frozen", "pack-aarch"],  # docker
            }
            cmd = platform_map.get(self.pack_platform)
            if not cmd:
                raise ValueError(
                    f"Unknown pack_platform: {self.pack_platform}. "
                    f"Valid options: {list(platform_map.keys())}"
                )
            return cmd

        # Auto-detect if enabled
        if self.auto_detect_platform:
            system = platform.system().lower()
            machine = platform.machine().lower()

            self.logger.info(f"Auto-detected platform: {system}/{machine}")

            # Map to pack command
            if system == "darwin" and "arm" in machine:
                # macOS ARM (M1/M2/M3)
                self.logger.info("Using pack-aarch for macOS ARM")
                return ["pixi", "run", "--frozen", "pack-aarch"]
            elif system == "linux" and ("aarch64" in machine or "arm" in machine):
                # Linux ARM
                self.logger.info("Using pack-aarch for Linux ARM")
                return ["pixi", "run", "--frozen", "pack-aarch"]
            else:
                # Default to x86_64
                self.logger.info("Using pack for x86_64")
                return ["pixi", "run", "--frozen", "pack"]

        # Default fallback
        return ["pixi", "run", "--frozen", "pack"]

    def _get_remote_base(self, run_id: str, ssh_pool: SSHConnectionPool) -> str:
        """
        Get remote base directory with proper expansion of $HOME.

        Args:
            run_id: Dagster run ID
            ssh_pool: SSH connection pool to query remote home

        Returns:
            Expanded remote base directory path
        """
        if hasattr(self.slurm, "remote_base") and self.slurm.remote_base:
            remote_base = self.slurm.remote_base
        else:
            # Query actual HOME directory from remote
            home_dir = ssh_pool.run("echo $HOME").strip()
            remote_base = f"{home_dir}/pipelines"
            self.logger.info(f"No remote_base configured, using: {remote_base}")

        # Expand $HOME if present
        if "$HOME" in remote_base:
            home_dir = ssh_pool.run("echo $HOME").strip()
            remote_base = remote_base.replace("$HOME", home_dir)
            self.logger.debug(f"Expanded $HOME to: {remote_base}")

        return remote_base

    def _extract_environment(
        self,
        ssh_pool: SSHConnectionPool,
        pack_file_path: str,
        extract_dir: str,
    ) -> str:
        """
        Extract the self-extracting environment.

        The pixi-pack self-extracting executable will create a complete
        conda/pixi environment with bin/python and all dependencies.

        Also creates an activate.sh script in the run directory.

        Args:
            ssh_pool: SSH connection pool
            pack_file_path: Remote path to packed environment (environment.sh)
            extract_dir: Directory to extract into

        Returns:
            Path to activation script
        """
        # Make the pack file executable
        self.logger.debug(f"Making executable: {pack_file_path}")
        ssh_pool.run(f"chmod +x {pack_file_path}")

        # Execute the self-extracting script
        # The pixi-pack executable extracts to parent directory by default
        # and creates an activate.sh there
        run_dir = str(Path(extract_dir).parent)
        extract_cmd = f"cd {run_dir} && {pack_file_path}"

        self.logger.debug(f"Running extraction command: {extract_cmd}")

        try:
            output = ssh_pool.run(extract_cmd, timeout=600)  # 10 minute timeout
            self.logger.info(f"Extraction output:\n{output}")
        except Exception as e:
            self.logger.error(f"Environment extraction failed: {e}")
            # Try to get more details
            try:
                ls_output = ssh_pool.run(f"ls -la {run_dir}")
                self.logger.error(f"Run directory contents:\n{ls_output}")
            except:
                pass
            raise RuntimeError(
                f"Failed to extract environment from {pack_file_path}"
            ) from e

        # Verify extraction - check for activate.sh and python
        activation_script = f"{run_dir}/activate.sh"
        verify_cmd = f"test -f {activation_script} && test -f {extract_dir}/bin/python"

        try:
            ssh_pool.run(verify_cmd)
            # Also list what we got
            ls_result = ssh_pool.run(f"ls -la {run_dir}")
            self.logger.info(f"Environment extracted successfully")
            self.logger.info(f"Activation script: {activation_script}")

            self.logger.info(f"Python executable: {extract_dir}/bin/python")
            self.logger.debug(f"Run directory contents:\n{ls_result}")

            # Verify activation script works
            test_activate = ssh_pool.run(
                f"bash -c 'source {activation_script} && which python'"
            )
            self.logger.debug(
                f"Activation test - Python location: {test_activate.strip()}"
            )

            return activation_script

        except Exception as e:
            # Try to debug what went wrong
            try:
                tree_output = ssh_pool.run(f"ls -laR {run_dir} | head -100")
                self.logger.error(f"Files in run dir:\n{tree_output}")
            except:
                pass
            raise RuntimeError(
                f"Environment extraction appeared to succeed but validation failed. "
                f"Expected activate.sh in {run_dir} and bin/python in {extract_dir}"
            ) from e

    def _execute_in_session(
        self,
        execution_plan,
        context: AssetExecutionContext,
    ) -> int:
        """Execute in shared Slurm allocation."""
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
            f"-J dagster_{run_dir.split('/')[-1]}",
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
