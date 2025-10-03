"""Slurm Pipes client for remote execution."""

import uuid
import shutil
import platform
import subprocess
import sys
import time
import threading
import re
import signal
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
    Pipes client for Slurm execution with real-time log streaming and cancellation support.

    Features:
    - Real-time stdout/stderr streaming to Dagster logs
    - Packaging environment with pixi pack
    - Auto-reconnect message reading
    - Metrics collection
    - Graceful cancellation with Slurm job termination

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
            cleanup_on_failure: Whether to cleanup remote files on failure
            debug_mode: If True, never cleanup files (for debugging)
            auto_detect_platform: Auto-detect platform (ARM vs x86) for pixi pack
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
        self._control_path = None
        self._current_job_id = None
        self._ssh_pool = None
        self._cancellation_requested = False

    def run(
        self,
        context: AssetExecutionContext,
        *,
        payload_path: str,
        extra_env: Optional[Dict[str, str]] = None,
        extras: Optional[Dict[str, Any]] = None,
        use_session: bool = False,
        extra_slurm_opts: Optional[Dict[str, Any]] = None,
    ) -> Iterator:
        """
        Execute payload on Slurm cluster with real-time log streaming.

        Args:
            context: Dagster execution context
            payload_path: Local path to Python script
            extra_env: Additional environment variables
            extras: Extra data to pass via Pipes
            use_session: If True and session_resource provided, use shared allocation
            extra_slurm_opts: Override Slurm options (non-session mode)

        Yields:
            Dagster events
        """
        run_id = context.run_id or uuid.uuid4().hex

        # Setup SSH connection pool
        ssh_pool = SSHConnectionPool(self.slurm.ssh)
        self._ssh_pool = ssh_pool
        run_dir = None
        job_id = None

        # Setup cancellation handler
        def handle_cancellation(signum, frame):
            self.logger.warning(f"⚠️  Cancellation signal received (signal {signum})")
            self._cancellation_requested = True
            if self._current_job_id:
                self._cancel_slurm_job(self._current_job_id)

        # Register signal handlers
        original_sigint = signal.signal(signal.SIGINT, handle_cancellation)
        original_sigterm = signal.signal(signal.SIGTERM, handle_cancellation)

        try:
            with ssh_pool:
                # Store control path for log streaming
                self._control_path = ssh_pool.control_path

                # Pack environment using pixi
                self.logger.info("Packing environment with pixi...")
                pack_cmd = self._get_pack_command()
                pack_file = pack_environment_with_pixi(pack_cmd=pack_cmd)

                # Setup remote directories
                remote_base = self._get_remote_base(run_id, ssh_pool)
                run_dir = f"{remote_base}/runs/{run_id}"
                messages_path = f"{run_dir}/messages.jsonl"
                env_dir = f"{run_dir}/env"

                self.logger.debug(f"Creating remote directory: {run_dir}")
                ssh_pool.run(f"mkdir -p {run_dir}")
                ssh_pool.run(f"mkdir -p {env_dir}")

                # Check for cancellation
                if self._cancellation_requested:
                    raise RuntimeError("Execution cancelled before job submission")

                # Upload packed environment
                self.logger.debug(f"Uploading environment to {run_dir}...")
                remote_pack_file = f"{run_dir}/{pack_file.name}"
                ssh_pool.upload_file(str(pack_file.absolute()), remote_pack_file)

                # Extract environment
                self.logger.debug("Extracting environment on remote host...")
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
                    remote_path=messages_path,
                    ssh_config=self.slurm.ssh,
                    control_path=ssh_pool.control_path,
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
                        activation_script=activation_script,
                    )

                    # Check for cancellation before submission
                    if self._cancellation_requested:
                        raise RuntimeError("Execution cancelled before job submission")

                    # Execute with real-time log streaming
                    if use_session and self.session:
                        self.logger.info("Executing in Slurm session")
                        job_id = self._execute_in_session(
                            execution_plan=execution_plan,
                            context=context,
                            run_dir=run_dir,
                            ssh_pool=ssh_pool,
                        )
                    else:
                        self.logger.info("Executing as standalone Slurm job")
                        job_id = self._execute_standalone(
                            execution_plan=execution_plan,
                            run_dir=run_dir,
                            ssh_pool=ssh_pool,
                        )

                    self.logger.info(f"Job {job_id} completed successfully")

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

                # Cleanup (unless debug mode)
                if not self.debug_mode:
                    try:
                        ssh_pool.run(f"rm -rf {run_dir}")
                        self.logger.info(f"Cleaned up remote directory: {run_dir}")
                    except Exception as e:
                        self.logger.warning(f"Cleanup failed: {e}")

        except Exception as e:
            # Cancel job if it's running
            if self._current_job_id and not self._cancellation_requested:
                self.logger.warning(
                    f"Cancelling job {self._current_job_id} due to error"
                )
                self._cancel_slurm_job(self._current_job_id)

            self.logger.error(f"Execution failed: {e}")

            # Cleanup on failure (unless debug mode)
            if self.cleanup_on_failure and not self.debug_mode and run_dir:
                try:
                    with ssh_pool:
                        ssh_pool.run(f"rm -rf {run_dir}")
                        self.logger.info(f"Cleaned up remote directory: {run_dir}")
                except Exception as cleanup_error:
                    self.logger.warning(f"Cleanup failed: {cleanup_error}")
            elif self.debug_mode and run_dir:
                self.logger.warning(
                    f"🐛 DEBUG MODE: Keeping failed run directory for inspection: {run_dir}"
                )
                self.logger.warning(
                    f"   To inspect: ssh {self.slurm.ssh.user}@{self.slurm.ssh.host} -p {self.slurm.ssh.port}"
                )
                self.logger.warning(f"   Directory: {run_dir}")

            raise

        finally:
            # Restore original signal handlers
            signal.signal(signal.SIGINT, original_sigint)
            signal.signal(signal.SIGTERM, original_sigterm)

            # Clear state
            self._current_job_id = None
            self._ssh_pool = None
            self._cancellation_requested = False

    def _cancel_slurm_job(self, job_id: int):
        """
        Cancel a Slurm job.

        Args:
            job_id: Slurm job ID to cancel
        """
        if not self._ssh_pool:
            self.logger.warning(f"Cannot cancel job {job_id}: no SSH connection")
            return

        try:
            self.logger.warning(f"🛑 Cancelling Slurm job {job_id}...")
            self._ssh_pool.run(f"scancel {job_id}")
            self.logger.info(f"Slurm job {job_id} cancelled")
        except Exception as e:
            self.logger.error(f"Failed to cancel Slurm job {job_id}: {e}")

    def _get_pack_command(self) -> list[str]:
        """Determine the appropriate pack command based on platform."""
        if self.pack_platform:
            platform_map = {
                "linux-64": ["pixi", "run", "--frozen", "pack"],
                "linux-aarch64": ["pixi", "run", "--frozen", "pack-aarch"],
                "osx-arm64": ["pixi", "run", "--frozen", "pack-aarch"],
            }
            cmd = platform_map.get(self.pack_platform)
            if not cmd:
                raise ValueError(f"Unknown pack_platform: {self.pack_platform}")
            return cmd

        if self.auto_detect_platform:
            system = platform.system().lower()
            machine = platform.machine().lower()

            self.logger.info(f"Auto-detected platform: {system}/{machine}")

            if system == "darwin" and "arm" in machine:
                return ["pixi", "run", "--frozen", "pack-aarch"]
            elif system == "linux" and ("aarch64" in machine or "arm" in machine):
                return ["pixi", "run", "--frozen", "pack-aarch"]

        return ["pixi", "run", "--frozen", "pack"]

    def _get_remote_base(self, run_id: str, ssh_pool: SSHConnectionPool) -> str:
        """Get remote base directory with proper expansion of $HOME."""
        if hasattr(self.slurm, "remote_base") and self.slurm.remote_base:
            remote_base = self.slurm.remote_base
        else:
            home_dir = ssh_pool.run("echo $HOME").strip()
            remote_base = f"{home_dir}/pipelines"
            self.logger.info(f"No remote_base configured, using: {remote_base}")

        if "$HOME" in remote_base:
            home_dir = ssh_pool.run("echo $HOME").strip()
            remote_base = remote_base.replace("$HOME", home_dir)

        return remote_base

    def _extract_environment(
        self,
        ssh_pool: SSHConnectionPool,
        pack_file_path: str,
        extract_dir: str,
    ) -> str:
        """Extract the self-extracting environment and return activation script path."""
        ssh_pool.run(f"chmod +x {pack_file_path}")

        run_dir = str(Path(extract_dir).parent)
        extract_cmd = f"cd {run_dir} && {pack_file_path}"

        self.logger.debug(f"Running extraction command: {extract_cmd}")

        try:
            output = ssh_pool.run(extract_cmd, timeout=600)
            self.logger.debug(f"Extraction output:\n{output}")
        except Exception as e:
            self.logger.error(f"Environment extraction failed: {e}")
            raise RuntimeError(
                f"Failed to extract environment from {pack_file_path}"
            ) from e

        # Verify extraction
        activation_script = f"{run_dir}/activate.sh"
        verify_cmd = f"test -f {activation_script} && test -f {extract_dir}/bin/python"

        try:
            ssh_pool.run(verify_cmd)
            ls_result = ssh_pool.run(f"ls -la {run_dir}")
            self.logger.info(f"Environment extracted successfully")

            # Test activation
            test_activate = ssh_pool.run(
                f"bash -c 'source {activation_script} && which python'"
            )
            self.logger.debug(
                f"Activation test - Python location: {test_activate.strip()}"
            )

            return activation_script

        except Exception as e:
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
        run_dir: str,
        ssh_pool: SSHConnectionPool,
    ) -> int:
        """Execute in shared Slurm allocation with log streaming."""
        if not self.session:
            raise RuntimeError("Session resource not provided")
        if not self.session._initialized:
            raise RuntimeError("Session not initialized")

        # For session mode, we use srun which doesn't create separate log files
        # Logs go directly to the session allocation's output
        # The session resource handles execution
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
        """Execute as standalone sbatch job with real-time log streaming."""
        if execution_plan.kind != "shell_script":
            raise ValueError(
                f"Standalone mode only supports shell_script, got {execution_plan.kind}"
            )

        # Write script with error checking
        script_path = f"{run_dir}/job.sh"
        script_content = "\n".join(execution_plan.payload)

        # Verify script content is not empty
        if not script_content.strip():
            raise ValueError("Execution plan generated empty script content")

        self.logger.debug(
            f"Writing job script ({len(script_content)} bytes) to {script_path}"
        )

        try:
            ssh_pool.write_file(script_content, script_path)
        except Exception as e:
            self.logger.error(f"Failed to write job script: {e}")
            raise RuntimeError(f"Could not write job script to {script_path}") from e

        # Verify file was written
        try:
            verify_output = ssh_pool.run(
                f"test -f {script_path} && wc -l {script_path}"
            )
            self.logger.debug(f"Job script verified: {verify_output.strip()}")
        except Exception as e:
            self.logger.error(f"Job script verification failed: {e}")
            try:
                dir_listing = ssh_pool.run(f"ls -la {run_dir}")
                self.logger.error(f"Directory contents:\n{dir_listing}")
            except:
                pass
            raise RuntimeError(f"Job script was not created at {script_path}") from e

        # Make executable
        try:
            ssh_pool.run(f"chmod +x {script_path}")
        except Exception as e:
            self.logger.error(f"Failed to chmod job script: {e}")
            raise RuntimeError(f"Could not make job script executable") from e

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

        # Check for cancellation before submission
        if self._cancellation_requested:
            raise RuntimeError("Execution cancelled before job submission")

        # Submit
        self.logger.debug(f"Submitting: {sbatch_cmd}")
        output = ssh_pool.run(sbatch_cmd)

        # Parse job ID
        match = re.search(r"Submitted batch job (\d+)", output)
        if not match:
            raise RuntimeError(f"Could not parse job ID from:\n{output}")

        job_id = int(match.group(1))
        self._current_job_id = job_id
        self.logger.info(f"Submitted job {job_id}")

        # Wait for completion WITH live log streaming
        self._wait_for_job_with_streaming(job_id, ssh_pool, run_dir)

        return job_id

    def _wait_for_job_with_streaming(
        self,
        job_id: int,
        ssh_pool: SSHConnectionPool,
        run_dir: str,
    ):
        """
        Wait for job completion while streaming stdout/stderr to Dagster stdout/stderr.

        Args:
            job_id: Slurm job ID
            ssh_pool: SSH connection pool
            run_dir: Remote working directory
        """
        self.logger.info(f"Waiting for job {job_id} with live log streaming...")

        # Determine log file paths
        stdout_path = f"{run_dir}/slurm-{job_id}.out"
        stderr_path = f"{run_dir}/slurm-{job_id}.err"

        # Wait a moment for job to start and create log files
        time.sleep(2)

        # Start streaming threads
        stop_streaming = threading.Event()

        def stream_file(remote_path: str, output_stream, prefix: str):
            """Stream remote file to local output stream WITHOUT logging to Dagster."""
            try:
                # Build tail command
                tail_cmd = self._build_tail_command(remote_path)

                proc = subprocess.Popen(
                    tail_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    text=True,
                    bufsize=1,
                )

                try:
                    while not stop_streaming.is_set():
                        line = proc.stdout.readline()
                        if not line:
                            if proc.poll() is not None:
                                break
                            time.sleep(0.1)
                            continue

                        # Write ONLY to output stream (stdout or stderr)
                        output_stream.write(f"{prefix}{line}")
                        output_stream.flush()

                finally:
                    proc.terminate()
                    try:
                        proc.wait(timeout=2)
                    except:
                        proc.kill()

            except Exception as e:
                self.logger.warning(f"Error streaming {remote_path}: {e}")

        # Start stdout streaming thread
        stdout_thread = threading.Thread(
            target=stream_file,
            args=(stdout_path, sys.stdout, "[SLURM] "),
            daemon=True,
        )
        stdout_thread.start()

        # Start stderr streaming thread
        stderr_thread = threading.Thread(
            target=stream_file,
            args=(stderr_path, sys.stderr, "[SLURM ERR] "),
            daemon=True,
        )
        stderr_thread.start()

        # Poll job status
        try:
            while True:
                # Check for cancellation
                if self._cancellation_requested:
                    self.logger.warning("Cancellation detected during job execution")
                    raise RuntimeError(f"Job {job_id} was cancelled")

                state = self._get_job_state(job_id, ssh_pool)

                if not state:
                    self.logger.warning("Job state unknown, assuming complete")
                    break

                if state in TERMINAL_STATES:
                    self.logger.info(f"Job {job_id} finished: {state}")

                    # Give streaming threads time to catch up with final output
                    time.sleep(3)

                    if state == "CANCELLED":
                        raise RuntimeError(f"Job {job_id} was cancelled")
                    elif state != "COMPLETED":
                        raise RuntimeError(
                            f"Job {job_id} failed with state: {state}. "
                            f"Check stdout/stderr above for details."
                        )

                    break

                time.sleep(5)

        finally:
            # Stop streaming
            stop_streaming.set()

            # Wait for threads to finish
            stdout_thread.join(timeout=5)
            stderr_thread.join(timeout=5)

            # Clear current job ID
            self._current_job_id = None

    def _build_tail_command(self, remote_path: str) -> list[str]:
        """
        Build SSH tail command for streaming logs.

        Args:
            remote_path: Remote file path to tail

        Returns:
            Command list for subprocess.Popen
        """
        cmd = [
            "ssh",
            "-p",
            str(self.slurm.ssh.port),
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "LogLevel=ERROR",
        ]

        # Use ControlMaster if available
        if self._control_path:
            cmd.extend(
                [
                    "-o",
                    f"ControlPath={self._control_path}",
                    "-o",
                    "ControlMaster=no",
                ]
            )
        elif self.slurm.ssh.uses_key_auth:
            cmd.extend(
                [
                    "-i",
                    self.slurm.ssh.key_path,
                    "-o",
                    "IdentitiesOnly=yes",
                ]
            )

        cmd.extend(self.slurm.ssh.extra_opts)
        cmd.append(f"{self.slurm.ssh.user}@{self.slurm.ssh.host}")

        # Tail command - wait for file to appear, then follow
        cmd.append(
            f"tail -F --retry -n +1 {remote_path} 2>/dev/null || "
            f"tail -f {remote_path} 2>/dev/null || "
            f"sleep infinity"
        )

        return cmd

    def _get_job_state(self, job_id: int, ssh_pool: SSHConnectionPool) -> str:
        """Query job state from Slurm."""
        try:
            # Try squeue first (for running jobs)
            output = ssh_pool.run(f"squeue -h -j {job_id} -o '%T' 2>/dev/null || true")
            state = output.strip()
            if state:
                return state

            # Fall back to sacct (for completed jobs)
            output = ssh_pool.run(
                f"sacct -X -n -j {job_id} -o State 2>/dev/null || true"
            )
            state = output.strip()
            return state.split()[0] if state else ""

        except Exception as e:
            self.logger.warning(f"Error querying job state: {e}")
            return ""
