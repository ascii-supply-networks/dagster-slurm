"""Slurm Pipes client for remote execution."""

import platform
import re
import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

from dagster import (
    AssetExecutionContext,
    PipesClient,
    PipesEnvContextInjector,
    get_dagster_logger,
    open_pipes_session,
)

from ..helpers.env_packaging import pack_environment_with_pixi
from ..helpers.message_readers import SSHMessageReader
from ..helpers.metrics import SlurmMetricsCollector
from ..helpers.ssh_helpers import TERMINAL_STATES
from ..helpers.ssh_pool import SSHConnectionPool
from ..launchers.base import ComputeLauncher
from ..resources.session import SlurmSessionResource
from ..resources.slurm import SlurmResource


class SlurmPipesClient(PipesClient):
    """Pipes client for Slurm execution with real-time log streaming and cancellation support.

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
        pre_deployed_env_path: Optional[str] = None,
    ):
        """Args:
        slurm_resource: Slurm cluster configuration
        launcher: Launcher to generate execution plans
        session_resource: Optional session resource for operator fusion
        cleanup_on_failure: Whether to cleanup remote files on failure
        debug_mode: If True, never cleanup files (for debugging)
        auto_detect_platform: Auto-detect platform (ARM vs x86) for pixi pack
        pack_platform: Override platform ('linux-64', 'linux-aarch64', 'osx-arm64').

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
        self.pre_deployed_env_path = pre_deployed_env_path

    def run(  # type: ignore[override]
        self,
        context: AssetExecutionContext,
        *,
        payload_path: str,
        extra_env: Optional[Dict[str, str]] = None,
        extras: Optional[Dict[str, Any]] = None,
        use_session: bool = False,
        extra_slurm_opts: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Iterator:
        """Execute payload on Slurm cluster with real-time log streaming.

        Args:
            context: Dagster execution context
            payload_path: Local path to Python script
            launcher: Ignored (launcher is set at client construction time)
            extra_env: Additional environment variables
            extras: Extra data to pass via Pipes
            use_session: If True and session_resource provided, use shared allocation
            extra_slurm_opts: Override Slurm options (non-session mode)
            **kwargs: Additional arguments (ignored, for forward compatibility)

        Yields:
            Dagster events

        """
        run_id = context.run_id or uuid.uuid4().hex

        # Setup SSH connection pool
        ssh_pool = SSHConnectionPool(self.slurm.ssh)
        self._ssh_pool = ssh_pool  # type: ignore
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
                self._control_path = ssh_pool.control_path  # type: ignore

                if self.pre_deployed_env_path:
                    self.logger.info(
                        f"PROD mode: Using pre-deployed environment at: {self.pre_deployed_env_path}"
                    )

                    # Define paths based on the pre-deployed location
                    env_base_dir = self.pre_deployed_env_path
                    env_dir = f"{env_base_dir}/env"
                    activation_script = f"{env_base_dir}/activate.sh"
                    python_executable = f"{env_dir}/bin/python"

                    # We still need a run-specific directory for logs, payload, etc.
                    remote_base = self._get_remote_base(run_id, ssh_pool)
                    run_dir = f"{remote_base}/runs/{run_id}"
                    messages_path = f"{run_dir}/messages.jsonl"
                    self.logger.debug(f"Creating remote run directory: {run_dir}")
                    ssh_pool.run(f"mkdir -p {run_dir}")
                else:
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
                        pipes_context=pipes_env,  # type: ignore
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
                            extra_slurm_opts=extra_slurm_opts,
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
        """Cancel a Slurm job.

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
            ssh_pool.run(f"ls -la {run_dir}")
            self.logger.info("Environment extracted successfully")

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
            except:  # noqa: E722
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
        extra_slurm_opts: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Execute as standalone sbatch job with real-time log streaming.

        Args:
            execution_plan: Execution plan from launcher
            run_dir: Remote working directory
            ssh_pool: SSH connection pool
            extra_slurm_opts: Optional Slurm option overrides

        """
        import tempfile
        from pathlib import Path

        from ..config.runtime import RuntimeVariant

        # Determine node count
        if extra_slurm_opts and "nodes" in extra_slurm_opts:
            num_nodes = extra_slurm_opts["nodes"]
        elif self.slurm.queue.num_nodes:
            num_nodes = self.slurm.queue.num_nodes
        elif "nodes" in execution_plan.resources:
            num_nodes = execution_plan.resources["nodes"]
        else:
            num_nodes = 1

        self.logger.debug(f"Standalone job will request {num_nodes} node(s)")

        # Get the job script content
        payload_lines = execution_plan.payload

        # If multi-node Ray/Spark job, inject cluster detection
        if num_nodes > 1 and execution_plan.kind in {
            RuntimeVariant.RAY,
            RuntimeVariant.SPARK,
        }:
            self.logger.info(f"Multi-node {execution_plan.kind} job: {num_nodes} nodes")
            slurm_detection = """
    # Detect Slurm allocation for cluster mode
    if [[ -n "$SLURM_JOB_ID" ]] && [[ "$SLURM_JOB_NUM_NODES" -gt 1 ]]; then
        export SLURM_CLUSTER_MODE=1
        export SLURM_NODES=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
        export SLURM_JOB_ID="$SLURM_JOB_ID"
        export SLURM_JOB_NUM_NODES="$SLURM_JOB_NUM_NODES"
        echo "Detected multi-node Slurm allocation: $SLURM_JOB_NUM_NODES nodes"
    fi
    """
            # Insert after shebang
            job_script_content = (
                payload_lines[0]
                + "\n"
                + slurm_detection
                + "\n"
                + "\n".join(payload_lines[1:])
            )
        else:
            job_script_content = "\n".join(payload_lines)

        # Write script to local temp file then upload
        script_path = f"{run_dir}/job.sh"
        if not job_script_content.strip():
            raise ValueError("Execution plan generated empty script content")

        self.logger.debug(
            f"Writing job script ({len(job_script_content)} bytes) to {script_path}"
        )

        # Check if execution plan has auxiliary scripts (Ray head/worker scripts)
        auxiliary_scripts = getattr(execution_plan, "auxiliary_scripts", {})

        try:
            # Write main job script to local temp file
            with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".sh") as f:
                f.write(job_script_content)
                local_temp_path = f.name

            # Upload the main job script via SFTP
            ssh_pool.upload_file(local_temp_path, script_path)

            # Clean up local temp file
            Path(local_temp_path).unlink()

            # Upload auxiliary scripts (e.g., Ray head/worker scripts)
            if auxiliary_scripts:
                for script_name, script_content in auxiliary_scripts.items():
                    remote_script_path = f"{run_dir}/{script_name}"
                    self.logger.debug(f"Uploading auxiliary script: {script_name}")

                    with tempfile.NamedTemporaryFile(
                        mode="w", delete=False, suffix=".sh"
                    ) as f:
                        f.write(script_content)
                        local_aux_path = f.name

                    ssh_pool.upload_file(local_aux_path, remote_script_path)
                    Path(local_aux_path).unlink()

                    # Make executable
                    ssh_pool.run(f"chmod +x {remote_script_path}")
            else:
                self.logger.debug("No auxiliary scripts to upload")
        except Exception as e:
            self.logger.error(f"Failed to write job script: {e}")
            # Clean up temp files if they exist
            try:
                if "local_temp_path" in locals():
                    Path(local_temp_path).unlink(missing_ok=True)  # type: ignore
                if "local_aux_path" in locals():
                    Path(local_aux_path).unlink(missing_ok=True)  # type: ignore
            except:  # noqa: E722
                pass
            raise RuntimeError(f"Could not write job script to {script_path}") from e

        # Verify file was uploaded
        try:
            verify_output = ssh_pool.run(
                f"test -f {script_path} && wc -l {script_path}"
            )
            self.logger.debug(f"Job script verified: {verify_output.strip()}")
        except Exception as e:
            self.logger.error(f"Job script verification failed: {e}")
            raise RuntimeError(f"Job script was not created at {script_path}") from e

        # Make executable
        try:
            ssh_pool.run(f"chmod +x {script_path}")
        except Exception as e:
            self.logger.error(f"Failed to chmod job script: {e}")
            raise RuntimeError("Could not make job script executable") from e

        # Build sbatch command
        sbatch_cmd = self._build_sbatch_command(
            job_name=f"dagster_{run_dir.split('/')[-1]}",
            working_dir=run_dir,
            output_file=f"{run_dir}/slurm-%j.out",
            error_file=f"{run_dir}/slurm-%j.err",
            script_path=script_path,
            extra_opts=extra_slurm_opts,
        )

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
        self._current_job_id = job_id  # type: ignore
        self.logger.info(f"Submitted job {job_id}")

        # Wait for completion WITH live log streaming
        self._wait_for_job_with_streaming(job_id, ssh_pool, run_dir)

        return job_id

    def _build_sbatch_command(
        self,
        job_name: str,
        working_dir: str,
        output_file: str,
        error_file: str,
        script_path: str,
        extra_opts: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Build sbatch submission command.

        Uses SlurmQueueConfig as defaults, which can be overridden by extra_opts.

        Args:
            job_name: Job name
            working_dir: Working directory
            output_file: Stdout file path
            error_file: Stderr file path
            script_path: Path to job script
            extra_opts: Optional overrides for Slurm options
                - nodes: int
                - cpus_per_task: int
                - mem: str (e.g., "32G")
                - time_limit: str (e.g., "02:00:00")
                - gpus_per_node: int
                - partition: str

        Returns:
            Complete sbatch command string

        """
        sbatch_opts = [
            f"-J {job_name}",
            f"-D {working_dir}",
            f"-o {output_file}",
            f"-e {error_file}",
        ]

        # Start with queue config defaults
        partition = self.slurm.queue.partition
        time_limit = self.slurm.queue.time_limit
        cpus = self.slurm.queue.cpus
        mem = self.slurm.queue.mem
        num_nodes = self.slurm.queue.num_nodes
        gpus_per_node = None

        # Override with extra_opts if provided
        if extra_opts:
            partition = extra_opts.get("partition", partition)
            time_limit = extra_opts.get("time_limit", time_limit)
            cpus = extra_opts.get("cpus_per_task", cpus)
            mem = extra_opts.get("mem", mem)
            num_nodes = extra_opts.get("nodes", num_nodes)
            gpus_per_node = extra_opts.get("gpus_per_node")

        # Build command with final values
        if partition:
            sbatch_opts.append(f"-p {partition}")

        sbatch_opts.append(f"-t {time_limit}")
        sbatch_opts.append(f"-c {cpus}")
        sbatch_opts.append(f"--mem={mem}")

        # Add node count (important for multi-node jobs)
        if num_nodes:
            sbatch_opts.append(f"-N {num_nodes}")

        # Add GPUs if requested
        if gpus_per_node:
            sbatch_opts.append(f"--gpus-per-node={gpus_per_node}")

        sbatch_opts.append(script_path)

        return f"sbatch {' '.join(sbatch_opts)}"

    def _wait_for_job_with_streaming(
        self,
        job_id: int,
        ssh_pool: SSHConnectionPool,
        run_dir: str,
        poll_timeout: int = 3600,
    ):
        """Wait for job completion while streaming stdout/stderr and robustly
        polling the job state.
        """
        self.logger.info(f"Waiting for job {job_id} with live log streaming...")
        stdout_path = f"{run_dir}/slurm-{job_id}.out"
        stderr_path = f"{run_dir}/slurm-{job_id}.err"

        time.sleep(2)  # Wait for job to start and create log files

        stop_streaming = threading.Event()

        def stream_file(remote_path: str, output_stream, prefix: str):
            try:
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
                        line = proc.stdout.readline()  # type: ignore
                        if not line:
                            if proc.poll() is not None:
                                break
                            time.sleep(0.1)
                            continue
                        output_stream.write(f"{prefix}{line}")
                        output_stream.flush()
                finally:
                    proc.terminate()
                    try:
                        proc.wait(timeout=2)
                    except:  # noqa: E722
                        proc.kill()
            except Exception as e:
                self.logger.warning(f"Error streaming {remote_path}: {e}")

        stdout_thread = threading.Thread(
            target=stream_file,
            args=(stdout_path, sys.stdout, "[SLURM] "),
            daemon=True,
        )
        stdout_thread.start()

        stderr_thread = threading.Thread(
            target=stream_file,
            args=(stderr_path, sys.stderr, "[SLURM ERR] "),
            daemon=True,
        )
        stderr_thread.start()

        start_time = time.time()
        last_state = None
        state_check_count = 0

        # Poll job status
        try:
            while True:
                # Check for overall timeout
                if time.time() - start_time > poll_timeout:
                    raise RuntimeError(
                        f"Timed out after {poll_timeout}s waiting for job {job_id} to complete."
                    )

                # Check for user cancellation request
                if self._cancellation_requested:
                    self.logger.warning("Cancellation detected during job execution")
                    raise RuntimeError(f"Job {job_id} was cancelled by user request")

                # Get current job state
                state = self._get_job_state(job_id, ssh_pool)

                # Track state changes
                if state != last_state:
                    if state:
                        self.logger.debug(
                            f"Job {job_id} state changed: {last_state} -> {state}"
                        )
                    last_state = state
                    state_check_count = 0
                else:
                    state_check_count += 1

                # Handle empty state
                if not state:
                    state_check_count += 1

                    # If we've seen empty state multiple times, check sacct
                    if state_check_count >= 3:
                        try:
                            detailed_output = ssh_pool.run(
                                f"sacct -j {job_id} -o JobID,State,ExitCode -P 2>/dev/null || true"
                            )
                            self.logger.debug(
                                f"Detailed sacct output:\n{detailed_output}"
                            )

                            # Parse sacct output
                            for line in detailed_output.strip().split("\n"):
                                if str(job_id) in line and "CANCELLED" in line.upper():
                                    self.logger.warning(
                                        f"Job {job_id} was cancelled externally"
                                    )
                                    self._cancellation_requested = True
                                    # Give logs time to catch up
                                    time.sleep(2)
                                    raise RuntimeError(
                                        f"Job {job_id} was cancelled externally (detected via sacct). "
                                        f"The job may have been cancelled by scancel or administrator action."
                                    )
                        except RuntimeError:
                            raise  # Re-raise cancellation detection
                        except Exception as e:
                            self.logger.debug(f"Could not get detailed job info: {e}")

                    # Continue polling
                    if state_check_count < 12:
                        self.logger.debug(
                            f"Job {job_id} state unknown (attempt {state_check_count}/12)"
                        )
                        time.sleep(5)
                        continue
                    else:
                        # State unknown for too long
                        self.logger.warning(
                            f"Job {job_id} state unknown for {state_check_count * 5}s. "
                            f"Assuming completion."
                        )
                        break

                # Check for COMPLETING state (transitional state before terminal)
                if state == "COMPLETING":
                    self.logger.debug(
                        f"Job {job_id} is completing, waiting for terminal state..."
                    )
                    time.sleep(2)
                    continue

                # Handle terminal states
                if state in TERMINAL_STATES:
                    self.logger.info(f"Job {job_id} reached terminal state: {state}")

                    # Give streaming threads time to catch final output
                    time.sleep(3)

                    # Check if it's a cancellation state
                    if state in {"CANCELLED", "PREEMPTED"}:
                        self.logger.warning(f"Job {job_id} was {state}")
                        self._cancellation_requested = True
                        raise RuntimeError(
                            f"Job {job_id} was {state}. "
                            f"This may have been triggered by scancel, administrator action, or resource limits."
                        )

                    # Check for other failure states
                    if state != "COMPLETED":
                        raise RuntimeError(
                            f"Job {job_id} did not complete successfully. Final state: {state}. "
                            f"Check logs for details."
                        )

                    # Success
                    self.logger.info(f"Job {job_id} completed successfully")
                    break

                # Job still running
                time.sleep(5)

        finally:
            # Stop streaming and clean up
            stop_streaming.set()
            stdout_thread.join(timeout=5)
            stderr_thread.join(timeout=5)
            self._current_job_id = None

    def _build_tail_command(self, remote_path: str) -> list[str]:
        """Build SSH tail command for streaming logs.

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
                [  # type: ignore
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
