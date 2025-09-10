# slurm_pipes_client.py
import os
import re
import shlex
import textwrap
import time
import uuid
from pathlib import Path
from typing import Callable, Dict, Iterable, Iterator, List, Optional
from string import Template
from .local_runner import LocalRunner
from .docker_runner import DockerRunner

from dagster import AssetExecutionContext, PipesClient, PipesEnvContextInjector, get_dagster_logger, open_pipes_session

from .ssh_helpers import ssh_check, scp_put, ssh_job_state, TERMINAL_STATES, upload_lib, install_lib_locally
from .ssh_message_reader import SshExecTailMessageReader

_ALLOWED_KEYS = {
    "PARTITION_OPTION",
    "JOB_NAME",
    "GIVEN_NODE",
    "NUM_NODES",
    "NUM_GPUS_PER_NODE",
    "LOAD_ENV",
    "COMMAND_PLACEHOLDER",
}
REMOTE_BASE = os.environ.get("SLURM_REMOTE_BASE", "/home/submitter").rstrip("/")
ACTIVATE_SH = os.environ.get("SLURM_ACTIVATE_SH", "/home/submitter/activate.sh")
REMOTE_PY   = os.environ.get("SLURM_PYTHON", "python")

def get_runner(logger):
    # Use an env var to decide which runner to use. Default to local.
    env = os.environ.get("DAGSTER_ENV", "dev").lower()
    if env == "staging":
        logger.info("Using SlurmRunner for staging environment.")
        return DockerRunner(logger)
    logger.info("Using LocalRunner for dev environment.")
    return LocalRunner(logger)


class _PipesBaseSlurmClient(PipesClient):
    """
    Minimal, generic Slurm+SSH Pipes client.

    Responsibilities:
      - create per-run remote dirs
      - upload payload
      - write `pipes.env`
      - write/submit `sbatch` script
      - stream Pipes messages while polling Slurm
    """

    def __init__(
        self,
        *,
        # Remote & env bootstrap
        remote_base: Optional[str] = None,
        activate_sh: Optional[str] = None,
        remote_python: Optional[str] = None,

        # sbatch defaults (can be overridden per-run)
        default_partition: Optional[str] = "",
        default_time_limit: Optional[str] = "00:10:00",
        default_cpus: Optional[str] = "1",
        default_mem: Optional[str] = "256M",
        default_mem_per_cpu: Optional[str] = "",  # leave empty to prefer --mem

        # Pipes bits
        poll_interval_seconds: float = 1.0,
        context_injector: Optional[PipesEnvContextInjector] = None,
        message_reader_factory: Optional[
            Callable[[str], SshExecTailMessageReader]
        ] = None,
    ):
        super().__init__()
        # Resolve defaults from env
        self.remote_base = (remote_base or os.environ.get("SLURM_REMOTE_BASE", "/home/submitter")).rstrip("/")
        self.activate_sh  = activate_sh or os.environ.get("SLURM_ACTIVATE_SH", "/home/submitter/activate.sh")
        self.remote_python = remote_python or os.environ.get("SLURM_PYTHON", "python3")

        self.default_partition   = default_partition or os.environ.get("SLURM_PARTITION", "")
        self.default_time_limit  = os.environ.get("SLURM_TIME", default_time_limit)
        self.default_cpus        = os.environ.get("SLURM_CPUS", default_cpus)
        self.default_mem         = os.environ.get("SLURM_MEM", default_mem)
        self.default_mem_per_cpu = os.environ.get("SLURM_MEM_PER_CPU", default_mem_per_cpu)

        self.poll_interval_seconds = poll_interval_seconds
        self.context_injector = context_injector or PipesEnvContextInjector()
        self._message_reader_factory = (
            message_reader_factory
            or (lambda remote_path: SshExecTailMessageReader(remote_path, include_stdio_in_messages=True))
        )
        self.logger = get_dagster_logger()
        self.runner = get_runner(self.logger)
        
        # surfaces for the caller to read after run()
        self.last_job_id: Optional[int] = None
        self.last_remote_run_dir: Optional[str] = None

    # utility to compose sbatch options
    def _sbatch_opts(
        self,
        *,
        time_limit: Optional[str],
        cpus: Optional[str],
        mem: Optional[str],
        mem_per_cpu: Optional[str],
        partition: Optional[str],
    ) -> List[str]:
        tl = time_limit or self.default_time_limit
        c  = cpus or self.default_cpus
        m  = mem if (mem is not None and mem != "") else self.default_mem
        mpc = mem_per_cpu if (mem_per_cpu is not None) else self.default_mem_per_cpu
        part = partition if (partition is not None) else self.default_partition

        opts = ["-t", tl, "-c", c]
        # Slurm forbids using both
        if mpc:
            opts += [f"--mem-per-cpu={mpc}"]
        else:
            opts += [f"--mem={m}"]
        if part:
            opts += ["-p", part]
        return opts

    def _render_template_keep_others(self, template_text: str, params: Dict[str, str]) -> str:
        """
        Substitute only our known placeholders and leave *all other* $VARS
        (e.g. real bash/SLURM vars) intact for runtime expansion.

        Works with ${NAME} and $NAME. Unknown placeholders are left as-is.
        """
        # keep only placeholders we intend to touch
        mapping = {k: str(v) for k, v in (params or {}).items() if k in _ALLOWED_KEYS}
        return Template(template_text).safe_substitute(mapping)

    def _patch_rendered_for_pipes_and_ray(self, rendered: str, *, remote_run_dir: str) -> str:
        """
        Last-mile fixes *without* changing the original template file:
        - Remove '--gpus-per-task=0' if NUM_GPUS_PER_NODE is 0 (many Slurm setups
            reject a zero GPU request).
        - Pre-create the Pipes message file path so the tailer always has a file.
        - Add a few debug echos so it’s easy to see what was rendered.
        """
        lines = rendered.splitlines()

        out: list[str] = []
        inserted_setup = False

        for i, line in enumerate(lines):
            # 1) Drop an illegal zero-GPU request (keeps GPUs when >0)
            if re.search(r"^#SBATCH\s+--gpus-per-task\s*=\s*0\s*$", line):
                continue

            out.append(line)

            # 2) Right after shebang or just after ${LOAD_ENV}, add a tiny setup block
            #    to pre-create the messages file and print a bit of context.
            if (not inserted_setup) and (
                line.startswith("#!/bin/bash") or
                "${LOAD_ENV}" in line  # if template inlines LOAD_ENV here
            ):
                out.append(
                    f"""\
# --- auto-patch: Pipes/Ray setup (do not edit template) ---
: > "{remote_run_dir}/messages.jsonl" || true
echo "[$(date -Is)] DAGSTER_PIPES_MESSAGES=$DAGSTER_PIPES_MESSAGES"
echo "[$(date -Is)] SLURM_JOB_ID=${{SLURM_JOB_ID:-unknown}}  RUN_DIR={remote_run_dir}"
# ----------------------------------------------------------"""
                )
                inserted_setup = True

        return "\n".join(out) + ("\n" if not rendered.endswith("\n") else "")

    def run(
        self,
        context: AssetExecutionContext,
        *,
        local_payload: str,
        job_name: str = "pipes_ext",
        results_dir_name: str = "results",
        logs_dir_name: str = "logs",
        time_limit: Optional[str] = None,
        cpus: Optional[str] = None,
        mem: Optional[str] = None,
        mem_per_cpu: Optional[str] = None,
        partition: Optional[str] = None,
        extra_sbatch_args: Optional[Iterable[str]] = None,
        extra_env: Optional[Dict[str, str]] = None,
        slurm_template_path: Optional[str] = None,
        template_params: Optional[Dict[str, str]] = None,
        package_src_dir: str = '../projects/dagster-slurm',
        package_name: str = "dagster-slurm",
    ) -> Iterator:
        """
        Yields Dagster events (logs/materializations) from the remote Pipes process.
        On success, sets `self.last_job_id` and `self.last_remote_run_dir`.
        If slurm_template_path is provided, it renders it and submit that as the batch script.
        Otherwise, fall back to the built-in tiny batch script.
        """
        run_id = context.run_id or uuid.uuid4().hex
        is_staging = isinstance(self.runner, DockerRunner)
        wheel_remote_path = None
        exec_base_dir = self.remote_base if is_staging else "/tmp/dagster_local_runs"
        temp_dir_base = "/tmp" if is_staging else exec_base_dir

        exec_run_dir = f"{exec_base_dir}/pipes/{run_id}"
        temp_run_dir = f"{temp_dir_base}/pipes/{run_id}"\

        if is_staging:
            wheel_remote_path = upload_lib(
                source=package_src_dir,
                dest=exec_run_dir,
                examples_dir="./examples", # Adjust path to your examples dir
                package_name=package_name,
            )
        else:
            install_lib_locally(
                source=package_src_dir,
                examples_dir="./examples", # Adjust path to your examples dir
                package_name=package_name,
                log=self.logger,
            )
            
            
        Path(temp_run_dir).mkdir(parents=True, exist_ok=True)
        remote_msgs = f"{exec_run_dir}/messages.jsonl"
        
        self.runner.run_command(f"mkdir -p {shlex.quote(exec_run_dir)}")
        payload_filename = Path(local_payload).name
        remote_payload_path = f"{exec_run_dir}/{payload_filename}"
        if local_payload != remote_payload_path:
            self.runner.put_file(local_payload, remote_payload_path)

        py_executable = self.remote_python 
        bootstrap_content = f"#!/bin/bash\nsource {shlex.quote(self.activate_sh)}\n{py_executable} {shlex.quote(remote_payload_path)}\n"
        
        local_job_script = Path(temp_run_dir) / "job.sh"
        local_job_script.write_text(bootstrap_content)
        
        remote_job_script = f"{exec_run_dir}/job.sh"
        if str(local_job_script) != remote_job_script:
            self.runner.put_file(str(local_job_script), remote_job_script)
        self.runner.run_command(f"chmod +x {shlex.quote(remote_job_script)}")

        bootstrap_steps = [
            "#!/bin/bash",
            "set -euo pipefail",
            f"source {shlex.quote(self.activate_sh)}",
        ]

        if is_staging and wheel_remote_path:
            # If in staging, the job must install the uploaded wheel
            bootstrap_steps.append(
                f"{shlex.quote(self.remote_python)} -m pip install {shlex.quote(wheel_remote_path)}"
            )
        bootstrap_steps.append(
            f"exec {shlex.quote(self.remote_python)} {shlex.quote(remote_payload_path)}"
        )
        
        job_script_content = "\n".join(bootstrap_steps)

        message_reader_path = remote_msgs if is_staging else f"{exec_run_dir}/messages.jsonl"
        reader = self._message_reader_factory(message_reader_path)

        with open_pipes_session(context=context, context_injector=self.context_injector, message_reader=reader) as session:
            # 3. Submit the job using the runner
            job_id = self.runner.submit_job(job_script_content)
            context.log.info(f"Submitted job {job_id} using {type(self.runner).__name__}")
            
            # 4. Wait for completion using the runner
            self.runner.wait_for_job(job_id)

            self.last_job_id = self.runner.last_job_id
            self.last_remote_run_dir = exec_run_dir

            for ev in session.get_results():
                yield ev