# slurm_pipes_client.py
import os
import re
import shlex
import textwrap
import time
import uuid
from pathlib import Path
from typing import Callable, Dict, Iterable, Iterator, List, Optional

from dagster import AssetExecutionContext, PipesClient, PipesEnvContextInjector, get_dagster_logger, open_pipes_session

from .ssh_helpers import ssh_check, scp_put, ssh_job_state, TERMINAL_STATES, upload_lib
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


def _render_template_keep_others(src: str, mapping: Dict[str, str]) -> str:
    """
    Replaces ${KEY} only if KEY is in _ALLOWED_KEYS (and provided in mapping).
    Leave everything else (including ${SLURM_*}, ${ip}, etc.) untouched.
    """
    def repl(m: re.Match[str]) -> str:
        key = m.group(1)
        if key in _ALLOWED_KEYS and key in mapping:
            return str(mapping[key])
        return m.group(0)
    return re.sub(r"\$\{([A-Z0-9_]+)\}", repl, src)

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
        default_partition: str = "",
        default_time_limit: str = "00:10:00",
        default_cpus: str = "1",
        default_mem: str = "256M",
        default_mem_per_cpu: str = "",  # leave empty to prefer --mem

        # Pipes bits
        poll_interval_seconds: float = 1.0,
        context_injector: Optional[PipesEnvContextInjector] = None,
        message_reader_factory: Optional[
            Callable[[str], SshExecTailMessageReader]
        ] = None,
    ):
        # Resolve defaults from env (same names you used)
        self.remote_base = (remote_base or os.environ.get("SLURM_REMOTE_BASE", "/home/submitter")).rstrip("/")
        self.activate_sh  = activate_sh or os.environ.get("SLURM_ACTIVATE_SH", "/home/submitter/activate.sh")
        self.remote_python = remote_python or os.environ.get("SLURM_PYTHON", "python")

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

    def run(  # type: ignore[override]
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
    ) -> Iterator:
        """
        Yields Dagster events (logs/materializations) from the remote Pipes process.
        On success, sets `self.last_job_id` and `self.last_remote_run_dir`.
        If slurm_template_path is provided, it renders it and submit that as the batch script.
        Otherwise, fall back to the built-in tiny batch script.
        """
        run_id = context.run_id or uuid.uuid4().hex
        remote_run_dir = f"{REMOTE_BASE}/pipes/{run_id}"
        remote_msgs    = f"{remote_run_dir}/messages.jsonl"
        remote_results = f"{REMOTE_BASE}/{results_dir_name}"
        remote_logs    = f"{REMOTE_BASE}/{logs_dir_name}"
        mkdir_cmd = "mkdir -p -- " + " ".join(
            shlex.quote(p) for p in (remote_run_dir, remote_results, remote_logs)
        )
        # 1) create dirs
        ssh_check("mkdir -p -- " + " ".join(map(shlex.quote, (remote_run_dir, remote_results, remote_logs))))
        # 2) upload payload and chmod
        #TODO: make this push dagster-slurm pixi pack verify pack exist if not packing 
        #TODO: make this push share pixi pack verify pack exist if not packing 
        scp_put(str(local_payload), f"{remote_run_dir}/external_file.py") #TODO: make this keep the original name
        ssh_check(f"chmod a+rx {shlex.quote(remote_run_dir)}/external_file.py")
        wheel_remote  = upload_lib(source=package_src_dir, dest=remote_run_dir)
        # 3) Pipes wiring: reader + injector
        injector = self.context_injector
        reader   = self._message_reader_factory(remote_msgs)

        with open_pipes_session(context=context, context_injector=injector, message_reader=reader) as session:
            # 3a) write pipes.env (bootstrap DAGSTER_PIPES_* + caller's extra_env)
            env = session.get_bootstrap_env_vars()
            if extra_env:
                env.update({k: str(v) for k, v in extra_env.items()})
            exports = "\n".join(f"export {k}={shlex.quote(v)}" for k, v in env.items())
            ssh_check(f"cat > {shlex.quote(remote_run_dir)}/pipes.env <<'EOF'\n{exports}\nEOF")

            # 3b) job.sbatch
            if slurm_template_path:
                template_text = Path(slurm_template_path).read_text(encoding="utf-8")
                install_wheel_shell = textwrap.dedent(f"""
                    # Install freshly built package into the python we'll use
                    if command -v uv >/dev/null 2>&1; then
                        "{self.remote_python}" -m uv pip install -U {wheel_remote}
                    else
                        "{self.remote_python}" -m pip install -U {wheel_remote} || (
                        "{self.remote_python}" -m ensurepip --upgrade &&
                        "{self.remote_python}" -m pip install -U {wheel_remote}
                        )
                    fi
                    """).strip()         

                default_load_env = "\n".join([
                    f"source {shlex.quote(self.activate_sh)}",
                    f"source {shlex.quote(remote_run_dir)}/pipes.env",
                    f"export JOB_OUTPUT_DIR={shlex.quote(remote_results)}",
                    'mkdir -p -- "$JOB_OUTPUT_DIR"',
                    install_wheel_shell,
                ])
                partition_line = f"#SBATCH -p {partition}" if partition else ""
                params: Dict[str, str] = {
                    "PARTITION_OPTION": partition_line,
                    "JOB_NAME": job_name,
                    "GIVEN_NODE": template_params.get("GIVEN_NODE", "") if template_params else "",
                    "NUM_NODES": template_params.get("NUM_NODES", "1") if template_params else "1",
                    "NUM_GPUS_PER_NODE": template_params.get("NUM_GPUS_PER_NODE", "0") if template_params else "0",
                    "LOAD_ENV": template_params.get("LOAD_ENV", default_load_env) if template_params else default_load_env,
                    # This is where your payload runs with Pipes env loaded:
                    "COMMAND_PLACEHOLDER": template_params.get(
                        "COMMAND_PLACEHOLDER",
                        f'exec {shlex.quote(self.remote_python)} {shlex.quote(remote_run_dir)}/external_file.py',
                    ) if template_params else f'exec {shlex.quote(self.remote_python)} {shlex.quote(remote_run_dir)}/external_file.py',
                }
                if template_params:
                    params.update(template_params)
                rendered = _render_template_keep_others(template_text, params)
                #ng = str(params.get("NUM_GPUS_PER_NODE", "0")).strip().lower()
                #if ng in ("0", "", "none", "false"):
                #    rendered = "\n".join(
                #        line for line in rendered.splitlines()
                #        if "--gpus-per-task" not in line and "--gres=gpu" not in line
                #    ) + "\n"
                ssh_check(
                    "cat > {path}/job.sbatch <<'SB'\n{body}\nSB\nchmod +x {path}/job.sbatch".format(
                        path=shlex.quote(remote_run_dir), body=rendered
                    )
                )
                cmd = f"sbatch -D {shlex.quote(remote_run_dir)} {shlex.quote(remote_run_dir)}/job.sbatch"
                if extra_sbatch_args:
                    cmd = "sbatch " + " ".join(shlex.quote(x) for x in extra_sbatch_args) + f" -D {shlex.quote(remote_run_dir)} {shlex.quote(remote_run_dir)}/job.sbatch"

                out = ssh_check(cmd)
            else:
                
                bootstrap = textwrap.dedent(
                    f"""\
                    set -euo pipefail
                    source {shlex.quote(self.activate_sh)}
                    source {shlex.quote(remote_run_dir)}/pipes.env
                    export JOB_OUTPUT_DIR={shlex.quote(remote_results)}
                    mkdir -p -- "$JOB_OUTPUT_DIR"
                    exec {shlex.quote(self.remote_python)} {shlex.quote(remote_run_dir)}/external_file.py
                    """
                )
                ssh_check(
                    "cat > {path}/job.sbatch <<'SB'\n#!/bin/bash\n{body}\nSB\nchmod +x {path}/job.sbatch".format(
                        path=shlex.quote(remote_run_dir), body=bootstrap
                    )
                )

                # 3c) sbatch submit
                args = [
                    "-J", job_name,
                    "-D", remote_run_dir,
                    "-o", f"{remote_run_dir}/slurm-%j.out",
                    "-e", f"{remote_run_dir}/slurm-%j.err",
                ]
                if time_limit: args += ["-t", time_limit]
                if cpus:       args += ["-c", cpus]
                if mem:        args += [f"--mem={mem}"]
                if mem_per_cpu and not mem: args += [f"--mem-per-cpu={mem_per_cpu}"]
                if partition:  args += ["-p", partition]
                if extra_sbatch_args: args += list(extra_sbatch_args)

                out = ssh_check("sbatch " + " ".join(shlex.quote(x) for x in args) + f" {shlex.quote(remote_run_dir)}/job.sbatch")
            
            m = re.search(r"Submitted batch job (\d+)", out)
            if not m:
                raise RuntimeError(f"Could not parse job id from sbatch.\nstdout:\n{out}")
            job_id = int(m.group(1))
            self.last_job_id = job_id
            self.last_remote_run_dir = remote_run_dir
            context.log.info(f"Submitted job {job_id}")

            # 4) stream Pipes messages while polling Slurm
            while True:
                st = ssh_job_state(job_id)
                if st in TERMINAL_STATES:
                    break
                time.sleep(self.poll_interval_seconds)

            for ev in session.get_results():
                yield ev
