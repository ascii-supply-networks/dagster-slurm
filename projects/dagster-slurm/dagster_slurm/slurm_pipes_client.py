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

from .ssh_helpers import ssh_check, scp_put, ssh_job_state, TERMINAL_STATES
from .ssh_message_reader import SshExecTailMessageReader


def _default_local_payload() -> str:
    # same default you used before
    return str(
        (Path(__file__).resolve().parents[3]
         / "dagster_slurm_example" / "defs" / "shell" / "external_file.py"
        ).resolve()
    )


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
        local_payload: Optional[str] = None,
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
    ) -> Iterator:
        """
        Yields Dagster events (logs/materializations) from the remote Pipes process.
        On success, sets `self.last_job_id` and `self.last_remote_run_dir`.
        """
        run_id = context.run_id or uuid.uuid4().hex
        remote_run_dir = f"{self.remote_base}/pipes/{run_id}"
        remote_msgs    = f"{remote_run_dir}/messages.jsonl"
        remote_results = f"{self.remote_base}/{results_dir_name}"
        remote_logs    = f"{self.remote_base}/{logs_dir_name}"

        # 1) create dirs
        mkdir_cmd = "mkdir -p -- " + " ".join(
            shlex.quote(p) for p in (remote_run_dir, remote_results, remote_logs)
        )
        ssh_check(mkdir_cmd)

        # 2) upload payload and chmod
        payload = local_payload or _default_local_payload()
        scp_put(payload, f"{remote_run_dir}/external_file.py")
        ssh_check(f"chmod a+rx {shlex.quote(remote_run_dir)}/external_file.py")

        # 3) Pipes wiring: reader + injector
        reader = self._message_reader_factory(remote_msgs)

        with open_pipes_session(
            context=context,
            context_injector=self.context_injector,
            message_reader=reader,
        ) as session:
            # 3a) write pipes.env (bootstrap DAGSTER_PIPES_* + caller's extra_env)
            env = session.get_bootstrap_env_vars()
            if extra_env:
                env.update({k: str(v) for k, v in extra_env.items()})
            exports = "\n".join(f"export {k}={shlex.quote(v)}" for k, v in env.items())
            ssh_check(f"cat > {shlex.quote(remote_run_dir)}/pipes.env <<'EOF'\n{exports}\nEOF")

            # 3b) job.sbatch
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
            opts = ["-J", job_name, "-D", remote_run_dir,
                    "-o", f"{remote_run_dir}/slurm-%j.out",
                    "-e", f"{remote_run_dir}/slurm-%j.err"]
            opts += self._sbatch_opts(
                time_limit=time_limit, cpus=cpus, mem=mem,
                mem_per_cpu=mem_per_cpu, partition=partition,
            )
            if extra_sbatch_args:
                opts += list(extra_sbatch_args)

            submit_cmd = "sbatch " + " ".join(shlex.quote(x) for x in opts) + f" {shlex.quote(remote_run_dir)}/job.sbatch"
            out = ssh_check(submit_cmd)

            m = re.search(r"Submitted batch job (\d+)", out)
            if not m:
                raise RuntimeError(f"Could not parse job id from sbatch.\nstdout:\n{out}")
            job_id = int(m.group(1))
            self.last_job_id = job_id
            self.last_remote_run_dir = remote_run_dir
            get_dagster_logger().info(f"Submitted job {job_id}")

            # 4) stream Pipes messages while polling Slurm
            while True:
                for ev in session.get_results():
                    yield ev
                st = ssh_job_state(job_id)
                if st in TERMINAL_STATES:
                    break
                time.sleep(self.poll_interval_seconds)

            # Final drain (captures any buffered tail lines)
            for ev in session.get_results():
                yield ev
