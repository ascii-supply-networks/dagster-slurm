import os
import re
import shlex
import textwrap
import time
import uuid
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    asset,
    open_pipes_session,
    Output,
    PipesEnvContextInjector,
)
from .ssh_helpers import ssh_check, scp_put, ssh_job_state, TERMINAL_STATES
from .ssh_message_reader import SshExecTailMessageReader

# ---------- Config via env ----------
REMOTE_BASE         = os.environ.get("SLURM_REMOTE_BASE", "/home/submitter").rstrip("/")
ACTIVATE_SH         = os.environ.get("SLURM_ACTIVATE_SH", "/home/submitter/activate.sh")
REMOTE_PY           = os.environ.get("SLURM_PYTHON", "python")
PARTITION           = os.environ.get("SLURM_PARTITION", "")
TIME_LIMIT          = os.environ.get("SLURM_TIME", "00:10:00")
CPUS                = os.environ.get("SLURM_CPUS", "1")
MEM                 = os.environ.get("SLURM_MEM", "256M")
MEM_PER_CPU   = os.environ.get("SLURM_MEM_PER_CPU", "256M")

# Local external script executed remotely under Pipes
LOCAL_PAYLOAD = os.environ.get(
    "LOCAL_EXTERNAL_FILE",
    str(
        (Path(__file__).resolve().parents[3]
         / "dagster_slurm_example" / "defs" / "shell" / "external_file.py"
        ).resolve()
    ),
)


@asset(name="slurm_submit_pipes_ssh")
def slurm_submit_pipes(context: AssetExecutionContext):
    # --- Per-run remote paths ---
    run_id         = context.run_id or uuid.uuid4().hex
    remote_run_dir = f"{REMOTE_BASE}/pipes/{run_id}"
    remote_msgs    = f"{remote_run_dir}/messages.jsonl"
    remote_results = f"{REMOTE_BASE}/results"
    remote_logs    = f"{REMOTE_BASE}/logs"

    # --- Create directories once (robust quoting; single mkdir invocation) ---
    mkdir_cmd = "mkdir -p -- " + " ".join(
        shlex.quote(p) for p in (remote_run_dir, remote_results, remote_logs)
    )
    ssh_check(mkdir_cmd)

    # --- Upload payload & set perms ---
    scp_put(LOCAL_PAYLOAD, f"{remote_run_dir}/external_file.py")
    ssh_check(f"chmod a+rx {shlex.quote(remote_run_dir)}/external_file.py")

    # --- Pipes: env-based context + SSH-tailed file messages from remote ---
    injector = PipesEnvContextInjector()
    reader   = SshExecTailMessageReader(
        remote_messages_path=remote_msgs,
        include_stdio_in_messages=True,
    )

    with open_pipes_session(
        context=context,
        context_injector=injector,
        message_reader=reader,
    ) as session:
        # 1) Write DAGSTER_PIPES_* bootstrap env to a file on the remote host
        env = session.get_bootstrap_env_vars()
        exports = "\n".join(f"export {k}={shlex.quote(v)}" for k, v in env.items())
        ssh_check(
            f"cat > {shlex.quote(remote_run_dir)}/pipes.env <<'EOF'\n{exports}\nEOF"
        )

        # 2) Build remote job script: activate env, source Pipes env, exec payload
        bootstrap = textwrap.dedent(
            f"""\
            set -euo pipefail
            source {shlex.quote(ACTIVATE_SH)}
            source {shlex.quote(remote_run_dir)}/pipes.env
            export JOB_OUTPUT_DIR={shlex.quote(remote_results)}
            mkdir -p -- "$JOB_OUTPUT_DIR"
            exec {shlex.quote(REMOTE_PY)} {shlex.quote(remote_run_dir)}/external_file.py
            """
        )
        ssh_check(
            "cat > {path}/job.sbatch <<'SB'\n#!/bin/bash\n{body}\nSB\nchmod +x {path}/job.sbatch".format(
                path=shlex.quote(remote_run_dir),
                body=bootstrap,
            )
        )

        # 3) Submit via sbatch
        opts = [
            "-J", "pipes_ext",
            "-D", remote_run_dir,
            "-o", f"{remote_run_dir}/slurm-%j.out",
            "-e", f"{remote_run_dir}/slurm-%j.err",
            "-t", TIME_LIMIT,
            "-c", CPUS,
            f"--mem={MEM}",
            #f"--mem-per-cpu={MEM_PER_CPU}"
        ]
        if PARTITION:
            opts += ["-p", PARTITION]

        submit_cmd = "sbatch " + " ".join(shlex.quote(x) for x in opts) + f" {shlex.quote(remote_run_dir)}/job.sbatch"
        out = ssh_check(submit_cmd)

        m = re.search(r"Submitted batch job (\d+)", out)
        if not m:
            raise RuntimeError(f"Could not parse job id from sbatch.\nstdout:\n{out}")
        job_id = int(m.group(1))
        context.log.info(f"Submitted job {job_id}")

        # 4) Stream messages while polling job state
        while True:
            #for ev in session.get_results():
            #    yield ev
            st = ssh_job_state(job_id)
            if st in TERMINAL_STATES:
                break
            time.sleep(1.0)

    # Final drain after the session context exits (captures any buffered tail)
    #for ev in session.get_results():
    #    yield ev

    yield Output(
        {"job_id": job_id, "remote_run_dir": remote_run_dir},
        metadata={"job_id": job_id, "remote_run_dir": remote_run_dir},
    )
