import os
import re
import shlex
import textwrap
import time
import uuid
from pathlib import Path
import shutil
import dagster as dg
from dagster import (
    AssetExecutionContext,
    asset,
    open_pipes_session,
    Output,
    PipesEnvContextInjector,
)
from dagster_slurm.ssh_helpers import ssh_check, scp_put, ssh_job_state, TERMINAL_STATES
from dagster_slurm.ssh_message_reader import SshExecTailMessageReader
from dagster_slurm.slurm_factory import make_slurm_pipes_asset
from dagster_slurm_example.defs.shared import *


def get_python_executable() -> str:
    """Get the Python executable path from PATH.

    Returns:
        str: Path to the Python executable

    Raises:
        RuntimeError: If Python executable is not found in PATH
    """
    python_path = shutil.which("python")
    if python_path is None:
        raise RuntimeError("Python executable not found in PATH")
    return python_path


@dg.multi_asset(
    specs=[dg.AssetSpec(key=[example_defs_prefix, "orders"]), dg.AssetSpec("users")]
)
def subprocess_asset(
    context: dg.AssetExecutionContext,
    pipes_subprocess_client: dg.PipesSubprocessClient,
):
    python_path = get_python_executable()
    cmd = [python_path, dg.file_relative_path(__file__, "shell_external.py")]

    return pipes_subprocess_client.run(
        command=cmd,
        context=context,
        extras={"foo": "bar"},
        env={
            "MY_ENV_VAR_IN_SUBPROCESS": "my_value",
        },
    ).get_results()


@dg.asset_check(
    asset=dg.AssetKey([example_defs_prefix, "orders"]),
    blocking=True,
)
def no_empty_order_check(
    context: dg.AssetCheckExecutionContext,
    pipes_subprocess_client: dg.PipesSubprocessClient,
) -> dg.AssetCheckResult:
    python_path = get_python_executable()
    cmd = [
        python_path,
        dg.file_relative_path(__file__, "shell_integration_test.py"),
    ]

    results = pipes_subprocess_client.run(
        command=cmd, context=context.op_execution_context
    ).get_asset_check_result()
    return results


@asset(name="slurm_submit_pipes_ssh")
def slurm_submit_pipes(context: AssetExecutionContext):
    run_id         = context.run_id or uuid.uuid4().hex
    remote_run_dir = f"{REMOTE_BASE}/pipes/{run_id}"
    remote_msgs    = f"{remote_run_dir}/messages.jsonl"
    remote_results = f"{REMOTE_BASE}/results"
    remote_logs    = f"{REMOTE_BASE}/logs"

    mkdir_cmd = "mkdir -p -- " + " ".join(
        shlex.quote(p) for p in (remote_run_dir, remote_results, remote_logs)
    )
    ssh_check(mkdir_cmd)

    scp_put(LOCAL_PAYLOAD, f"{remote_run_dir}/external_file.py")
    ssh_check(f"chmod a+rx {shlex.quote(remote_run_dir)}/external_file.py")

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
            #f"--mem-per-cpu={MEM_PER_CPU}" #--mem and --mem-per-cpu cannot be together
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

            st = ssh_job_state(job_id)
            if st in TERMINAL_STATES:
                break
            time.sleep(1.0)

    yield Output(
        {"job_id": job_id, "remote_run_dir": remote_run_dir},
        metadata={"job_id": job_id, "remote_run_dir": remote_run_dir},
    )


slurm_submit_pipes_ssh = make_slurm_pipes_asset(
    name="slurm_submit_pipes_ssh2",
    local_payload="external_file.py",
    time_limit="00:10:00",
    cpus="1",
    mem="256M",
    # partition="normal",                      # optional
    # extra_env={"FOO": "bar"},                # optional
)

slurm_submit_pipes_ssh = make_slurm_pipes_asset(
    job_name="ray_pipes_job",
    name="slurm_submit_pipes_ray",
    local_payload="../ray/ray_slurm.py",
    time_limit="00:10:00",
    cpus="1",
    mem="256M",
    # partition="normal",                      # optional
    # extra_env={"FOO": "bar"},                # optional
)
