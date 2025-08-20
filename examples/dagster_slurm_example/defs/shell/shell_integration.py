
import dagster as dg
from dagster import (
    PipesSession, 
    PipesContextInjector, 
    PipesMessageReader, 
    asset, 
    AssetExecutionContext, 
    MaterializeResult, 
    AssetExecutionContext, 
    open_pipes_session)
import os
import re
import shlex
import subprocess
from dataclasses import dataclass
from typing import Optional

from dagster_slurm_example.defs.shared import example_defs_prefix

CONTAINER_DATA_DIR = "/data"                 # shared inside the Slurm containers
PYTHON_IN_NODE = "/usr/bin/python3"          # python in your Slurm nodes
DEFAULT_LOCAL_SCRIPT = os.path.join(
    os.path.dirname(__file__), "test.py"
)

def _run(cmd, input_bytes=None, check=True):
    p = subprocess.run(
        cmd, input=input_bytes, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if check and p.returncode != 0:
        raise RuntimeError(
            f"cmd failed: {' '.join(cmd)}\nstdout:\n{p.stdout.decode()}\nstderr:\n{p.stderr.decode()}"
        )
    return p.stdout.decode(), p.stderr.decode(), p.returncode

def _dexec(container, inner_cmd, stdin_bytes=None):
    return _run(["docker", "exec", "-i", container, "bash", "-lc", inner_cmd], stdin_bytes)

def _find_controller() -> str:
    out, _, _ = _run(["docker", "ps", "--format", "{{.Names}}"])
    for name in out.strip().splitlines():
        if "slurmctld" in name:
            return name
    raise RuntimeError("Could not find controller container (name containing 'slurmctld').")

def _put_file(container: str, local_path: str, remote_path: str):
    local_path = os.path.abspath(local_path)
    if not os.path.isfile(local_path):
        raise FileNotFoundError(local_path)
    remote_dir = os.path.dirname(remote_path) or "/"
    _dexec(container, f"mkdir -p {shlex.quote(remote_dir)}")
    _run(["docker", "cp", local_path, f"{container}:{remote_path}"])
    # normalize line endings & make readable
    _dexec(container, f"sed -i 's/\\r$//' {shlex.quote(remote_path)}; chmod a+r {shlex.quote(remote_path)}")

def _sanitize_partition(p: Optional[str]) -> Optional[str]:
    if not p:
        return None
    p = p.split(",")[0].rstrip("*").strip()
    return p or None

def _detect_partition(container: str) -> Optional[str]:
    out, _, _ = _dexec(container, "sinfo -h -o '%P' | head -n1")
    return _sanitize_partition(out.strip())
class SharedDirContextInjector(PipesContextInjector):
    def __init__(self, base="/data/pipes"): self.base = base
    def __enter__(self, context):  # create path + write context.json
        import json, os, uuid
        self.run_dir = f"{self.base}/{context.run_id or uuid.uuid4().hex}"
        os.makedirs(self.run_dir, exist_ok=True)
        ctx_path = f"{self.run_dir}/context.json"
        with open(ctx_path, "w") as f:
            json.dump(self.build_context_payload(context), f)
        # expose to external via env var
        return {"DAGSTER_PIPES_CONTEXT_PATH": ctx_path}
    def __exit__(self, *_): pass

class SharedDirMessageReader(PipesMessageReader):
    def __enter__(self, context):
        import os, uuid
        self.msg_path = f"/data/pipes/{context.run_id}/messages.jsonl"
        os.makedirs(os.path.dirname(self.msg_path), exist_ok=True)
        return {"DAGSTER_PIPES_MESSAGES_PATH": self.msg_path}
    def get_messages(self):
        # tail messages.jsonl and yield parsed events
        yield from self._read_from_path(self.msg_path)

@dataclass
class SubmitResult:
    job_id: int
    node: Optional[str]

def _submit_python(container: str, script_in_container: str, job_name="hello", minutes=2, cpus=1, mem_mb=256,
                   args=None, partition: Optional[str] = None) -> SubmitResult:
    partition = _sanitize_partition(partition)
    argstr = " ".join(shlex.quote(a) for a in (args or []))
    # ensure shared dirs and permissive perms (dev-friendly)
    _dexec(container, "mkdir -p /data/logs /data/results && chmod 1777 /data/logs /data/results")

    opts = [
        "-J", job_name,
        "-D", CONTAINER_DATA_DIR,
        "-o", f"{CONTAINER_DATA_DIR}/logs/{job_name}-%j.out",
        "-e", f"{CONTAINER_DATA_DIR}/logs/{job_name}-%j.err",
        "-t", f"00:{minutes:02d}:00",
        "-c", str(cpus),
        f"--mem={mem_mb}",
    ]
    if partition:
        opts += ["-p", partition]

    cmd = "sbatch " + " ".join(shlex.quote(x) for x in opts) + " --wrap " + shlex.quote(f"{PYTHON_IN_NODE} {script_in_container} {argstr}")
    out, err, _ = _dexec(container, cmd)
    m = re.search(r"Submitted batch job (\d+)", out)
    if not m:
        raise RuntimeError(f"Could not parse job id.\nstdout:\n{out}\nstderr:\n{err}")
    job_id = int(m.group(1))

    # figure out node (via sacct if available; otherwise scontrol)
    node = None
    o, _, _ = _dexec(container, f"sacct -X -n -j {job_id} -o NodeList")
    if o.strip():
        node = o.strip().split(",")[0]
    else:
        o, _, _ = _dexec(container, f"scontrol show job {job_id}")
        mm = re.search(r"BatchHost=(\S+)", o) or re.search(r"Nodes?=(\S+)", o)
        node = mm.group(1) if mm else None

    return SubmitResult(job_id=job_id, node=node)


def _job_state(container: str, job_id: int) -> str:
    o, _, _ = _dexec(container, f"squeue -h -j {job_id} -o '%T'"); st = o.strip()
    if st:
        return st
    o, _, _ = _dexec(container, f"sacct -X -n -j {job_id} -o State")
    return o.strip().splitlines()[0].strip() if o.strip() else "UNKNOWN"


def _wait_done(container: str, job_id: int, poll=2.0) -> str:
    import time
    while True:
        st = _job_state(container, job_id)
        if st in ("PENDING", "RUNNING", "COMPLETING", "CONFIGURING"):
            time.sleep(poll); continue
        return st


def _tail_logs(container: str, job_name: str, job_id: int, n=50):
    out_path = f"{CONTAINER_DATA_DIR}/logs/{job_name}-{job_id}.out"
    err_path = f"{CONTAINER_DATA_DIR}/logs/{job_name}-{job_id}.err"
    out_txt, _, _ = _dexec(container, f"test -f {shlex.quote(out_path)} && tail -n {n} {shlex.quote(out_path)} || echo '(no stdout yet)'")
    err_txt, _, _ = _dexec(container, f"test -f {shlex.quote(err_path)} && tail -n {n} {shlex.quote(err_path)} || echo '(no stderr yet)'")
    return out_txt, err_txt


@asset(name="slurm_submit_external")
def slurm_submit_external(context: AssetExecutionContext) -> MaterializeResult:
    """
    Upload a local script (default: external_file.py) to /data, submit to Slurm in Docker,
    wait for completion, and materialize what happened.
    """
    controller = _find_controller()
    partition = _detect_partition(controller)  # may be None -> default

    # choose the script you want to run:
    local_script = os.environ.get("LOCAL_EXTERNAL_FILE", DEFAULT_LOCAL_SCRIPT)
    context.log.info(f"Using local script: {local_script}")

    remote_script = f"{CONTAINER_DATA_DIR}/external_file.py"
    _put_file(controller, local_script, remote_script)

    submit = _submit_python(
        controller,
        remote_script,
        job_name="external",
        minutes=2,
        cpus=1,
        mem_mb=256,
        args=["--message", "hello from dagster"],  # only used by a non-Pipes payload like test.py
        partition=partition,
    )
    context.log.info(f"Submitted job id={submit.job_id} on node={submit.node or 'unknown'}")

    final_state = _wait_done(controller, submit.job_id)
    context.log.info(f"Job {submit.job_id} finished: {final_state}")

    out_txt, err_txt = _tail_logs(controller, "external", submit.job_id, n=80)
    # (Optionally) bring back results directory to host here with docker cp, or parse summary from stdout.

    # Materialize: attach rich metadata so you can inspect in the UI.
    md = {
        "job_id": submit.job_id,
        "node": submit.node or "unknown",
        "state": final_state,
        "stdout_tail": out_txt,
        "stderr_tail": err_txt,
    }
    return MaterializeResult(metadata=md)