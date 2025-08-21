import os
import re
import shlex
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

CONTAINER = "slurmctld"
CONTAINER_DATA_DIR = "/data"
HOST_SHARED = os.path.abspath("./shared")
PIPES_BASE = os.path.join(HOST_SHARED, "pipes")

LOCAL_PAYLOAD = os.environ.get(
    "LOCAL_EXTERNAL_FILE",
    str(
        (
            Path(__file__).resolve().parents[3]
            / "dagster_slurm_example"
            / "defs"
            / "shell"
            / "external_file.py"
        ).resolve()
    ),
)

PYTHON_IN_NODE = os.environ.get("PYTHON_IN_NODE", "python3")

PYTHON_IN_NODE = "/usr/bin/python3"
DEFAULT_LOCAL_SCRIPT = os.path.join(os.path.dirname(__file__), "test.py")


@dataclass
class SubmitResult:
    job_id: int
    node: Optional[str]


def _submit_python(
    container: str,
    script_in_container: str,
    job_name="hello",
    minutes=2,
    cpus=1,
    mem_mb=256,
    args=None,
    partition: Optional[str] = None,
) -> SubmitResult:
    partition = _sanitize_partition(partition)
    argstr = " ".join(shlex.quote(a) for a in (args or []))
    # ensure shared dirs and permissive perms (dev-friendly)
    _dexec(
        container,
        "mkdir -p /data/logs /data/results && chmod 1777 /data/logs /data/results",
    )

    opts = [
        "-J",
        job_name,
        "-D",
        CONTAINER_DATA_DIR,
        "-o",
        f"{CONTAINER_DATA_DIR}/logs/{job_name}-%j.out",
        "-e",
        f"{CONTAINER_DATA_DIR}/logs/{job_name}-%j.err",
        "-t",
        f"00:{minutes:02d}:00",
        "-c",
        str(cpus),
        f"--mem={mem_mb}",
    ]
    if partition:
        opts += ["-p", partition]

    cmd = (
        "sbatch "
        + " ".join(shlex.quote(x) for x in opts)
        + " --wrap "
        + shlex.quote(f"{PYTHON_IN_NODE} {script_in_container} {argstr}")
    )
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
    o, _, _ = _dexec(container, f"squeue -h -j {job_id} -o '%T'")
    st = o.strip()
    if st:
        return st
    o, _, _ = _dexec(container, f"sacct -X -n -j {job_id} -o State")
    return o.strip().splitlines()[0].strip() if o.strip() else "UNKNOWN"


def _wait_done(container: str, job_id: int, poll=2.0) -> str:
    import time

    while True:
        st = _job_state(container, job_id)
        if st in ("PENDING", "RUNNING", "COMPLETING", "CONFIGURING"):
            time.sleep(poll)
            continue
        return st


def _tail_logs(container: str, job_name: str, job_id: int, n=50):
    out_path = f"{CONTAINER_DATA_DIR}/logs/{job_name}-{job_id}.out"
    err_path = f"{CONTAINER_DATA_DIR}/logs/{job_name}-{job_id}.err"
    out_txt, _, _ = _dexec(
        container,
        f"test -f {shlex.quote(out_path)} && tail -n {n} {shlex.quote(out_path)} || echo '(no stdout yet)'",
    )
    err_txt, _, _ = _dexec(
        container,
        f"test -f {shlex.quote(err_path)} && tail -n {n} {shlex.quote(err_path)} || echo '(no stderr yet)'",
    )
    return out_txt, err_txt


def upload_file(local_path: str, dest_uri: str, *, mkdirs=True): # noqa: C901
    """
    Copy a *local* file to one of:
      - docker://<container>/<abs/path>
      - ssh://[user@]host/<abs/path>
      - s3://bucket/key
      - file:///abs/path   (or plain absolute path)
    Returns the destination string you can run.
    """
    local_path = os.path.abspath(local_path)
    if not os.path.isfile(local_path):
        raise FileNotFoundError(local_path)

    u = urlparse(dest_uri)
    scheme = u.scheme or ("file" if os.path.isabs(dest_uri) else "")
    path = u.path

    if scheme == "docker":
        container = u.netloc
        if not container or not path.startswith("/"):
            raise ValueError("docker URI must be docker://<container>/<abs/path>")
        if mkdirs:
            _run(
                [
                    "docker",
                    "exec",
                    "-i",
                    container,
                    "bash",
                    "-lc",
                    f"mkdir -p {shlex.quote(os.path.dirname(path))}",
                ]
            )
        _run(["docker", "cp", local_path, f"{container}:{path}"])
        _run(
            [
                "docker",
                "exec",
                "-i",
                container,
                "bash",
                "-lc",
                f"chmod a+r {shlex.quote(path)}",
            ]
        )
        return dest_uri

    if scheme in ("file", ""):
        # local filesystem copy
        dst = path if scheme == "file" else dest_uri
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        with open(local_path, "rb") as src, open(dst, "wb") as out:
            out.write(src.read())
        os.chmod(dst, 0o644)
        return f"file://{dst}"

    raise ValueError(f"Unsupported scheme: {scheme}")


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
    return _run(
        ["docker", "exec", "-i", container, "bash", "-lc", inner_cmd], stdin_bytes
    )


def _wait_slurm_done(job_id, poll=2.0):
    while True:
        out, _, _ = _dexec(CONTAINER, f"squeue -h -j {job_id} -o '%T'")
        st = out.strip()
        if not st:
            out, _, _ = _dexec(CONTAINER, f"sacct -X -n -j {job_id} -o State")
            st = out.strip().splitlines()[0].strip() if out.strip() else "UNKNOWN"
        if st in {"PENDING", "RUNNING", "COMPLETING", "CONFIGURING"}:
            time.sleep(poll)
            continue
        return st


def _find_controller() -> str:
    out, _, _ = _run(["docker", "ps", "--format", "{{.Names}}"])
    for name in out.strip().splitlines():
        if "slurmctld" in name:
            return name
    raise RuntimeError(
        "Could not find controller container (name containing 'slurmctld')."
    )


def _put_file(container: str, local_path: str, remote_path: str):
    local_path = os.path.abspath(local_path)
    if not os.path.isfile(local_path):
        raise FileNotFoundError(local_path)
    remote_dir = os.path.dirname(remote_path) or "/"
    _dexec(container, f"mkdir -p {shlex.quote(remote_dir)}")
    _run(["docker", "cp", local_path, f"{container}:{remote_path}"])
    # normalize line endings & make readable
    _dexec(
        container,
        f"sed -i 's/\\r$//' {shlex.quote(remote_path)}; chmod a+r {shlex.quote(remote_path)}",
    )


def _sanitize_partition(p: Optional[str]) -> Optional[str]:
    if not p:
        return None
    p = p.split(",")[0].rstrip("*").strip()
    return p or None


def _detect_partition(container: str = CONTAINER) -> Optional[str]:
    out, _, _ = _dexec(container, "sinfo -h -o '%P' | head -n1")
    return _sanitize_partition(out.strip())
