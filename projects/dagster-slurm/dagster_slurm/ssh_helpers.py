import os
import shlex
import subprocess
from pathlib import Path

# --- Connection settings ---
SSH_HOST = os.environ.get("SLURM_SSH_HOST", "localhost")
SSH_PORT = str(int(os.environ.get("SLURM_SSH_PORT", "2223")))
SSH_USER = os.environ.get("SLURM_SSH_USER", "submitter")

# Key can be set via SLURM_SSH_KEY (preferred) or SSH_KEY (fallback)
SSH_KEY = os.path.expanduser(
    os.environ.get("SLURM_SSH_KEY", os.environ.get("SSH_KEY", "~/.ssh/dagster-slurm"))
)

# Base options: non-interactive, key-only, quiet, and resilient
BASE_OPTS = [
    "-o", "StrictHostKeyChecking=no",
    "-o", "UserKnownHostsFile=/dev/null",
    "-o", "GlobalKnownHostsFile=/dev/null",
    "-o", "IdentitiesOnly=yes",
    "-o", "BatchMode=yes",
    "-o", "PreferredAuthentications=publickey",
    "-o", "PasswordAuthentication=no",
    "-o", "LogLevel=ERROR",
    "-o", "ServerAliveInterval=30",
    "-o", "ServerAliveCountMax=6",
]

# Allow extra site/user opts via env
EXTRA_OPTS = shlex.split(os.environ.get("SLURM_SSH_OPTS_EXTRA", ""))

COMMON_SSH_OPTS = BASE_OPTS + EXTRA_OPTS


def _ssh_cmdline(cmd: str) -> list[str]:
    # Run remote command in a clean shell so login banners or profile scripts can’t break it
    remote = "bash --noprofile --norc -lc " + shlex.quote(cmd)
    return [
        "ssh", "-p", SSH_PORT, "-i", SSH_KEY, *COMMON_SSH_OPTS,
        f"{SSH_USER}@{SSH_HOST}", remote
    ]


def ssh_run(cmd: str) -> tuple[str, str, int]:
    p = subprocess.run(_ssh_cmdline(cmd), capture_output=True, text=True)
    return p.stdout, p.stderr, p.returncode


def ssh_check(cmd: str) -> str:
    p = subprocess.run(_ssh_cmdline(cmd), capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(
            f"ssh failed: {cmd}\nstdout:\n{p.stdout}\nstderr:\n{p.stderr}"
        )
    return p.stdout


def scp_put(local_path: str, remote_path: str) -> None:
    # Note: remote_path should not contain unescaped spaces; your asset uses safe paths.
    dest = f"{SSH_USER}@{SSH_HOST}:{remote_path}"
    p = subprocess.run(
        ["scp", "-P", SSH_PORT, "-i", SSH_KEY, *COMMON_SSH_OPTS, local_path, dest],
        capture_output=True, text=True
    )
    if p.returncode != 0:
        raise RuntimeError(
            f"scp failed: {local_path} -> {remote_path}\nstdout:\n{p.stdout}\nstderr:\n{p.stderr}"
        )


def ssh_job_state(job_id: int) -> str:
    # try squeue first, then sacct
    out, _, _ = ssh_run(f"squeue -h -j {job_id} -o '%T' || true")
    s = (out or "").strip()
    if s:
        return s
    out, _, _ = ssh_run(f"sacct -X -n -j {job_id} -o State || true")
    s = (out or "").strip()
    return s.split()[0] if s else ""

def _clean_env() -> dict:
    env = os.environ.copy()
    env.pop("PIXI_ENVIRONMENT", None)
    env.pop("PIXI_PROJECT_MANIFEST", None)
    return env

def upload_lib(source: str, dest: str):
    env = _clean_env()
    pkg_dir = Path(source).resolve()
    if not pkg_dir.exists():
        raise FileNotFoundError(f"package_src_dir not found: {pkg_dir}")
    subprocess.run(["pixi","run", "-e", "default", "--manifest-path", str(pkg_dir / "pyproject.toml"), "build-wheel"], cwd=str(pkg_dir), check=True, env=env)
    wheels = sorted((pkg_dir / "dist").glob("*.whl"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not wheels:
        raise RuntimeError(f"No wheel found in {pkg_dir/'dist'} after build")
    wheel_local = wheels[0]
    wheel_remote = f"{dest}/{wheel_local.name}"
    scp_put(str(wheel_local), wheel_remote)   
    ssh_check(f"chmod a+rx {shlex.quote(wheel_remote)}")
    return wheel_remote  
    
TERMINAL_STATES = {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "PREEMPTED", "NODE_FAIL"}
