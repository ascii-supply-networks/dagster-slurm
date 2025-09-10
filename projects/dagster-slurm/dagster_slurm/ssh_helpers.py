import os
import shlex
import subprocess
from pathlib import Path
from dagster import get_dagster_logger
from glob import glob as _glob
import logging 
import sys

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

def _latest(pattern: str | Path) -> Path | None:
    matches = [_p for _p in (_glob(str(pattern)) or [])]
    if not matches:
        return None
    paths = [Path(m) for m in matches]
    return max(paths, key=lambda p: p.stat().st_mtime)

def _run_logged(cmd: list[str], *, cwd: Path | None, env: dict, log: logging.Logger, label: str):
    log.info(f"{label}: running: {shlex.join(cmd)}")
    log.info(f"{label}: cwd={cwd}")
    p = subprocess.run(cmd, cwd=str(cwd) if cwd else None, env=env,
                       capture_output=True, text=True)
    if p.returncode != 0:
        log.error(f"{label}: returncode={p.returncode}")
        if p.stdout:
            log.error(f"{label}: stdout:\n{p.stdout}")
        if p.stderr:
            log.error(f"{label}: stderr:\n{p.stderr}")
        raise subprocess.CalledProcessError(p.returncode, cmd, p.stdout, p.stderr)
    if p.stdout:
        log.debug(f"{label}: stdout:\n{p.stdout}")
    if p.stderr:
        log.debug(f"{label}: stderr:\n{p.stderr}")
    return p


def upload_lib(
    source: str,                # ../projects/dagster-slurm
    dest: str,                  # remote run dir: /home/submitter/pipes/<run_id>
    *, 
    examples_dir: str | None = "../../examples",  # repo ./examples (where tasks.pack lives)
    package_name: str = "dagster_slurm",
) -> str:
    log = get_dagster_logger()
    env = _clean_env()
    pkg_dir = Path(source).resolve()
    if not pkg_dir.exists():
        log.error(f"package_src_dir not found: {pkg_dir}")
        raise FileNotFoundError(f"package_src_dir not found: {pkg_dir}")
    log.debug(f"upload_lib: package_src_dir={pkg_dir}")

    wheel_local: Path | None = None

    # 1) Preferred: build via examples task
    if examples_dir:
        log.debug(f"upload_lib: examples_dir (requested)={examples_dir}")
        ex_dir = Path(examples_dir).resolve()
        log.debug(f"upload_lib: ex_dir (resolved)={ex_dir}")
        if (ex_dir / "pyproject.toml").exists():
            _run_logged(
                    ["pixi", "run", "-e", "dev", "--frozen", "pack"],
                    cwd=ex_dir,
                    env=env,
                    log=log,
                    label="pixi-pack(examples)",
                )
            # task writes wheels to one of these
            wheel_local = (
                _latest(str(ex_dir.parent / "dist" / f"{package_name}-*.whl"))
                or _latest(str(pkg_dir / "dist" / f"{package_name}-*.whl"))
            )
        log.debug(f"Using wheel: {wheel_local}")


    if wheel_local is None:
            raise RuntimeError(f"No wheel found in {pkg_dir/'dist'} after build")
    wheel_remote = f"{dest}/{wheel_local.name}"
    log.info(f"Uploading wheel to remote: {wheel_remote}")
    scp_put(str(wheel_local), wheel_remote)
    ssh_check(f"chmod a+r {shlex.quote(wheel_remote)}")
    log.info(f"Wheel uploaded: {wheel_remote}")
    return wheel_remote

def install_lib_locally(
    source: str,
    *,
    examples_dir: str | None,
    package_name: str,
    log: logging.Logger,
) -> None:
    """Builds a wheel from source and pip installs it into the current Python environment."""
    env = _clean_env()
    pkg_dir = Path(source).resolve()
    if not pkg_dir.exists():
        raise FileNotFoundError(f"Package source directory not found: {pkg_dir}")
    log.debug(f"install_lib_locally: package_src_dir={pkg_dir}")

    wheel_local: Path | None = None

    # 1. Build the wheel using the same logic as upload_lib
    if examples_dir:
        ex_dir = Path(examples_dir).resolve()
        if (ex_dir / "pyproject.toml").exists():
            _run_logged(
                ["pixi", "run", "-e", "dev", "--frozen", "pack"],
                cwd=ex_dir,
                env=env,
                log=log,
                label="pixi-pack(local-install)",
            )
            # Find the built wheel
            wheel_local = (
                _latest(str(ex_dir.parent / "dist" / f"{package_name}-*.whl"))
                or _latest(str(pkg_dir / "dist" / f"{package_name}-*.whl"))
            )
    
    if wheel_local is None:
        raise RuntimeError(f"No wheel found in {pkg_dir/'dist'} or parent dist after build")

    log.debug(f"Found wheel for local install: {wheel_local}")

    # 2. Pip install the wheel into the current environment
    # Using sys.executable ensures we use the pip from the correct virtual env
    pip_install_cmd = [
        sys.executable,
        "-m", "pip", "install",
        "--force-reinstall", # Ensures the latest build is used
        str(wheel_local)
    ]
    _run_logged(
        pip_install_cmd,
        cwd=None, # Run from anywhere
        env=os.environ.copy(), # Use current env
        log=log,
        label="pip-install-local-wheel"
    )
    log.info(f"Successfully installed {wheel_local.name} into the current environment.")
        
TERMINAL_STATES = {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "PREEMPTED", "NODE_FAIL"}

