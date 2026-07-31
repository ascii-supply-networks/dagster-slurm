"""Shared SSH ControlMaster conventions.

HPC login nodes rate-limit (and increasingly ban) accounts that open a fresh
TCP connection, key exchange and authentication for every remote command.
Slurm supervision is inherently chatty - status polling, log tailing, Pipes
message reading - so every SSH and SCP invocation in this package must share a
single multiplexed connection per ``(user, host, port)``.

This module owns the socket naming so that the connection pool, one-off helper
commands and long-running ``tail`` processes all point at the *same* socket.
A per-process random socket name would technically enable multiplexing while
still paying a full handshake per Dagster step, per sensor tick and per
process - exactly the behaviour cluster operators complain about.

Environment overrides:

- ``DAGSTER_SLURM_SSH_CONTROL_DIR``: directory holding the control sockets
  (default ``~/.ssh/dagster-slurm``).
- ``DAGSTER_SLURM_SSH_CONTROL_PERSIST``: OpenSSH ``ControlPersist`` value kept
  after the last client detaches (default ``10m``).
"""

import hashlib
import os
from pathlib import Path
from typing import Optional

CONTROL_DIR_ENV = "DAGSTER_SLURM_SSH_CONTROL_DIR"
CONTROL_PERSIST_ENV = "DAGSTER_SLURM_SSH_CONTROL_PERSIST"

DEFAULT_CONTROL_DIR = "~/.ssh/dagster-slurm"
DEFAULT_CONTROL_PERSIST = "10m"

# AF_UNIX socket paths are capped near 104 bytes on macOS and 108 on Linux.
# Stay well below the lower bound so OpenSSH never silently refuses the socket.
MAX_CONTROL_PATH_LENGTH = 100

_control_dir_ready: set[str] = set()


def control_persist_value() -> str:
    """Return the configured ``ControlPersist`` value."""
    value = os.getenv(CONTROL_PERSIST_ENV, "").strip()
    return value or DEFAULT_CONTROL_PERSIST


def control_dir() -> Path:
    """Return the directory that holds ControlMaster sockets."""
    configured = os.getenv(CONTROL_DIR_ENV, "").strip()
    return Path(configured or DEFAULT_CONTROL_DIR).expanduser()


def ensure_control_dir() -> Optional[Path]:
    """Create the control socket directory with private permissions.

    Returns:
        The directory, or None when it could not be prepared.

    """
    directory = control_dir()
    key = str(directory)
    if key in _control_dir_ready:
        return directory

    try:
        directory.mkdir(parents=True, exist_ok=True)
        os.chmod(directory, 0o700)
    except OSError:
        return None

    _control_dir_ready.add(key)
    return directory


def control_socket_path(user: str, host: str, port: int) -> Optional[str]:
    """Return the deterministic control socket path for a connection target.

    The same target always maps to the same socket, so a master started by one
    Dagster step, sensor tick or process is reused by every later command
    instead of triggering another handshake.
    """
    directory = ensure_control_dir()
    if directory is None:
        return None

    target = f"{user}@{host}:{port}"
    path = directory / f"cm-{target}"
    if len(str(path)) >= MAX_CONTROL_PATH_LENGTH:
        digest = hashlib.sha256(target.encode()).hexdigest()[:16]
        path = directory / f"cm-{digest}"
    if len(str(path)) >= MAX_CONTROL_PATH_LENGTH:
        return None

    return str(path)


def multiplexing_opts(control_path: Optional[str], *, create: bool) -> list[str]:
    """Build OpenSSH options that attach a command to the shared control socket.

    Args:
        control_path: Socket path, or None to disable multiplexing.
        create: When True the command may promote itself to master if no master
            is running yet (``ControlMaster=auto``). Use False for commands
            issued by :class:`~dagster_slurm.helpers.ssh_pool.SSHConnectionPool`,
            which manages the master explicitly.

    """
    if not control_path:
        return []

    opts = [
        "-o",
        f"ControlPath={control_path}",
        "-o",
        f"ControlMaster={'auto' if create else 'no'}",
    ]
    if create:
        opts.extend(["-o", f"ControlPersist={control_persist_value()}"])
    return opts
