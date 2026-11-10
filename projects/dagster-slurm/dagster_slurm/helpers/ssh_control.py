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
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from dagster import get_dagster_logger

CONTROL_DIR_ENV = "DAGSTER_SLURM_SSH_CONTROL_DIR"
CONTROL_PERSIST_ENV = "DAGSTER_SLURM_SSH_CONTROL_PERSIST"

DEFAULT_CONTROL_DIR = "~/.ssh/dagster-slurm"
DEFAULT_CONTROL_PERSIST = "10m"

# AF_UNIX socket paths are capped near 104 bytes on macOS and 108 on Linux.
# Stay well below the lower bound so OpenSSH never silently refuses the socket.
MAX_CONTROL_PATH_LENGTH = 100

#: Hex characters kept from the target digest when the readable socket name is
#: too long. 48 bits is far more than enough to keep distinct hosts apart, and
#: a shorter name leaves more headroom for deep control directories.
DIGEST_LENGTH = 12


def control_persist_value() -> str:
    """Return the configured ``ControlPersist`` value."""
    value = os.getenv(CONTROL_PERSIST_ENV, "").strip()
    return value or DEFAULT_CONTROL_PERSIST


def control_dir() -> Path:
    """Return the directory that holds ControlMaster sockets."""
    configured = os.getenv(CONTROL_DIR_ENV, "").strip()
    return Path(configured or DEFAULT_CONTROL_DIR).expanduser()


def ensure_control_dir() -> Path | None:
    """Create the control socket directory with private permissions.

    Returns:
        The directory, or None when it could not be prepared.

    """
    directory = control_dir()
    try:
        directory.mkdir(parents=True, exist_ok=True)
        os.chmod(directory, 0o700)
    except OSError:
        return None

    return directory


@contextmanager
def master_start_lock(control_path: str) -> Iterator[None]:
    """Serialize cold starts across workers without opening SSH connections."""
    import fcntl

    with open(f"{control_path}.lock", "a") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def control_socket_path(user: str, host: str, port: int) -> str | None:
    """Return the deterministic control socket path for a connection target.

    The same target always maps to the same socket, so a master started by one
    Dagster step, sensor tick or process is reused by every later command
    instead of triggering another handshake.
    """
    directory = ensure_control_dir()
    if directory is None:
        get_dagster_logger().warning(
            "Could not prepare the SSH control directory %s, so connections to "
            "%s@%s cannot be multiplexed. Set %s to a writable directory.",
            control_dir(),
            user,
            host,
            CONTROL_DIR_ENV,
        )
        return None

    target = f"{user}@{host}:{port}"
    path = directory / f"cm-{target}"
    if len(str(path)) >= MAX_CONTROL_PATH_LENGTH:
        digest = hashlib.sha256(target.encode()).hexdigest()[:DIGEST_LENGTH]
        path = directory / f"cm-{digest}"
    if len(str(path)) >= MAX_CONTROL_PATH_LENGTH:
        # Silently giving up here would disable multiplexing exactly where it
        # is hardest to notice, so say so instead.
        get_dagster_logger().warning(
            "SSH control socket path would exceed %s characters under %s, so "
            "connections to %s@%s cannot be multiplexed. Point %s at a shorter "
            "directory path.",
            MAX_CONTROL_PATH_LENGTH,
            directory,
            user,
            host,
            CONTROL_DIR_ENV,
        )
        return None

    return str(path)


def multiplexing_opts(control_path: str | None) -> list[str]:
    """Attach to an existing master, refusing any direct network fallback.

    Only SSHConnectionPool creates masters, under its cross-process lock.
    """
    if not control_path:
        raise RuntimeError(
            "SSH ControlMaster socket is unavailable; refusing unpooled SSH"
        )
    return [
        "-o",
        f"ControlPath={control_path}",
        "-o",
        "ControlMaster=no",
        # A live master bypasses ProxyCommand. If its socket disappears between
        # health check and exec, OpenSSH must not reconnect directly.
        "-o",
        "ProxyCommand=false",
    ]
