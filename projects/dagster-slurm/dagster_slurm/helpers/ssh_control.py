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
- ``DAGSTER_SLURM_SSH_FAILURE_BACKOFF_BASE``: seconds to wait after the first
  failed master start before the next login attempt (default ``60``). The wait
  doubles for each consecutive failure. ``0`` disables the backoff.
- ``DAGSTER_SLURM_SSH_FAILURE_BACKOFF_MAX``: upper limit in seconds for that
  wait (default ``1800``).
- ``DAGSTER_SLURM_SSH_MAX_SESSIONS``: sessions (commands, transfers, reads)
  that all processes together may open on one master at the same time
  (default ``4``). ``SSHConnectionResource.max_sessions`` takes precedence.
- ``DAGSTER_SLURM_SSH_SESSION_WAIT_TIMEOUT``: seconds to wait for a free
  session slot before failing (default ``1800``).

Failed master starts are recorded in ``<control path>.failed`` next to the
socket. Every process that uses the same target reads this marker under the
target's start lock, so a refused login is not retried by each sensor tick,
run worker or helper until the backoff window ends.

sshd refuses more than ``MaxSessions`` (default 10) concurrent sessions per
connection, and every Dagster step process, sensor and run worker shares the
same master. Each session therefore first takes one of ``max_sessions`` slots,
an ``flock`` on ``<control path>.slot<N>``. The kernel releases the lock when
its holder exits, so a crashed process cannot leak a slot.
"""

import hashlib
import json
import math
import os
import random
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from dagster import get_dagster_logger

CONTROL_DIR_ENV = "DAGSTER_SLURM_SSH_CONTROL_DIR"
CONTROL_PERSIST_ENV = "DAGSTER_SLURM_SSH_CONTROL_PERSIST"
FAILURE_BACKOFF_BASE_ENV = "DAGSTER_SLURM_SSH_FAILURE_BACKOFF_BASE"
FAILURE_BACKOFF_MAX_ENV = "DAGSTER_SLURM_SSH_FAILURE_BACKOFF_MAX"
MAX_SESSIONS_ENV = "DAGSTER_SLURM_SSH_MAX_SESSIONS"
SESSION_WAIT_TIMEOUT_ENV = "DAGSTER_SLURM_SSH_SESSION_WAIT_TIMEOUT"

DEFAULT_CONTROL_DIR = "~/.ssh/dagster-slurm"
DEFAULT_CONTROL_PERSIST = "10m"
DEFAULT_FAILURE_BACKOFF_BASE_SECONDS = 60.0
DEFAULT_FAILURE_BACKOFF_MAX_SECONDS = 1800.0
#: Below OpenSSH's default ``MaxSessions`` of 10, and safe on sites that lower it.
DEFAULT_MAX_SESSIONS = 4
DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS = 1800.0

#: Wait between two scans of the session slots, doubled up to the maximum.
_SLOT_RETRY_INITIAL_SECONDS = 0.02
_SLOT_RETRY_MAX_SECONDS = 0.5
#: Waits longer than this are logged, so a saturated master is visible.
_SLOT_WAIT_LOG_AFTER_SECONDS = 5.0

#: Characters of the failure reason kept in the marker file.
MAX_FAILURE_REASON_LENGTH = 500

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
def master_start_lock(control_path: str) -> Generator[None, None, None]:
    """Serialize cold starts across workers without opening SSH connections."""
    import fcntl

    with open(f"{control_path}.lock", "a") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


class SSHSessionSlotTimeoutError(RuntimeError):
    """No session slot on the shared master became free in time.

    A subclass of ``RuntimeError``, like every other failed SSH command.
    """


def resolve_max_sessions(configured: int | None = None) -> int:
    """Return the session limit per master: configured, from env, or default."""
    if configured is not None:
        return max(1, int(configured))
    raw = os.getenv(MAX_SESSIONS_ENV, "").strip()
    if not raw:
        return DEFAULT_MAX_SESSIONS
    try:
        value = int(raw)
    except ValueError:
        value = 0
    if value < 1:
        get_dagster_logger().warning(
            "Ignoring %s=%r: expected a positive integer. Using %s.",
            MAX_SESSIONS_ENV,
            raw,
            DEFAULT_MAX_SESSIONS,
        )
        return DEFAULT_MAX_SESSIONS
    return value


def session_wait_timeout() -> float:
    """Return how long a caller waits for a free session slot."""
    return _seconds_from_env(
        SESSION_WAIT_TIMEOUT_ENV, DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS
    )


def session_slot_path(control_path: str, index: int) -> Path:
    """Return the lock file of one session slot on a master."""
    return Path(f"{control_path}.slot{index}")


#: Slots held by the current thread: control path -> [fd, nesting depth].
_held_slots = threading.local()


def _slots_held_by_thread() -> dict[str, list[int]]:
    held = getattr(_held_slots, "slots", None)
    if held is None:
        held = {}
        _held_slots.slots = held
    return held


def _try_lock_slot(control_path: str, index: int) -> int | None:
    """Lock one slot without waiting; return its descriptor, or None if busy."""
    import fcntl

    # O_CLOEXEC keeps the lock out of the ssh/scp children, so it is released
    # exactly when this process lets go of it or dies.
    fd = os.open(
        session_slot_path(control_path, index),
        os.O_RDWR | os.O_CREAT | os.O_CLOEXEC,
        0o600,
    )
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        return None
    except BaseException:
        os.close(fd)
        raise
    return fd


def _acquire_slot(control_path: str, limit: int, timeout: float | None) -> int:
    """Wait for any free slot of ``limit`` and return its locked descriptor."""
    wait_limit = session_wait_timeout() if timeout is None else timeout
    started = time.monotonic()
    deadline = started + wait_limit
    # Start the scan at a process-specific slot so concurrent processes do not
    # all contend for slot 0 first.
    first = os.getpid() % limit
    delay = _SLOT_RETRY_INITIAL_SECONDS
    logged = False
    while True:
        for step in range(limit):
            fd = _try_lock_slot(control_path, (first + step) % limit)
            if fd is not None:
                if logged:
                    get_dagster_logger().debug(
                        "Got an SSH session slot for %s after %.1fs.",
                        control_path,
                        time.monotonic() - started,
                    )
                return fd
        now = time.monotonic()
        if now >= deadline:
            raise SSHSessionSlotTimeoutError(
                f"All {limit} SSH session slots for {control_path} stayed busy "
                f"for {wait_limit:.0f}s. Raise {MAX_SESSIONS_ENV} (keep it below "
                f"sshd's MaxSessions) or {SESSION_WAIT_TIMEOUT_ENV}."
            )
        if not logged and now - started >= _SLOT_WAIT_LOG_AFTER_SECONDS:
            logged = True
            get_dagster_logger().info(
                "Waiting for a free SSH session slot for %s: all %s are in use.",
                control_path,
                limit,
            )
        time.sleep(min(delay * random.uniform(0.5, 1.5), max(0.0, deadline - now)))
        delay = min(delay * 2, _SLOT_RETRY_MAX_SECONDS)


@contextmanager
def session_slot(
    control_path: str, limit: int, *, timeout: float | None = None
) -> Generator[None, None, None]:
    """Hold one of ``limit`` session slots on a master, across processes.

    Wraps every SSH session (command, transfer or read) on the shared master,
    so all processes together never open more than ``limit`` sessions and so
    never hit sshd's ``MaxSessions``. A caller waits for a free slot instead of
    failing; after ``timeout`` (default from
    ``DAGSTER_SLURM_SSH_SESSION_WAIT_TIMEOUT``) it raises
    :class:`SSHSessionSlotTimeoutError`.

    Nested calls in one thread reuse the slot the thread already holds.
    """
    held = _slots_held_by_thread()
    entry = held.get(control_path)
    if entry is not None:
        entry[1] += 1
        try:
            yield
        finally:
            entry[1] -= 1
        return

    fd = _acquire_slot(control_path, max(1, limit), timeout)
    held[control_path] = [fd, 1]
    try:
        yield
    finally:
        held.pop(control_path, None)
        os.close(fd)  # Closing the last descriptor releases the flock.


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


def _seconds_from_env(name: str, default: float) -> float:
    """Return a non-negative number of seconds from an env variable."""
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        value = -1.0
    if not math.isfinite(value) or value < 0:
        get_dagster_logger().warning(
            "Ignoring %s=%r: expected a non-negative number of seconds. Using %s.",
            name,
            raw,
            default,
        )
        return default
    return value


def failure_backoff_seconds(failures: int) -> float:
    """Return the wait after ``failures`` consecutive failed master starts."""
    base = _seconds_from_env(
        FAILURE_BACKOFF_BASE_ENV, DEFAULT_FAILURE_BACKOFF_BASE_SECONDS
    )
    cap = _seconds_from_env(
        FAILURE_BACKOFF_MAX_ENV, DEFAULT_FAILURE_BACKOFF_MAX_SECONDS
    )
    if failures < 1 or base == 0:
        return 0.0
    # Bound the exponent so a large failure count cannot overflow the float.
    return min(cap, base * 2 ** min(failures - 1, 32))


@dataclass(frozen=True)
class MasterStartFailure:
    """Consecutive failed master starts for one target, as stored on disk."""

    failed_at: float
    failures: int
    reason: str

    @property
    def retry_at(self) -> float:
        """Wall-clock time before which no new login is attempted."""
        return self.failed_at + failure_backoff_seconds(self.failures)

    def blocks(self, now: float | None = None) -> bool:
        """Whether a new master start must wait at ``now``.

        A marker from the future (for example after the clock moved back) does
        not block, so a clock change cannot lock the target out.
        """
        current = time.time() if now is None else now
        return self.failed_at <= current < self.retry_at


def failure_marker_path(control_path: str) -> Path:
    """Return the failed-start marker that belongs to a control socket."""
    return Path(f"{control_path}.failed")


def read_master_start_failure(control_path: str) -> MasterStartFailure | None:
    """Return the recorded master start failure, if any.

    Call this while holding :func:`master_start_lock`. A damaged marker is
    ignored, so it can never block a target permanently.
    """
    try:
        data = json.loads(failure_marker_path(control_path).read_text())
        return MasterStartFailure(
            failed_at=float(data["failed_at"]),
            failures=max(1, int(data["failures"])),
            reason=str(data.get("reason", "")),
        )
    except FileNotFoundError:
        return None
    except (OSError, ValueError, TypeError, KeyError) as exc:
        get_dagster_logger().debug(
            "Ignoring unreadable SSH failure marker for %s: %s", control_path, exc
        )
        return None


def record_master_start_failure(
    control_path: str, reason: str, *, now: float | None = None
) -> MasterStartFailure:
    """Count one more failed master start and return the new state.

    Call this while holding :func:`master_start_lock`. The marker is replaced
    atomically, so a reader never sees a partial file.
    """
    previous = read_master_start_failure(control_path)
    failure = MasterStartFailure(
        failed_at=time.time() if now is None else now,
        failures=(previous.failures if previous else 0) + 1,
        reason=reason.strip()[:MAX_FAILURE_REASON_LENGTH],
    )
    marker = failure_marker_path(control_path)
    temporary = marker.with_name(f"{marker.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(
                {
                    "failed_at": failure.failed_at,
                    "failures": failure.failures,
                    "reason": failure.reason,
                }
            )
        )
        os.replace(temporary, marker)
    except OSError as exc:
        temporary.unlink(missing_ok=True)
        get_dagster_logger().warning(
            "Could not record the failed SSH master start in %s: %s", marker, exc
        )
    return failure


def clear_master_start_failure(control_path: str) -> None:
    """Remove the failure marker after a successful master start."""
    failure_marker_path(control_path).unlink(missing_ok=True)
