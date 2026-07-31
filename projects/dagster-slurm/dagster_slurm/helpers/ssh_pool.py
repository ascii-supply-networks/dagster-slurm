"""SSH connection pooling via ControlMaster."""

import os
import shlex
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Callable, Optional, Pattern, Union, cast

from dagster import get_dagster_logger
from typing import TYPE_CHECKING

from .ssh_control import control_persist_value, control_socket_path

if TYPE_CHECKING:
    from ..resources.ssh import SSHConnectionResource

#: ControlMaster was expected to work and did not. Actionable, and serious.
MULTIPLEXING_FAILED_MESSAGE = (
    "SSH MULTIPLEXING FAILED for {host}. Dagster is falling back to one-off SSH "
    "connections. Frequent Slurm polling and log streaming may overload the HPC "
    "login node's SSH capacity and could cause the account to be blocked. "
    "Stop the run and fix ControlMaster configuration. Reason: {reason}"
)

#: ControlMaster cannot apply to this configuration at all. Still worth saying,
#: because the connection load is real, but nothing is broken and there is no
#: ControlMaster configuration to fix.
MULTIPLEXING_UNSUPPORTED_MESSAGE = (
    "SSH multiplexing is unavailable for {host} because {reason}. OpenSSH cannot "
    "share a connection that may still need to answer a password prompt, so every "
    "command opens its own connection. HPC sites rate-limit that, so prefer "
    "key-based authentication where the site allows it."
)


class SSHConnectionPool:
    """Reuse SSH connections via ControlMaster.

    The control socket is derived deterministically from ``user@host:port``
    (see :mod:`dagster_slurm.helpers.ssh_control`), so a master started by one
    Dagster step, sensor tick or process is reused by all later ones instead of
    triggering another DNS lookup, TCP handshake and authentication.

    The master is health-checked before use. OpenSSH silently opens a *full*
    connection when a ``ControlPath`` socket is stale, which would otherwise
    turn a one-second Slurm polling loop into thousands of handshakes per hour
    while the pool still reported multiplexing as healthy.

    Supports both key-based and password-based authentication. Password-based
    auth cannot be multiplexed and always uses one-off connections.
    """

    _CLOSE_MASTER_ENV = "DAGSTER_SLURM_SSH_CLOSE_MASTER_ON_EXIT"

    #: Minimum seconds between two ``ssh -O check`` probes of the master.
    MASTER_CHECK_INTERVAL_SECONDS = 15.0

    def __init__(self, ssh_config: "SSHConnectionResource"):
        self.config = ssh_config
        self.logger = get_dagster_logger()
        self.control_path: Optional[str] = self._prepare_control_path()
        self._master_started = False
        self._owns_master = False
        self._fallback_mode = False
        self._fallback_reason: Optional[str] = None
        self._lock = threading.RLock()
        self._depth = 0
        self._last_master_check = 0.0
        self._fallback_unsupported = False
        #: Optional (level, message) sink. When unset the pool logs the state
        #: itself, so every caller - sensors, session and hetjob setup included
        #: - reports it without having to remember to.
        self.reporter: Optional[Callable[[str, str], None]] = None

    def _collect_passwords(self) -> list[str]:
        passwords: list[str] = []
        if self.config.jump_host and self.config.jump_host.password:
            passwords.append(self.config.jump_host.password)
        if self.config.password:
            passwords.append(self.config.password)
        return passwords

    def _prepare_control_path(self) -> Optional[str]:
        """Return the shared ControlMaster socket path, or None when unusable."""
        if not self.config.supports_multiplexing:
            return None

        path = control_socket_path(self.config.user, self.config.host, self.config.port)
        if path is None:
            self.logger.warning(
                "Could not prepare the SSH control directory for %s. "
                "Falling back to non-pooled SSH connections.",
                self.config.host,
            )
        return path

    @property
    def multiplexing_active(self) -> bool:
        """Whether commands are using the pool's ControlMaster connection."""
        return self._master_started and not self._fallback_mode

    @property
    def fallback_reason(self) -> str | None:
        """Why the pool is using one-off SSH connections, if known."""
        return self._fallback_reason

    def _multiplexing_opts(self) -> list[str]:
        """ControlMaster options for a command issued through this pool."""
        return self.config.get_multiplexing_opts(
            create=False, control_path=self.control_path
        )

    def _master_is_alive(self) -> bool:
        """Ask OpenSSH whether the control socket has a live master behind it.

        ``ssh -O check`` only talks to the local unix socket, so this is cheap
        and never creates a network connection.
        """
        if not self.control_path:
            return False

        try:
            completed = subprocess.run(
                [
                    "ssh",
                    "-O",
                    "check",
                    "-o",
                    f"ControlPath={self.control_path}",
                    f"{self.config.user}@{self.config.host}",
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
        except Exception:  # pragma: no cover - best effort probe
            return False

        return completed.returncode == 0

    def _spawn_master(self) -> None:
        """Start a background ControlMaster, raising on failure."""
        cmd = [
            "ssh",
            # Keepalives matter: without them an idle NAT or firewall silently
            # drops the master and every later command reconnects on its own.
            *self.config.get_common_ssh_opts(
                server_alive_interval=30,
                server_alive_count_max=6,
            ),
            "-M",
            "-N",
            "-f",
            "-p",
            str(self.config.port),
            "-o",
            f"ControlPath={self.control_path}",
            "-o",
            f"ControlPersist={control_persist_value()}",
        ]
        cmd.extend(self.config.get_proxy_command_opts())
        cmd.extend(self.config.get_key_auth_opts())
        cmd.append(f"{self.config.user}@{self.config.host}")

        completed = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if completed.returncode != 0:
            # Another process may have won the race for the same socket.
            if self._master_is_alive():
                self._owns_master = False
                return
            stderr = completed.stderr
            raise RuntimeError(
                "Failed to start SSH master: "
                f"{stderr.strip()}{self.config.host_key_error_hint(stderr)}"
                f"{self.config.auth_error_hint(stderr)}"
            )

        self._owns_master = True

    @property
    def multiplexing_unsupported(self) -> bool:
        """Whether multiplexing is impossible here rather than broken.

        Password authentication cannot be multiplexed by OpenSSH at all, so
        reporting it as a ControlMaster *failure* would be both untrue and
        unactionable - and would fire on every single run of such a deployment.
        """
        return self._fallback_unsupported

    def describe_multiplexing(self) -> Optional[tuple[str, str]]:
        """Return (log level, message) describing a degraded state, if any."""
        if not self._fallback_mode:
            return None
        reason = self._fallback_reason or "unknown ControlMaster startup failure"
        if self._fallback_unsupported:
            return (
                "warning",
                MULTIPLEXING_UNSUPPORTED_MESSAGE.format(
                    host=self.config.host, reason=reason
                ),
            )
        return (
            "error",
            MULTIPLEXING_FAILED_MESSAGE.format(host=self.config.host, reason=reason),
        )

    def _report_multiplexing_state(self) -> None:
        """Report a degraded connection once, however the pool was created."""
        state = self.describe_multiplexing()
        if state is None:
            return
        level, message = state
        if self.reporter is not None:
            try:
                self.reporter(level, message)
                return
            except Exception as exc:  # pragma: no cover - reporting must not fail
                self.logger.debug(f"Multiplexing reporter failed: {exc}")
        if level == "error":
            self.logger.error(message)
        else:
            self.logger.warning(message)

    def _enter_fallback(self, reason: str, *, unsupported: bool = False) -> None:
        """Switch to one-off connections, record why, and say so."""
        self._fallback_mode = True
        self._fallback_reason = reason
        self._fallback_unsupported = unsupported
        self.control_path = None
        self._report_multiplexing_state()

    def _start_master(self) -> None:
        """Reuse a live ControlMaster or start a new one."""
        self.logger.debug("Starting SSH ControlMaster...")

        unsupported_reason = self.config.multiplexing_unsupported_reason
        if unsupported_reason:
            self.logger.debug(
                "ControlMaster disabled for %s because password-based or jump-host "
                "authentication is in use.",
                self.config.host,
            )
            self._enter_fallback(unsupported_reason, unsupported=True)
            return

        if not self.control_path:
            self._enter_fallback("SSH control socket path is unavailable")
            return

        if self._master_is_alive():
            self._master_started = True
            self._owns_master = False
            self._last_master_check = time.monotonic()
            self.logger.debug(
                "Reusing existing SSH ControlMaster at %s", self.control_path
            )
            return

        try:
            self._spawn_master()
        except Exception as exc:
            self._enter_fallback(str(exc))
            return

        self._master_started = True
        self._last_master_check = time.monotonic()
        auth_method = "key" if self.config.uses_key_auth else "inherited"
        self.logger.debug(f"SSH ControlMaster started ({auth_method} auth)")

    def _ensure_master_alive(self, *, force: bool = False) -> None:
        """Re-check and, if needed, restart the master before running a command.

        Without this a dead socket degrades silently: OpenSSH just opens a full
        connection per command and reports success.
        """
        if not self._master_started or self._fallback_mode:
            return

        now = time.monotonic()
        if (
            not force
            and now - self._last_master_check < self.MASTER_CHECK_INTERVAL_SECONDS
        ):
            return

        self._last_master_check = now
        if self._master_is_alive():
            return

        self.logger.warning(
            "SSH ControlMaster for %s is gone; restarting it before continuing.",
            self.config.host,
        )
        try:
            self._spawn_master()
        except Exception as exc:
            self._enter_fallback(
                f"ControlMaster died mid-run and could not be restarted: {exc}"
            )
            return

        self._last_master_check = time.monotonic()
        self.logger.info("SSH ControlMaster for %s restarted.", self.config.host)

    def __enter__(self):
        """Start (or attach to) the shared SSH ControlMaster."""
        with self._lock:
            self._depth += 1
            if self._depth > 1:
                # Nested/re-entered context: the master is already set up.
                return self
            if self._master_started and not self._fallback_mode:
                # Re-entering a pool whose master was intentionally left alive.
                self._ensure_master_alive(force=True)
                return self
            self._start_master()
            return self

    def _run_with_password(self, cmd, password, timeout=30):
        """Run SSH command with password using pexpect."""
        try:
            import pexpect
        except ImportError:
            raise RuntimeError(
                "Password authentication requires 'pexpect' library.\n"
                "Install it with: pip install pexpect\n\n"
                "Alternatively, use key-based authentication instead."
            )

        # Join command for pexpect
        cmd_str = " ".join(shlex.quote(arg) for arg in cmd)

        if isinstance(password, (list, tuple)):
            passwords = [p for p in password if p]
        elif password:
            passwords = [password]
        else:
            passwords = []

        if not passwords:
            raise RuntimeError("No password provided for SSH authentication.")

        fallback_password = passwords[-1]
        pw_index = 0

        try:
            effective_timeout = timeout or 60
            child = pexpect.spawn(cmd_str, timeout=effective_timeout, encoding="utf-8")
            child.delaybeforesend = 0
            prompt_timeout = float(max(30, min(effective_timeout, 180)))
            stdout_chunks: list[str] = []

            ExpectPattern = Union[
                Pattern[str],
                Pattern[bytes],
                bytes,
                str,
                type[pexpect.EOF | pexpect.TIMEOUT],
            ]
            prompts = cast(
                list[ExpectPattern],
                [
                    r"(?i)password:",
                    r"(?i)passphrase",
                    r"(?i)verification code",
                    r"(?i)otp",
                    pexpect.EOF,
                    pexpect.TIMEOUT,
                ],
            )

            while True:
                index = child.expect(prompts, timeout=prompt_timeout)

                # Password / passphrase prompts
                if index in (0, 1, 2, 3):
                    if pw_index < len(passwords):
                        child.sendline(passwords[pw_index])
                        pw_index += 1
                    else:
                        child.sendline(fallback_password)
                    continue

                # EOF -> command finished
                if index == len(prompts) - 2:
                    stdout_chunks.append(child.before or "")
                    break

                # TIMEOUT -> assume authentication finished and command running
                stdout_chunks.append(child.before or "")
                break

            child.close()

            class Result:
                def __init__(self, returncode, stdout, stderr):
                    self.returncode = returncode
                    self.stdout = stdout
                    self.stderr = stderr

            return Result(
                returncode=child.exitstatus or 0,
                stdout="".join(stdout_chunks),
                stderr="",
            )

        except pexpect.exceptions.ExceptionPexpect as e:
            raise RuntimeError(f"Password authentication failed: {e}")

    def _should_close_master(self) -> bool:
        """Whether to tear the master down instead of leaving it for reuse."""
        return os.getenv(self._CLOSE_MASTER_ENV, "").strip().lower() in {
            "1",
            "true",
            "yes",
        }

    def close_master(self) -> None:
        """Terminate the shared ControlMaster immediately."""
        with self._lock:
            if not (self._master_started and not self._fallback_mode):
                return
            try:
                subprocess.run(
                    [
                        "ssh",
                        "-O",
                        "exit",
                        "-o",
                        f"ControlPath={self.control_path}",
                        f"{self.config.user}@{self.config.host}",
                    ],
                    capture_output=True,
                    timeout=5,
                )
                self.logger.debug("SSH ControlMaster closed")
            except Exception as e:
                self.logger.warning(f"Error closing SSH master: {e}")
            finally:
                self._master_started = False
                self._owns_master = False

    def __exit__(self, *args):
        """Detach from the master, leaving it alive for the next caller.

        Tearing the master down here would make ``ControlPersist`` pointless:
        the next Dagster step or sensor tick would pay another full handshake.
        Set ``DAGSTER_SLURM_SSH_CLOSE_MASTER_ON_EXIT=1`` to restore the old
        eager-shutdown behaviour.
        """
        with self._lock:
            self._depth = max(0, self._depth - 1)
            if self._depth > 0:
                return
            if self._should_close_master():
                self.close_master()

    def run(self, cmd: str, timeout: Optional[int] = None) -> str:
        """Run command using pooled connection.

        Args:
            cmd: Shell command to execute
            timeout: Command timeout in seconds

        Returns:
            Command stdout

        Raises:
            RuntimeError: If command fails or pool not started

        """
        with self._lock:
            if not self._master_started and not self._fallback_mode:
                raise RuntimeError("SSH pool not started - use context manager")

            self._ensure_master_alive()

            # Wrap in clean shell
            remote_cmd = f"bash --noprofile --norc -c {shlex.quote(cmd)}"
            if self.config.post_login_command:
                template = self.config.post_login_command
                if "{cmd}" in template:
                    remote_cmd = template.format(cmd=remote_cmd)
                else:
                    remote_cmd = f"{template} && {remote_cmd}"

            needs_tty = self.config.requires_tty

            if self._master_started and not self._fallback_mode:
                ssh_cmd = [
                    "ssh",
                    *self.config.get_common_ssh_opts(),
                    "-p",
                    str(self.config.port),
                    *self._multiplexing_opts(),
                ]
                ssh_cmd.extend(self.config.get_key_auth_opts())
                if needs_tty:
                    ssh_cmd.append("-tt")
                ssh_cmd.extend(self.config.get_proxy_command_opts())
                ssh_cmd.extend(
                    [
                        f"{self.config.user}@{self.config.host}",
                        remote_cmd,
                    ]
                )
            else:
                ssh_cmd = [
                    "ssh",
                    *self.config.get_common_ssh_opts(),
                    "-p",
                    str(self.config.port),
                ]
                ssh_cmd.extend(self.config.get_auth_opts())
                if needs_tty:
                    ssh_cmd.append("-tt")
                ssh_cmd.extend(self.config.get_proxy_command_opts())
                ssh_cmd.extend(
                    [
                        f"{self.config.user}@{self.config.host}",
                        remote_cmd,
                    ]
                )

            self.logger.debug(
                "Executing SSH command: %s",
                " ".join(shlex.quote(part) for part in ssh_cmd),
            )

            if self._master_started and not self._fallback_mode:
                result = subprocess.run(
                    ssh_cmd,
                    capture_output=True,
                    text=True,
                    timeout=timeout,
                )
                returncode = result.returncode
                stdout = result.stdout
                stderr = result.stderr
            else:
                if self.config.uses_password_auth or (
                    self.config.jump_host and self.config.jump_host.uses_password_auth
                ):
                    effective_timeout = timeout if timeout is not None else 300
                    result = self._run_with_password(
                        ssh_cmd, self._collect_passwords(), timeout=effective_timeout
                    )
                    returncode = result.returncode
                    stdout = result.stdout
                    stderr = result.stderr
                else:
                    result = subprocess.run(
                        ssh_cmd,
                        capture_output=True,
                        text=True,
                        timeout=timeout,
                    )
                    returncode = result.returncode
                    stdout = result.stdout
                    stderr = result.stderr

            if returncode != 0:
                raise RuntimeError(
                    f"SSH command failed (exit {returncode}): {cmd}\n"
                    f"stdout: {stdout}\n"
                    f"stderr: {stderr}{self.config.host_key_error_hint(stderr)}{self.config.auth_error_hint(stderr)}"
                )

            return stdout

    def write_file(self, content: str, remote_path: str):
        """Write content to remote file via heredoc."""
        if not content:
            raise ValueError("Cannot write empty content to file")

        content_lines = set(content.splitlines())
        delimiter = f"DAGSTER_EOF_{uuid.uuid4().hex}"
        while delimiter in content_lines:
            delimiter = f"DAGSTER_EOF_{uuid.uuid4().hex}"

        cmd = (
            f"cat > {shlex.quote(remote_path)} <<'{delimiter}'\n{content}\n{delimiter}"
        )

        try:
            self.run(cmd)
        except Exception as e:
            raise RuntimeError(f"Failed to write file to {remote_path}") from e

    def upload_file(self, local_path: str, remote_path: str):
        """Upload file via SCP using pooled connection."""
        if not self._master_started and not self._fallback_mode:
            raise RuntimeError("SSH pool not started")

        # Ensure remote directory exists
        remote_dir = str(Path(remote_path).parent)
        self.run(f"mkdir -p {shlex.quote(remote_dir)}")

        # Build SCP command
        with self._lock:
            self._ensure_master_alive()

            if self._master_started and not self._fallback_mode:
                scp_cmd = [
                    "scp",
                    *self.config.get_common_ssh_opts(),
                    "-C",  # Enable compression (critical for large files!)
                    "-P",
                    str(self.config.port),
                    *self._multiplexing_opts(),
                ]
                scp_cmd.extend(self.config.get_key_auth_opts())
            else:
                scp_cmd = [
                    "scp",
                    *self.config.get_common_ssh_opts(),
                    "-C",  # Enable compression (critical for large files!)
                    "-P",
                    str(self.config.port),
                ]
                scp_cmd.extend(self.config.get_auth_opts())

            scp_cmd.extend(self.config.get_proxy_command_opts())
            scp_cmd.extend(
                [
                    local_path,
                    f"{self.config.user}@{self.config.host}:{remote_path}",
                ]
            )

            self.logger.debug(
                "Executing SCP command: %s",
                " ".join(shlex.quote(part) for part in scp_cmd),
            )

            if self._master_started and not self._fallback_mode:
                proc = subprocess.run(scp_cmd, capture_output=True, text=True)
                returncode = proc.returncode
                stdout = proc.stdout
                stderr = proc.stderr
            else:
                if self.config.uses_password_auth or (
                    self.config.jump_host and self.config.jump_host.uses_password_auth
                ):
                    result = self._run_with_password(
                        scp_cmd, self._collect_passwords(), timeout=300
                    )
                    returncode = result.returncode
                    stdout = result.stdout
                    stderr = result.stderr
                else:
                    proc = subprocess.run(scp_cmd, capture_output=True, text=True)
                    returncode = proc.returncode
                    stdout = proc.stdout
                    stderr = proc.stderr

        if returncode != 0:
            raise RuntimeError(
                f"SCP upload failed: {local_path} -> {remote_path}\n"
                f"stdout: {stdout}\n"
                f"stderr: {stderr}{self.config.host_key_error_hint(stderr)}{self.config.auth_error_hint(stderr)}"
            )
