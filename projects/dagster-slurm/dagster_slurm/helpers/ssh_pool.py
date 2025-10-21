"""SSH connection pooling via ControlMaster."""

import os
import shlex
import subprocess
import threading
import uuid
from pathlib import Path
from typing import Optional

from dagster import get_dagster_logger
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..resources.ssh import SSHConnectionResource


class SSHConnectionPool:
    """Reuse SSH connections via ControlMaster.
    Supports both key-based and password-based authentication.
    Password-based auth uses SSH_ASKPASS for secure password handling.
    """

    _CONTROL_DIR_ENV = "DAGSTER_SLURM_SSH_CONTROL_DIR"
    _DEFAULT_CONTROL_DIR = Path("~/.ssh/dagster-slurm").expanduser()

    def __init__(self, ssh_config: "SSHConnectionResource"):
        self.config = ssh_config
        self.control_path: Optional[str] = self._prepare_control_path()
        self._master_started = False
        self._fallback_mode = False
        self._fallback_reason: Optional[str] = None
        self.logger = get_dagster_logger()
        self._lock = threading.RLock()

    def _collect_passwords(self) -> list[str]:
        passwords: list[str] = []
        if self.config.jump_host and self.config.jump_host.password:
            passwords.append(self.config.jump_host.password)
        if self.config.password:
            passwords.append(self.config.password)
        return passwords

    def _prepare_control_path(self) -> Optional[str]:
        """Return a secure path for the ControlMaster socket or None on failure."""
        base_dir = os.getenv(self._CONTROL_DIR_ENV)
        if base_dir:
            control_dir = Path(base_dir).expanduser()
        else:
            control_dir = self._DEFAULT_CONTROL_DIR

        try:
            control_dir.mkdir(parents=True, exist_ok=True)
            os.chmod(control_dir, 0o700)
        except Exception as exc:  # pragma: no cover - best effort
            self.logger.warning(
                "Could not prepare SSH control directory %s (%s). "
                "Falling back to non-pooled SSH connections.",
                control_dir,
                exc,
            )
            return None

        socket_name = f"cm-{uuid.uuid4().hex[:12]}"
        control_path = control_dir / socket_name

        if len(str(control_path)) >= 100:
            control_path = control_dir / socket_name[:8]

        return str(control_path)

    def __enter__(self):
        """Start SSH ControlMaster."""
        self.logger.debug("Starting SSH ControlMaster...")

        if not self.control_path:
            self.logger.debug(
                "Control path unavailable; using direct SSH connections for %s",
                self.config.host,
            )
            self._fallback_mode = True
            return self

        if self.config.uses_password_auth or (
            self.config.jump_host and self.config.jump_host.uses_password_auth
        ):
            self.logger.debug(
                "ControlMaster disabled for %s because password-based or jump-host "
                "authentication is in use.",
                self.config.host,
            )
            self._fallback_mode = True
            self.control_path = None
            return self

        # Build master connection command
        base_opts = [
            "-o",
            f"ControlPath={self.control_path}",
            "-o",
            "ControlPersist=10m",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "LogLevel=ERROR",
        ]

        try:
            cmd = [
                "ssh",
                "-M",
                "-N",
                "-f",
                "-p",
                str(self.config.port),
            ]
            cmd.extend(base_opts)
            cmd.extend(self.config.get_proxy_command_opts())
            if self.config.uses_key_auth:
                key_path = self.config.key_path
                if not key_path:
                    raise RuntimeError(
                        "SSH key authentication requires key_path to be set"
                    )
                cmd.extend(
                    [
                        "-i",
                        key_path,
                        "-o",
                        "IdentitiesOnly=yes",
                        "-o",
                        "BatchMode=yes",
                    ]
                )
            cmd.extend(self.config.extra_opts)
            cmd.append(f"{self.config.user}@{self.config.host}")

            if self.config.uses_password_auth or (
                self.config.jump_host and self.config.jump_host.uses_password_auth
            ):
                result = self._run_with_password(cmd, self._collect_passwords(), 30)
                returncode = result.returncode
                stderr = result.stderr
            else:
                completed = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=30
                )
                returncode = completed.returncode
                stderr = completed.stderr

            if returncode != 0:
                raise RuntimeError(f"Failed to start SSH master: {stderr.strip()}")

            self._master_started = True
            auth_method = "key" if self.config.uses_key_auth else "password"
            self.logger.debug(f"SSH ControlMaster started ({auth_method} auth)")
        except Exception as exc:
            self._fallback_mode = True
            self._fallback_reason = str(exc)
            self.control_path = None
            self.logger.warning(
                "SSH ControlMaster unavailable (%s). Falling back to direct SSH "
                "connections; performance may be reduced but functionality remains.",
                exc,
            )

        return self

    def _run_with_password(self, cmd, password, timeout=30):
        """Run SSH command with password using pexpect."""
        try:
            import pexpect  # type: ignore
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
            prompt_timeout = max(30, min(effective_timeout, 180))
            stdout_chunks: list[str] = []

            prompts = [
                r"(?i)password:",
                r"(?i)passphrase",
                r"(?i)verification code",
                r"(?i)otp",
                pexpect.EOF,
                pexpect.TIMEOUT,
            ]

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

    def __exit__(self, *args):
        """Close master connection."""
        if self._master_started and not self._fallback_mode:
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
                    "-o",
                    f"ControlPath={self.control_path}",
                    "-o",
                    "ControlMaster=no",
                ]
                if needs_tty:
                    ssh_cmd.append("-tt")
                ssh_cmd.extend(
                    [
                        f"{self.config.user}@{self.config.host}",
                        remote_cmd,
                    ]
                )
            else:
                ssh_cmd = [
                    "ssh",
                    "-p",
                    str(self.config.port),
                    "-o",
                    "StrictHostKeyChecking=no",
                    "-o",
                    "UserKnownHostsFile=/dev/null",
                    "-o",
                    "LogLevel=ERROR",
                ]
                if self.config.uses_key_auth:
                    key_path = self.config.key_path
                    if not key_path:
                        raise RuntimeError(
                            "SSH key authentication requires key_path to be set"
                        )
                    ssh_cmd.extend(
                        [
                            "-i",
                            key_path,
                            "-o",
                            "IdentitiesOnly=yes",
                            "-o",
                            "BatchMode=yes",
                        ]
                    )
                else:
                    ssh_cmd.extend(
                        [
                            "-o",
                            "NumberOfPasswordPrompts=3",
                            "-o",
                            "PreferredAuthentications=password,keyboard-interactive",
                        ]
                    )
                if needs_tty:
                    ssh_cmd.append("-tt")
                ssh_cmd.extend(self.config.get_proxy_command_opts())
                ssh_cmd.extend(self.config.extra_opts)
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
                    f"stderr: {stderr}"
                )

            return stdout

    def write_file(self, content: str, remote_path: str):
        """Write content to remote file via heredoc."""
        if not content:
            raise ValueError("Cannot write empty content to file")

        safe_content = content.replace("'", "'\\''")
        cmd = (
            f"cat > {shlex.quote(remote_path)} <<'DAGSTER_EOF'\n"
            f"{safe_content}\n"
            f"DAGSTER_EOF"
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
            if self._master_started and not self._fallback_mode:
                scp_cmd = [
                    "scp",
                    "-o",
                    f"ControlPath={self.control_path}",
                    "-P",
                    str(self.config.port),
                ]
            else:
                scp_cmd = [
                    "scp",
                    "-P",
                    str(self.config.port),
                    "-o",
                    "StrictHostKeyChecking=no",
                    "-o",
                    "UserKnownHostsFile=/dev/null",
                    "-o",
                    "LogLevel=ERROR",
                ]
                if self.config.uses_key_auth:
                    key_path = self.config.key_path
                    if not key_path:
                        raise RuntimeError(
                            "SSH key authentication requires key_path to be set"
                        )
                    scp_cmd.extend(
                        [
                            "-i",
                            key_path,
                            "-o",
                            "IdentitiesOnly=yes",
                            "-o",
                            "BatchMode=yes",
                        ]
                    )
                else:
                    scp_cmd.extend(
                        [
                            "-o",
                            "PreferredAuthentications=password,keyboard-interactive",
                            "-o",
                            "NumberOfPasswordPrompts=3",
                        ]
                    )

            scp_cmd.extend(self.config.get_proxy_command_opts())
            scp_cmd.extend(self.config.extra_opts)
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
                stderr = proc.stderr
            else:
                if self.config.uses_password_auth or (
                    self.config.jump_host and self.config.jump_host.uses_password_auth
                ):
                    result = self._run_with_password(
                        scp_cmd, self._collect_passwords(), timeout=300
                    )
                    returncode = result.returncode
                    stderr = result.stderr
                else:
                    proc = subprocess.run(scp_cmd, capture_output=True, text=True)
                    returncode = proc.returncode
                    stderr = proc.stderr

        if returncode != 0:
            raise RuntimeError(
                f"SCP upload failed: {local_path} -> {remote_path}\nstderr: {stderr}"
            )
