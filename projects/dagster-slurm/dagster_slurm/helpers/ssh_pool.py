"""SSH connection pooling via ControlMaster."""

import shlex
import subprocess
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

    def __init__(self, ssh_config: "SSHConnectionResource"):
        self.config = ssh_config
        self.control_path: Optional[str] = f"/tmp/dagster-ssh-{uuid.uuid4().hex}"
        self._master_started = False
        self._fallback_mode = False
        self._fallback_reason: Optional[str] = None
        self.logger = get_dagster_logger()

    def _collect_passwords(self) -> list[str]:
        passwords: list[str] = []
        if self.config.jump_host and self.config.jump_host.password:
            passwords.append(self.config.jump_host.password)
        if self.config.password:
            passwords.append(self.config.password)
        return passwords

    def __enter__(self):
        """Start SSH ControlMaster."""
        self.logger.debug("Starting SSH ControlMaster...")

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
            if self.config.uses_key_auth:
                # Key-based auth
                cmd = [
                    "ssh",
                    "-M",
                    "-N",
                    "-f",
                    "-p",
                    str(self.config.port),
                    "-i",
                    self.config.key_path,
                    "-o",
                    "IdentitiesOnly=yes",
                    "-o",
                    "BatchMode=yes",
                ]
                if self.config.force_tty:
                    cmd.append("-tt")
                cmd.extend(base_opts)
                cmd.extend(self.config.get_proxy_command_opts())
                cmd.extend(self.config.extra_opts)
                cmd.append(f"{self.config.user}@{self.config.host}")

                try:
                    result = subprocess.run(  # type: ignore
                        cmd, capture_output=True, text=True, timeout=30
                    )
                except FileNotFoundError as e:
                    raise RuntimeError(
                        "SSH command not found. Please ensure OpenSSH client is installed."
                    ) from e

                if result.returncode != 0:
                    raise RuntimeError(
                        f"Failed to start SSH master (key auth): {result.stderr.strip()}"
                    )
            else:
                # Password-based auth using SSH_ASKPASS
                cmd = [
                    "ssh",
                    "-M",
                    "-N",
                    "-f",
                    "-p",
                    str(self.config.port),
                ]
                if self.config.force_tty:
                    cmd.append("-tt")
                cmd.extend(
                    [
                        "-o",
                        "NumberOfPasswordPrompts=1",
                        "-o",
                        "PreferredAuthentications=password,keyboard-interactive",
                    ]
                )
                cmd.extend(base_opts)
                cmd.extend(self.config.get_proxy_command_opts())
                cmd.extend(self.config.extra_opts)
                cmd.append(f"{self.config.user}@{self.config.host}")

                passwords = self._collect_passwords()
                if not passwords:
                    raise RuntimeError(
                        "Password authentication requested but no password provided."
                    )
                try:
                    # Use pexpect for interactive password prompt(s)
                    result = self._run_with_password(cmd, passwords)
                except FileNotFoundError as e:
                    raise RuntimeError(
                        "SSH command not found. Please ensure OpenSSH client is installed."
                    ) from e

                if result.returncode != 0:
                    raise RuntimeError(
                        "Failed to start SSH master (password auth). "
                        "Note: Ensure password authentication is enabled on the server."
                    )

            self._master_started = True
            auth_method = "key" if self.config.uses_key_auth else "password"
            self.logger.debug(f"SSH ControlMaster started ({auth_method} auth)")
        except RuntimeError as exc:
            if self.config.uses_key_auth:
                # Fall back to direct SSH connections
                self._fallback_mode = True
                self._fallback_reason = str(exc)
                self.control_path = None
                self.logger.warning(
                    "SSH ControlMaster unavailable (%s). Falling back to direct SSH "
                    "connections; performance may be reduced but functionality remains.",
                    exc,
                )
            else:
                raise

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
            passwords = list(password)
        else:
            passwords = [password]
        if not passwords:
            raise RuntimeError("No password provided for SSH authentication.")

        pw_index = 0
        last_password = passwords[-1]

        try:
            child = pexpect.spawn(cmd_str, timeout=timeout, encoding="utf-8")

            while True:
                index = child.expect(
                    [r"(?i)password:", r"(?i)passphrase", pexpect.EOF, pexpect.TIMEOUT],
                    timeout=10,
                )

                if index in (0, 1):
                    if pw_index < len(passwords):
                        pw_value = passwords[pw_index]
                        pw_index += 1
                    else:
                        pw_value = last_password
                    child.sendline(pw_value)
                    continue
                if index == 2:
                    # EOF - command finished (might be success or failure)
                    break

                # Timeout
                child.close(force=True)
                raise TimeoutError("Timeout waiting for password prompt")

            child.close()

            # Create a result object similar to subprocess.run
            class Result:
                def __init__(self, returncode, stdout, stderr):
                    self.returncode = returncode
                    self.stdout = stdout
                    self.stderr = stderr

            return Result(
                returncode=child.exitstatus or 0, stdout=child.before or "", stderr=""
            )

        except pexpect.exceptions.ExceptionPexpect as e:
            raise RuntimeError(f"Password authentication failed: {e}")

    def __exit__(self, *args):
        """Close master connection."""
        if self._master_started:
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

        if self._master_started:
            ssh_cmd = [
                "ssh",
                "-o",
                f"ControlPath={self.control_path}",
                "-o",
                "ControlMaster=no",
            ]
            if self.config.force_tty:
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
                        "NumberOfPasswordPrompts=1",
                        "-o",
                        "PreferredAuthentications=password,keyboard-interactive",
                    ]
                )
            if self.config.force_tty:
                ssh_cmd.append("-tt")
            ssh_cmd.extend(self.config.get_proxy_command_opts())
            ssh_cmd.extend(self.config.extra_opts)
            ssh_cmd.extend(
                [
                    f"{self.config.user}@{self.config.host}",
                    remote_cmd,
                ]
            )

        result = subprocess.run(
            ssh_cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"SSH command failed (exit {result.returncode}): {cmd}\n"
                f"stdout: {result.stdout}\n"
                f"stderr: {result.stderr}"
            )

        return result.stdout

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
        if self._master_started:
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
                        "NumberOfPasswordPrompts=1",
                        "-o",
                        "PreferredAuthentications=password,keyboard-interactive",
                    ]
                )

        scp_cmd.extend(self.config.extra_opts)
        scp_cmd.extend(
            [
                local_path,
                f"{self.config.user}@{self.config.host}:{remote_path}",
            ]
        )

        result = subprocess.run(scp_cmd, capture_output=True, text=True)

        if result.returncode != 0:
            raise RuntimeError(
                f"SCP upload failed: {local_path} -> {remote_path}\n"
                f"stderr: {result.stderr}"
            )
