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
        self.control_path = f"/tmp/dagster-ssh-{uuid.uuid4().hex}"
        self._master_started = False
        self.logger = get_dagster_logger()

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
                *base_opts,
                *self.config.extra_opts,
                f"{self.config.user}@{self.config.host}",
            ]

            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)  # type: ignore
            except FileNotFoundError as e:
                raise RuntimeError(
                    "SSH command not found. Please ensure OpenSSH client is installed."
                ) from e

            if result.returncode != 0:
                raise RuntimeError(f"Failed to start SSH master:\n{result.stderr}")
        else:
            # Password-based auth using SSH_ASKPASS
            cmd = [
                "ssh",
                "-M",
                "-N",
                "-f",
                "-p",
                str(self.config.port),
                "-o",
                "NumberOfPasswordPrompts=1",
                "-o",
                "PreferredAuthentications=password,keyboard-interactive",
                *base_opts,
                *self.config.extra_opts,
                f"{self.config.user}@{self.config.host}",
            ]

            try:
                # Use pexpect for interactive password prompt
                result = self._run_with_password(cmd, self.config.password)
            except FileNotFoundError as e:
                raise RuntimeError(
                    "SSH command not found. Please ensure OpenSSH client is installed."
                ) from e

            if result.returncode != 0:
                raise RuntimeError(
                    f"Failed to start SSH master (password auth):\n{result.stderr}\n"
                    f"Note: Ensure password authentication is enabled on the server."
                )

        self._master_started = True
        auth_method = "key" if self.config.uses_key_auth else "password"
        self.logger.debug(f"SSH ControlMaster started ({auth_method} auth)")

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

        try:
            child = pexpect.spawn(cmd_str, timeout=timeout, encoding="utf-8")

            # Wait for password prompt
            index = child.expect(
                [r"(?i)password:", r"(?i)passphrase", pexpect.EOF, pexpect.TIMEOUT],
                timeout=10,
            )

            if index == 0 or index == 1:
                # Send password
                child.sendline(password)
                child.expect(pexpect.EOF, timeout=timeout)
            elif index == 2:
                # EOF - command finished (might be success or failure)
                pass
            else:
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
        if not self._master_started:
            raise RuntimeError("SSH pool not started - use context manager")

        # Wrap in clean shell
        remote_cmd = f"bash --noprofile --norc -c {shlex.quote(cmd)}"

        ssh_cmd = [
            "ssh",
            "-o",
            f"ControlPath={self.control_path}",
            "-o",
            "ControlMaster=no",
            f"{self.config.user}@{self.config.host}",
            remote_cmd,
        ]

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
        if not self._master_started:
            raise RuntimeError("SSH pool not started")

        # Ensure remote directory exists
        remote_dir = str(Path(remote_path).parent)
        self.run(f"mkdir -p {shlex.quote(remote_dir)}")

        # Build SCP command
        scp_cmd = [
            "scp",
            "-o",
            f"ControlPath={self.control_path}",
            "-P",
            str(self.config.port),
            local_path,
            f"{self.config.user}@{self.config.host}:{remote_path}",
        ]

        result = subprocess.run(scp_cmd, capture_output=True, text=True)

        if result.returncode != 0:
            raise RuntimeError(
                f"SCP upload failed: {local_path} -> {remote_path}\n"
                f"stderr: {result.stderr}"
            )
