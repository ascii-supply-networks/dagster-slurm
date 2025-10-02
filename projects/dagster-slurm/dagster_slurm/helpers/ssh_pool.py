"""SSH connection pooling via ControlMaster."""

import uuid
import subprocess
import shlex
import time
from typing import Optional
from pathlib import Path
from dagster import get_dagster_logger
from ..resources.ssh import SSHConnectionResource


class SSHConnectionPool:
    """
    Reuse SSH connections via ControlMaster.
    Supports both key-based and password-based authentication.
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
                "StrictHostKeyChecking=no",
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-o",
                "BatchMode=yes",
                "-o",
                "LogLevel=ERROR",
                *base_opts,
                *self.config.extra_opts,
                f"{self.config.user}@{self.config.host}",
            ]
        else:
            # Password-based auth
            cmd = [
                "sshpass",
                "-p",
                self.config.password,
                "ssh",
                "-M",
                "-N",
                "-f",
                "-p",
                str(self.config.port),
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-o",
                "LogLevel=ERROR",
                *base_opts,
                *self.config.extra_opts,
                f"{self.config.user}@{self.config.host}",
            ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            error_msg = f"Failed to start SSH master:\n{result.stderr}"
            if self.config.uses_password_auth and "sshpass: not found" in result.stderr:
                error_msg += (
                    "\n\nPassword authentication requires 'sshpass' to be installed.\n"
                )
                error_msg += "Install it with: apt-get install sshpass  # or  brew install sshpass"
            raise RuntimeError(error_msg)

        self._master_started = True
        auth_method = "key" if self.config.uses_key_auth else "password"
        self.logger.debug(f"SSH ControlMaster started ({auth_method} auth)")

        return self

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
        """
        Run command using pooled connection.

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
        safe_content = content.replace("'", "'\\''")
        cmd = (
            f"cat > {shlex.quote(remote_path)} <<'DAGSTER_EOF'\n"
            f"{safe_content}\n"
            f"DAGSTER_EOF"
        )
        self.run(cmd)

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
