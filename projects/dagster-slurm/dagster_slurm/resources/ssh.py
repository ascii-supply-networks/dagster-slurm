"""SSH connection configuration resource."""

import os
from typing import List, Optional

from dagster import ConfigurableResource
from pydantic import Field, model_validator


class SSHConnectionResource(ConfigurableResource):
    """SSH connection settings.

    Supports two authentication methods:
    1. SSH key (recommended for automation)
    2. Password (for interactive use or when keys unavailable)

    Either key_path OR password must be provided (not both).

    Examples:
        .. code-block:: python

            # Key-based auth
            ssh = SSHConnectionResource(
                host="cluster.example.com",
                user="username",
                key_path="~/.ssh/id_rsa",
            )

        .. code-block:: python

            # Password-based auth
            ssh = SSHConnectionResource(
                host="cluster.example.com",
                user="username",
                password="secret123",
            )

        .. code-block:: python

        # From environment variables
        ssh = SSHConnectionResource.from_env()

    """

    host: str = Field(description="SSH hostname or IP address")
    port: int = Field(default=22, description="SSH port")
    user: str = Field(description="SSH username")

    # Authentication (XOR - exactly one must be provided)
    key_path: Optional[str] = Field(
        default=None, description="Path to SSH private key (for key-based auth)"
    )
    password: Optional[str] = Field(
        default=None, description="SSH password (for password-based auth)"
    )

    # Optional settings
    extra_opts: List[str] = Field(
        default_factory=list,
        description="Additional SSH options (e.g., ['-o', 'Compression=yes'])",
    )

    @model_validator(mode="after")
    def validate_auth_method(self):
        """Ensure exactly one authentication method is provided."""
        has_key = self.key_path is not None
        has_password = self.password is not None

        if not has_key and not has_password:
            raise ValueError(
                "Either 'key_path' or 'password' must be provided for SSH authentication"
            )

        if has_key and has_password:
            raise ValueError(
                "Cannot specify both 'key_path' and 'password'. Choose one authentication method."
            )

        # Expand key path if provided
        if has_key:
            self.key_path = os.path.expanduser(self.key_path)  # type: ignore
            if not os.path.exists(self.key_path):  # type: ignore
                raise ValueError(f"SSH key not found: {self.key_path}")

        return self

    @property
    def uses_key_auth(self) -> bool:
        """Returns True if using key-based authentication."""
        return self.key_path is not None

    @property
    def uses_password_auth(self) -> bool:
        """Returns True if using password-based authentication."""
        return self.password is not None

    @classmethod
    def from_env(cls, prefix: str = "SLURM_SSH") -> "SSHConnectionResource":
        """Create from environment variables.

        Environment variables:
            ``{prefix}``_HOST - SSH hostname (required)
            ``{prefix}``_PORT - SSH port (optional, default: 22)
            ``{prefix}``_USER - SSH username (required)
            ``{prefix}``_KEY - Path to SSH key (optional)
            ``{prefix}``_PASSWORD - SSH password (optional)
            ``{prefix}``_OPTS_EXTRA - Additional SSH options (optional)

        Either KEY or PASSWORD must be set (not both).

        Args:
            prefix: Environment variable prefix (default: "SLURM_SSH")

        Returns:
            SSHConnectionResource instance

        Raises:
            ValueError: If required variables missing or both auth methods specified

        Example:

        .. code-block:: bash

            export SLURM_SSH_HOST=cluster.example.com
            export SLURM_SSH_USER=username
            export SLURM_SSH_KEY=~/.ssh/id_rsa
            # OR
            export SLURM_SSH_PASSWORD=secret123

        ...

        """
        import shlex

        host = os.getenv(f"{prefix}_HOST")
        if not host:
            raise ValueError(f"{prefix}_HOST environment variable is required")

        user = os.getenv(f"{prefix}_USER")
        if not user:
            raise ValueError(f"{prefix}_USER environment variable is required")

        port = int(os.getenv(f"{prefix}_PORT", "22"))
        key_path = os.getenv(f"{prefix}_KEY")
        password = os.getenv(f"{prefix}_PASSWORD")
        extra_opts = shlex.split(os.getenv(f"{prefix}_OPTS_EXTRA", ""))

        return cls(
            host=host,
            port=port,
            user=user,
            key_path=key_path,
            password=password,
            extra_opts=extra_opts,
        )

    def get_ssh_base_command(self) -> List[str]:
        """Build base SSH command for subprocess.

        Returns:
            List of command arguments for subprocess.run()

        Note:
            For password authentication, this returns the base command.
            Password handling is done separately via pexpect in SSHConnectionPool.

            This method is used by SSHMessageReader which uses ControlMaster,
            so password auth is already handled by the ControlMaster connection.

        Example:
            ['ssh', '-p', '22', '-i', '/path/to/key', 'user@host']
            # OR (password auth via ControlMaster)
            ['ssh', '-p', '22', '-o', 'ControlPath=/tmp/...', 'user@host']

        """
        base_opts = [
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "LogLevel=ERROR",
            "-o",
            "ServerAliveInterval=30",
            "-o",
            "ServerAliveCountMax=6",
        ]

        if self.uses_key_auth:
            # Key-based authentication
            return [  # type: ignore
                "ssh",
                "-p",
                str(self.port),
                "-i",
                self.key_path,
                "-o",
                "IdentitiesOnly=yes",
                "-o",
                "PreferredAuthentications=publickey",
                "-o",
                "PasswordAuthentication=no",
                "-o",
                "BatchMode=yes",
                *base_opts,
                *self.extra_opts,
                f"{self.user}@{self.host}",
            ]
        else:
            # Password-based authentication
            # NOTE: When used with SSHMessageReader, it will add ControlPath
            # to use the existing ControlMaster connection (password already handled)
            return [
                "ssh",
                "-p",
                str(self.port),
                "-o",
                "PreferredAuthentications=password,keyboard-interactive",
                "-o",
                "PubkeyAuthentication=no",
                "-o",
                "NumberOfPasswordPrompts=1",
                *base_opts,
                *self.extra_opts,
                f"{self.user}@{self.host}",
            ]

    def get_scp_base_command(self) -> List[str]:
        """Build base SCP command for file transfers.

        Returns:
            List of command arguments (without source/dest)

        Note:
            For password authentication, password handling is done via pexpect.

        Example:
            ['scp', '-P', '22', '-i', '/path/to/key']
            # OR (password auth)
            ['scp', '-P', '22']

        """
        base_opts = [
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "LogLevel=ERROR",
        ]

        if self.uses_key_auth:
            return [  # type: ignore
                "scp",
                "-P",
                str(self.port),
                "-i",
                self.key_path,
                "-o",
                "IdentitiesOnly=yes",
                "-o",
                "BatchMode=yes",
                *base_opts,
                *self.extra_opts,
            ]
        else:
            # Password-based authentication (handled by pexpect)
            return [
                "scp",
                "-P",
                str(self.port),
                "-o",
                "PreferredAuthentications=password",
                "-o",
                "PubkeyAuthentication=no",
                *base_opts,
                *self.extra_opts,
            ]

    def get_remote_target(self) -> str:
        """Get the remote target string for SCP commands.

        Returns:
            String in format 'user@host'

        Example:
            'username@cluster.example.com'

        """
        return f"{self.user}@{self.host}"
