"""SSH connection configuration resource."""

import os
from typing import Optional, List
from dagster import ConfigurableResource
from pydantic import Field, model_validator


class SSHConnectionResource(ConfigurableResource):
    """
    SSH connection settings.

    Supports two authentication methods:
    1. SSH key (recommended for automation)
    2. Password (for interactive use or when keys unavailable)

    Either key_path OR password must be provided (not both).

    Examples:
        # Key-based auth
        ssh = SSHConnectionResource(
            host="cluster.example.com",
            user="username",
            key_path="~/.ssh/id_rsa",
        )

        # Password-based auth
        ssh = SSHConnectionResource(
            host="cluster.example.com",
            user="username",
            password="secret123",
        )

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
            self.key_path = os.path.expanduser(self.key_path)
            if not os.path.exists(self.key_path):
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
        """
        Create from environment variables.

        Environment variables:
            {prefix}_HOST - SSH hostname (required)
            {prefix}_PORT - SSH port (optional, default: 22)
            {prefix}_USER - SSH username (required)
            {prefix}_KEY - Path to SSH key (optional)
            {prefix}_PASSWORD - SSH password (optional)
            {prefix}_OPTS_EXTRA - Additional SSH options (optional)

        Either KEY or PASSWORD must be set (not both).

        Args:
            prefix: Environment variable prefix (default: "SLURM_SSH")

        Returns:
            SSHConnectionResource instance

        Raises:
            ValueError: If required variables missing or both auth methods specified

        Example:
            export SLURM_SSH_HOST=cluster.example.com
            export SLURM_SSH_USER=username
            export SLURM_SSH_KEY=~/.ssh/id_rsa
            # OR
            export SLURM_SSH_PASSWORD=secret123
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
        """
        Build base SSH command for subprocess.

        Returns:
            List of command arguments for subprocess.run()

        Example:
            ['ssh', '-p', '22', '-i', '/path/to/key', 'user@host']
            # OR
            ['sshpass', '-p', 'password', 'ssh', '-p', '22', 'user@host']
        """
        base_opts = [
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "BatchMode=yes" if self.uses_key_auth else "no",
            "-o",
            "LogLevel=ERROR",
            "-o",
            "ServerAliveInterval=30",
            "-o",
            "ServerAliveCountMax=6",
        ]

        if self.uses_key_auth:
            # Key-based authentication
            return [
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
                *base_opts,
                *self.extra_opts,
                f"{self.user}@{self.host}",
            ]
        else:
            # Password-based authentication (requires sshpass)
            return [
                "sshpass",
                "-p",
                self.password,
                "ssh",
                "-p",
                str(self.port),
                "-o",
                "PreferredAuthentications=password",
                "-o",
                "PubkeyAuthentication=no",
                *base_opts,
                *self.extra_opts,
                f"{self.user}@{self.host}",
            ]

    def get_scp_base_command(self) -> List[str]:
        """
        Build base SCP command for file transfers.

        Returns:
            List of command arguments (without source/dest)

        Example:
            ['scp', '-P', '22', '-i', '/path/to/key']
            # OR
            ['sshpass', '-p', 'password', 'scp', '-P', '22']
        """
        base_opts = [
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "BatchMode=yes" if self.uses_key_auth else "no",
            "-o",
            "LogLevel=ERROR",
        ]

        if self.uses_key_auth:
            return [
                "scp",
                "-P",
                str(self.port),
                "-i",
                self.key_path,
                "-o",
                "IdentitiesOnly=yes",
                *base_opts,
                *self.extra_opts,
            ]
        else:
            return [
                "sshpass",
                "-p",
                self.password,
                "scp",
                "-P",
                str(self.port),
                *base_opts,
                *self.extra_opts,
            ]
