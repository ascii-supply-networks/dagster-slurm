"""SSH connection configuration resource."""

import os
import shlex
from typing import List, Optional
from pathlib import Path

from dagster import ConfigurableResource
from pydantic import Field, model_validator, field_validator


class SSHConnectionResource(ConfigurableResource):
    """SSH connection settings.

    This resource configures a connection to a remote host via SSH. It supports
    key-based or password-based authentication, pseudo-terminal allocation (`-t`),
    and connections through a proxy jump host.

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

            # With a proxy jump host
            jump_box = SSHConnectionResource(
                host="jump.example.com", user="jumpuser", key_path="~/.ssh/jump_key"
            )
            ssh_via_jump = SSHConnectionResource(
                host="private-cluster",
                user="user_on_cluster",
                key_path="~/.ssh/cluster_key",
                jump_host=jump_box
            )

            # With a post-login command (e.g., for VSC)
            vsc_ssh = SSHConnectionResource(
                host="vmos.vsc.ac.at",
                user="dagster01",
                key_path="~/.ssh/vsc_key",
                force_tty=True,
                post_login_command="vsc5"
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

    # Optional advanced settings
    force_tty: bool = Field(
        default=False,
        description="Allocate a pseudo-terminal (-t flag) for remote commands. "
        "Useful for commands that require an interactive terminal.",
    )
    post_login_command: Optional[str] = Field(
        default=None,
        description="A command to be executed immediately after login, before the main command. "
        "Example: 'vsc5' or 'sudo -u otheruser'.",
    )
    jump_host: Optional["SSHConnectionResource"] = Field(
        default=None,
        description="An optional SSH connection to use as a proxy jump host (-J equivalent). "
        "The jump host must use key-based authentication.",
    )
    extra_opts: List[str] = Field(
        default_factory=list,
        description="Additional raw SSH options (e.g., ['-o', 'Compression=yes'])",
    )

    @field_validator("key_path")
    @classmethod
    def _expand_and_validate_key_path(cls, v: Optional[str]) -> Optional[str]:
        """Expands user directory and checks for existence."""
        if v is None:
            return None
        expanded_path = Path(os.path.expanduser(v))
        if not expanded_path.exists():
            raise ValueError(f"SSH key not found at path: {expanded_path}")
        return str(expanded_path)

    @model_validator(mode="after")
    def _validate_config(self):
        """Ensure exactly one authentication method is provided and validate jump host."""
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

        if self.jump_host:
            if not self.jump_host.uses_key_auth:
                raise ValueError("Proxy jump host must use key-based authentication.")
            if self.jump_host.jump_host:
                raise ValueError("Multi-level proxy jumps are not supported.")
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
    def from_env(
        cls, prefix: str = "SLURM_SSH", _is_jump: bool = False
    ) -> "SSHConnectionResource":
        """Create from environment variables.

        This method reads connection details from environment variables. The variable
        names are constructed using the provided ``prefix``.

        With the default prefix, the following variables are used:

        - ``SLURM_SSH_HOST`` - SSH hostname (required)
        - ``SLURM_SSH_PORT`` - SSH port (optional, default: 22)
        - ``SLURM_SSH_USER`` - SSH username (required)
        - ``SLURM_SSH_KEY`` - Path to SSH key (optional)
        - ``SLURM_SSH_PASSWORD`` - SSH password (optional)
        - ``SLURM_SSH_FORCE_TTY`` - Set to 'true' or '1' to enable tty allocation (optional)
        - ``SLURM_SSH_POST_LOGIN_COMMAND`` - Post-login command string (optional)
        - ``SLURM_SSH_OPTS_EXTRA`` - Additional SSH options (optional)

        For proxy jumps, use the ``_JUMP`` suffix for jump host variables (e.g.,
        ``SLURM_SSH_JUMP_HOST``, ``SLURM_SSH_JUMP_USER``, etc.).

        Args:
            prefix: Environment variable prefix (default: "SLURM_SSH")

        Returns:
            SSHConnectionResource instance
        """
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
        force_tty = os.getenv(f"{prefix}_FORCE_TTY", "false").lower() in (
            "true",
            "1",
            "yes",
        )
        post_login_command = os.getenv(f"{prefix}_POST_LOGIN_COMMAND")

        jump_host = None
        # Only look for a jump host at the top level to prevent recursion
        if not _is_jump and os.getenv(f"{prefix}_JUMP_HOST"):
            jump_prefix = f"{prefix}_JUMP"
            jump_host = cls.from_env(prefix=jump_prefix, _is_jump=True)

        return cls(
            host=host,
            port=port,
            user=user,
            key_path=key_path,
            password=password,
            extra_opts=extra_opts,
            force_tty=force_tty,
            post_login_command=post_login_command,
            jump_host=jump_host,
        )

    def get_proxy_command_opts(self) -> List[str]:
        """Builds SSH options for ProxyCommand if a jump_host is configured."""
        if not self.jump_host:
            return []

        # Validator ensures jump host uses key auth, so key_path is not None.
        # We assert this for the type checker.
        assert self.jump_host.key_path is not None

        # Build the inner ssh command for the proxy. Jump host must use key auth.
        proxy_ssh_cmd = [
            "ssh",
            "-p",
            str(self.jump_host.port),
            "-i",
            self.jump_host.key_path,
        ]

        # Add standard options for a non-interactive proxy connection
        proxy_ssh_cmd.extend(
            [
                "-o",
                "IdentitiesOnly=yes",
                "-o",
                "BatchMode=yes",
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-o",
                "LogLevel=ERROR",
                *self.jump_host.extra_opts,
                f"{self.jump_host.user}@{self.jump_host.host}",
                "-W",
                "%h:%p",  # Connect to the final destination
            ]
        )

        proxy_command_str = " ".join(shlex.quote(arg) for arg in proxy_ssh_cmd)
        return ["-o", f"ProxyCommand={proxy_command_str}"]

    def get_ssh_base_command(self) -> List[str]:
        """Build base SSH command, including proxy and auth options."""
        proxy_opts = self.get_proxy_command_opts()
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
            # Assert for type checker, guaranteed by uses_key_auth property
            assert self.key_path is not None
            auth_opts = [
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
            ]
        else:  # Password-based authentication
            auth_opts = [
                "-o",
                "PreferredAuthentications=password,keyboard-interactive",
                "-o",
                "PubkeyAuthentication=no",
                "-o",
                "NumberOfPasswordPrompts=1",
            ]

        return [
            "ssh",
            *proxy_opts,
            "-p",
            str(self.port),
            *auth_opts,
            *base_opts,
            *self.extra_opts,
            f"{self.user}@{self.host}",
        ]

    def get_scp_base_command(self) -> List[str]:
        """Build base SCP command, including proxy and auth options."""
        proxy_opts = self.get_proxy_command_opts()
        base_opts = [
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "LogLevel=ERROR",
        ]

        if self.uses_key_auth:
            # Assert for type checker, guaranteed by uses_key_auth property
            assert self.key_path is not None
            auth_opts = [
                "-i",
                self.key_path,
                "-o",
                "IdentitiesOnly=yes",
                "-o",
                "BatchMode=yes",
            ]
        else:  # Password-based authentication
            auth_opts = [
                "-o",
                "PreferredAuthentications=password",
                "-o",
                "PubkeyAuthentication=no",
            ]

        return [
            "scp",
            *proxy_opts,
            "-P",
            str(self.port),
            *auth_opts,
            *base_opts,
            *self.extra_opts,
        ]

    def get_remote_target(self) -> str:
        """Get the remote target string for SCP commands."""
        return f"{self.user}@{self.host}"


# This is necessary for Pydantic to resolve the forward reference of "SSHConnectionResource"
# within its own definition (for the `jump_host` field).
SSHConnectionResource.model_rebuild()
