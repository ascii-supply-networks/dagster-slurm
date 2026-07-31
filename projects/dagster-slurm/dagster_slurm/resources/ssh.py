"""SSH connection configuration resource."""

import os
import shlex
from pathlib import Path
from typing import Literal

from dagster import ConfigurableResource
from pydantic import Field, field_validator, model_validator

HostKeyChecking = Literal["off", "accept-new", "strict"]


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
                host="jump.example.com", user="jumpuser", password="jump_password"
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
    key_path: str | None = Field(
        default=None, description="Path to SSH private key (for key-based auth)"
    )
    password: str | None = Field(
        default=None, description="SSH password (for password-based auth)"
    )

    # Optional advanced settings
    force_tty: bool = Field(
        default=False,
        description="Allocate a pseudo-terminal (-t flag) for remote commands. "
        "Useful for commands that require an interactive terminal.",
    )
    post_login_command: str | None = Field(
        default=None,
        description="A command to be executed immediately after login, before the main command. "
        "Example: 'vsc5' or 'sudo -u otheruser'.",
    )
    jump_host: "SSHConnectionResource | None" = Field(
        default=None,
        description="An optional SSH connection to use as a proxy jump host (-J equivalent). "
        "The jump host may use key- or password-based authentication.",
    )
    extra_opts: list[str] = Field(
        default_factory=list,
        description="Additional raw SSH options (e.g., ['-o', 'Compression=yes'])",
    )
    host_key_checking: HostKeyChecking = Field(
        default="strict",
        description="SSH host-key verification policy.",
    )
    known_hosts_file: str | None = Field(
        default=None,
        description=(
            "Known-hosts file. Defaults to /dev/null when verification is off "
            "and ~/.ssh/known_hosts otherwise."
        ),
    )

    @field_validator("key_path")
    @classmethod
    def _expand_and_validate_key_path(cls, v: str | None) -> str | None:
        """Expands user directory and checks for existence."""
        if v is None:
            return None
        expanded_path = Path(v).expanduser()
        if not expanded_path.exists():
            raise ValueError(f"SSH key not found at path: {expanded_path}")
        return str(expanded_path)

    @field_validator("known_hosts_file")
    @classmethod
    def _expand_known_hosts_file(cls, v: str | None) -> str | None:
        if v is None:
            return None
        if not v.strip():
            raise ValueError("known_hosts_file cannot be empty")
        return str(Path(v).expanduser())

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

        if self.jump_host and self.jump_host.jump_host:
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

    @property
    def requires_tty(self) -> bool:
        """Return True when the resource explicitly requires a TTY."""
        if self.force_tty:
            return True
        if self.jump_host and self.jump_host.requires_tty:
            return True
        return False

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
        - ``SLURM_SSH_HOST_KEY_CHECKING`` - off, accept-new, or strict (optional)
        - ``SLURM_SSH_KNOWN_HOSTS_FILE`` - Known-hosts file path (optional)

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
        host_key_checking = (
            os.getenv(f"{prefix}_HOST_KEY_CHECKING", "strict").strip().lower()
        )
        known_hosts_file = os.getenv(f"{prefix}_KNOWN_HOSTS_FILE")

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
            host_key_checking=host_key_checking,
            known_hosts_file=known_hosts_file,
            jump_host=jump_host,
        )

    def get_proxy_command_opts(self) -> list[str]:
        """Build a ProxyCommand whose SSH settings apply to the jump host."""
        if not self.jump_host:
            return []

        jump = self.jump_host
        command = [
            "ssh",
            *jump.get_common_ssh_opts(),
            "-p",
            str(jump.port),
            *jump.get_key_auth_opts(batch_mode=True),
        ]
        if jump.uses_password_auth:
            command.extend(
                [
                    "-o",
                    "PreferredAuthentications=password,keyboard-interactive",
                    "-o",
                    "NumberOfPasswordPrompts=3",
                ]
            )
        command.extend(
            [
                "-W",
                "%h:%p",
                f"{jump.user}@{jump.host}",
            ]
        )
        return ["-o", f"ProxyCommand={shlex.join(command)}"]

    def get_key_auth_opts(self, *, batch_mode: bool = True) -> list[str]:
        """Build key-based SSH authentication options."""
        if not self.uses_key_auth:
            return []

        key_path = self.key_path
        if not key_path:
            raise RuntimeError("SSH key authentication requires key_path to be set")

        auth_opts = [
            "-i",
            key_path,
            "-o",
            "IdentitiesOnly=yes",
        ]
        if batch_mode:
            auth_opts.extend(["-o", "BatchMode=yes"])
        return auth_opts

    def get_host_key_opts(self) -> list[str]:
        """Build host-key verification options."""
        checking = {
            "off": "no",
            "accept-new": "accept-new",
            "strict": "yes",
        }[self.host_key_checking]
        return [
            "-o",
            f"StrictHostKeyChecking={checking}",
            "-o",
            f"UserKnownHostsFile={self._resolved_known_hosts_file()}",
        ]

    def _resolved_known_hosts_file(self) -> str:
        if self.known_hosts_file is not None:
            return self.known_hosts_file
        if self.host_key_checking == "off":
            return "/dev/null"
        return str(Path.home() / ".ssh" / "known_hosts")

    def get_common_ssh_opts(
        self,
        *,
        server_alive_interval: int | None = None,
        server_alive_count_max: int | None = None,
    ) -> list[str]:
        """Build shared SSH/SCP options with caller overrides first."""
        opts = [
            *self.extra_opts,
            *self.get_host_key_opts(),
            "-o",
            "LogLevel=ERROR",
        ]
        if server_alive_interval is not None:
            opts.extend(["-o", f"ServerAliveInterval={server_alive_interval}"])
        if server_alive_count_max is not None:
            opts.extend(["-o", f"ServerAliveCountMax={server_alive_count_max}"])
        return opts

    def host_key_error_hint(self, stderr: str) -> str:
        """Return an actionable hint for an OpenSSH host-key failure."""
        if "host key verification failed" not in stderr.lower():
            return ""
        known_hosts_file = self._resolved_known_hosts_file()
        if self.host_key_checking == "accept-new":
            action = (
                "Verify the remote key, then update its existing entry in "
                f"{known_hosts_file}."
            )
        elif self.host_key_checking == "strict":
            action = (
                f"Add the verified key to {known_hosts_file}, or set "
                "host_key_checking='accept-new' for trust on first use."
            )
        else:
            action = "Review host-key options supplied through extra_opts."
        return f"\n{action}"

    def get_ssh_base_command(self) -> list[str]:
        """Build base SSH command, including proxy and auth options."""
        proxy_opts = self.get_proxy_command_opts()
        base_opts = self.get_common_ssh_opts(
            server_alive_interval=30,
            server_alive_count_max=6,
        )

        if self.uses_key_auth:
            auth_opts = self.get_key_auth_opts(batch_mode=True)
            auth_opts.extend(
                [
                    "-o",
                    "PreferredAuthentications=publickey",
                    "-o",
                    "PasswordAuthentication=no",
                ]
            )
        else:  # Password-based authentication
            auth_opts = [
                "-o",
                "PreferredAuthentications=password,keyboard-interactive",
                "-o",
                "PubkeyAuthentication=no",
                "-o",
                "NumberOfPasswordPrompts=3",
            ]

        return [
            "ssh",
            *base_opts,
            *proxy_opts,
            "-p",
            str(self.port),
            *auth_opts,
            f"{self.user}@{self.host}",
        ]

    def get_scp_base_command(self) -> list[str]:
        """Build base SCP command, including proxy and auth options."""
        proxy_opts = self.get_proxy_command_opts()
        base_opts = self.get_common_ssh_opts()

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
            *base_opts,
            *proxy_opts,
            "-P",
            str(self.port),
            *auth_opts,
        ]

    def get_remote_target(self) -> str:
        """Get the remote target string for SCP commands."""
        return f"{self.user}@{self.host}"


# This is necessary for Pydantic to resolve the forward reference of "SSHConnectionResource"
# within its own definition (for the `jump_host` field).
SSHConnectionResource.model_rebuild()
