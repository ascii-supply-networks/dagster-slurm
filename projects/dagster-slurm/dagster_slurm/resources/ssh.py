"""SSH connection configuration resource."""

import os
import shlex
from pathlib import Path
from typing import Literal

from dagster import ConfigurableResource
from pydantic import Field, field_validator, model_validator

HostKeyChecking = Literal["off", "accept-new", "strict", "inherit"]


def _blank_as_none(value: str | None) -> str | None:
    """Treat a blank environment value as unset.

    Writing ``FOO=`` in a dotenv file is the natural way to say "not set", so
    it must not be handed to the model as an empty string.
    """
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


class SSHConnectionResource(ConfigurableResource):
    """SSH connection settings.

    This resource configures a connection to a remote host via SSH. It supports
    key-based or password-based authentication, pseudo-terminal allocation (`-t`),
    and connections through a proxy jump host.

    Supports three authentication modes:
    1. SSH key (recommended for automation) - set ``key_path``
    2. Password (when keys are unavailable) - set ``password``
    3. Inherited - set neither, and ``ssh-agent`` plus ``~/.ssh/config`` decide

    ``key_path`` and ``password`` are mutually exclusive.

    Note that every option this resource emits overrides ``~/.ssh/config``,
    because OpenSSH prefers command-line options. That keeps a run reproducible
    across machines; see ``host_key_checking='inherit'`` and ``batch_mode=None``
    to hand individual settings back to the operator's own configuration.

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
        description=(
            "SSH host-key verification policy. Use 'inherit' to emit no host-key "
            "options at all so ~/.ssh/config decides."
        ),
    )
    known_hosts_file: str | None = Field(
        default=None,
        description=(
            "Known-hosts file. Defaults to /dev/null when verification is off "
            "and ~/.ssh/known_hosts otherwise. Left to ~/.ssh/config when "
            "host_key_checking is 'inherit' and this is unset."
        ),
    )
    batch_mode: bool | None = Field(
        default=True,
        description=(
            "Whether to pass BatchMode to OpenSSH. True (the default) refuses "
            "interactive prompts, which is what a non-interactive Dagster "
            "daemon needs. None emits no BatchMode option so ~/.ssh/config "
            "decides - use it when a key passphrase must be typed."
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
        """Ensure at most one authentication method is set and validate jump host."""
        if self.key_path is not None and self.password is not None:
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
    def uses_inherited_auth(self) -> bool:
        """Returns True when authentication is left entirely to OpenSSH.

        Neither a key nor a password was configured, so ``~/.ssh/config``,
        ``ssh-agent`` and the default identity files decide.
        """
        return self.key_path is None and self.password is None

    @property
    def defers_to_ssh_config(self) -> bool:
        """Whether this connection adds nothing OpenSSH cannot work out itself.

        Used to hand a jump host back to ``-J`` instead of rebuilding it as an
        explicit ProxyCommand, which would bypass ``~/.ssh/config`` entirely.
        """
        return (
            self.uses_inherited_auth
            and self.host_key_checking == "inherit"
            and self.known_hosts_file is None
            and self.batch_mode is None
            and not self.extra_opts
            and not self.force_tty
            and self.post_login_command is None
        )

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

        # Every optional variable treats a blank value as unset, so `FOO=` in a
        # dotenv file means "use the default" instead of failing validation.
        port = int(_blank_as_none(os.getenv(f"{prefix}_PORT")) or "22")
        key_path = _blank_as_none(os.getenv(f"{prefix}_KEY"))
        password = _blank_as_none(os.getenv(f"{prefix}_PASSWORD"))
        extra_opts = shlex.split(os.getenv(f"{prefix}_OPTS_EXTRA", ""))
        force_tty = (
            _blank_as_none(os.getenv(f"{prefix}_FORCE_TTY")) or "false"
        ).lower() in (
            "true",
            "1",
            "yes",
        )
        post_login_command = _blank_as_none(os.getenv(f"{prefix}_POST_LOGIN_COMMAND"))
        host_key_checking = (
            _blank_as_none(os.getenv(f"{prefix}_HOST_KEY_CHECKING")) or "strict"
        ).lower()
        known_hosts_file = _blank_as_none(os.getenv(f"{prefix}_KNOWN_HOSTS_FILE"))
        raw_batch_mode = _blank_as_none(os.getenv(f"{prefix}_BATCH_MODE"))
        if raw_batch_mode is None:
            batch_mode: bool | None = True
        elif raw_batch_mode.lower() == "inherit":
            batch_mode = None
        else:
            batch_mode = raw_batch_mode.lower() in ("true", "1", "yes")

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
            batch_mode=batch_mode,
            jump_host=jump_host,
        )

    def get_proxy_command_opts(self) -> list[str]:
        """Build a ProxyCommand whose SSH settings apply to the jump host."""
        if not self.jump_host:
            return []

        jump = self.jump_host

        # An explicit ProxyCommand is what makes the jump host's own key and
        # host-key policy apply, but it also bypasses ~/.ssh/config for the
        # bastion. When the resource configures nothing extra, hand it back to
        # OpenSSH so the operator's own bastion settings still work.
        if jump.defers_to_ssh_config:
            target = f"{jump.user}@{jump.host}"
            if jump.port != 22:
                target = f"{target}:{jump.port}"
            return ["-J", target]

        command = [
            "ssh",
            *jump.get_common_ssh_opts(),
            "-p",
            str(jump.port),
            *jump.get_key_auth_opts(),
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
        elif jump.uses_inherited_auth:
            command.extend(jump.get_batch_mode_opts())
        command.extend(
            [
                "-W",
                "%h:%p",
                f"{jump.user}@{jump.host}",
            ]
        )
        return ["-o", f"ProxyCommand={shlex.join(command)}"]

    def get_key_auth_opts(self, *, batch_mode: bool | None = None) -> list[str]:
        """Build key-based SSH authentication options.

        Args:
            batch_mode: Force BatchMode for this command. When omitted the
                resource's ``batch_mode`` setting applies, and ``None`` there
                emits no BatchMode option so ``~/.ssh/config`` decides.

        """
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
        effective = self.batch_mode if batch_mode is None else batch_mode
        if effective is not None:
            auth_opts.extend(["-o", f"BatchMode={'yes' if effective else 'no'}"])
        return auth_opts

    def get_host_key_opts(self) -> list[str]:
        """Build host-key verification options.

        Anything emitted here overrides ``~/.ssh/config``, because OpenSSH
        always prefers command-line options. Only the settings this resource
        actually specifies are emitted, so ``host_key_checking='inherit'``
        leaves the operator's own configuration untouched.
        """
        if self.host_key_checking == "inherit":
            if self.known_hosts_file is None:
                return []
            return ["-o", f"UserKnownHostsFile={self.known_hosts_file}"]

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

    def get_batch_mode_opts(self) -> list[str]:
        """Build the BatchMode option, or nothing when it is inherited."""
        if self.batch_mode is None:
            return []
        return ["-o", f"BatchMode={'yes' if self.batch_mode else 'no'}"]

    def get_auth_opts(self) -> list[str]:
        """Build authentication options for this connection's auth mode."""
        if self.uses_key_auth:
            auth_opts = self.get_key_auth_opts()
            auth_opts.extend(
                [
                    "-o",
                    "PreferredAuthentications=publickey",
                    "-o",
                    "PasswordAuthentication=no",
                ]
            )
            return auth_opts
        if self.uses_password_auth:
            return self.get_password_auth_opts()
        # Inherited auth: let OpenSSH pick the identity and method.
        return self.get_batch_mode_opts()

    def get_password_auth_opts(self, *, prompts: int = 3) -> list[str]:
        """Build password-based SSH authentication options."""
        if not self.uses_password_auth:
            return []
        return [
            "-o",
            "PreferredAuthentications=password,keyboard-interactive",
            "-o",
            "PubkeyAuthentication=no",
            "-o",
            f"NumberOfPasswordPrompts={prompts}",
        ]

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

    def auth_error_hint(self, stderr: str) -> str:
        """Return an actionable hint for a non-interactive authentication failure.

        BatchMode refuses every prompt, so a passphrase-protected key with no
        agent fails with a bare "Permission denied" instead of asking.
        """
        lowered = stderr.lower()
        if "permission denied" not in lowered and "no such identity" not in lowered:
            return ""
        if self.batch_mode is False or self.batch_mode is None:
            return ""
        return (
            "\nBatchMode is enabled, so OpenSSH cannot prompt for a key "
            "passphrase. Load the key into ssh-agent, use a passphrase-less "
            "key, or set batch_mode=None to defer to ~/.ssh/config."
        )

    def host_key_error_hint(self, stderr: str) -> str:
        """Return an actionable hint for an OpenSSH host-key failure."""
        if "host key verification failed" not in stderr.lower():
            return ""
        if self.host_key_checking == "inherit":
            return (
                "\nHost-key policy is inherited from ~/.ssh/config; check the "
                "UserKnownHostsFile and StrictHostKeyChecking settings there."
            )
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

        return [
            "ssh",
            *base_opts,
            *proxy_opts,
            "-p",
            str(self.port),
            *self.get_auth_opts(),
            f"{self.user}@{self.host}",
        ]

    def get_scp_base_command(self) -> list[str]:
        """Build base SCP command, including proxy and auth options."""
        proxy_opts = self.get_proxy_command_opts()
        base_opts = self.get_common_ssh_opts()

        return [
            "scp",
            *base_opts,
            *proxy_opts,
            "-P",
            str(self.port),
            *self.get_auth_opts(),
        ]

    def get_remote_target(self) -> str:
        """Get the remote target string for SCP commands."""
        return f"{self.user}@{self.host}"


# This is necessary for Pydantic to resolve the forward reference of "SSHConnectionResource"
# within its own definition (for the `jump_host` field).
SSHConnectionResource.model_rebuild()
