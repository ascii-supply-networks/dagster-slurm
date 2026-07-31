import os
import re
from typing import Optional

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field, PrivateAttr, model_validator

from .ssh import SSHConnectionResource

_SIGNAL_BEFORE_TIMEOUT_RE = re.compile(
    r"^(?P<signal>(?:SIG)?[A-Za-z][A-Za-z0-9_+]*|[0-9]+)@(?P<seconds>[1-9][0-9]*)$"
)
_SLURM_TIME_LIMIT_RE = re.compile(
    r"^(?:(?P<days>[0-9]+)-)?"
    r"(?P<first>[0-9]+)"
    r"(?::(?P<second>[0-9]+))?"
    r"(?::(?P<third>[0-9]+))?$"
)


def _optional_env(var_name: str, default: Optional[str] = None) -> Optional[str]:
    """Return an optional string from the environment.

    Treat blank strings as unset so callers can clear defaults by exporting an empty value.
    """
    raw_value = os.getenv(var_name)
    if raw_value is None:
        return default

    cleaned = raw_value.strip()
    if not cleaned:
        return None

    return cleaned


def _parse_signal_before_timeout(value: str) -> tuple[str, int]:
    normalized = value.strip().upper()
    match = _SIGNAL_BEFORE_TIMEOUT_RE.fullmatch(normalized)
    if match is None:
        raise ValueError(
            "signal_before_timeout must use SIGNAL@SECONDS, for example "
            "'TERM@120' or 'USR1@300'"
        )
    return normalized, int(match.group("seconds"))


def normalize_signal_before_timeout(value: str) -> str:
    """Validate and normalize a Slurm pre-timeout signal."""
    return _parse_signal_before_timeout(value)[0]


def _slurm_time_limit_seconds(value: str) -> int:
    """Parse a Slurm time limit into seconds."""
    normalized = value.strip()
    if not normalized:
        raise ValueError("time_limit cannot be empty")

    match = _SLURM_TIME_LIMIT_RE.fullmatch(normalized)
    if match is None:
        raise ValueError(f"Invalid Slurm time_limit: {value!r}")

    days_value = match.group("days")
    first = int(match.group("first"))
    second_value = match.group("second")
    third_value = match.group("third")

    if days_value is not None:
        days = int(days_value)
        hours = first
        minutes = int(second_value or 0)
        seconds = int(third_value or 0)
        if hours >= 24 or minutes >= 60 or seconds >= 60:
            raise ValueError(f"Invalid Slurm time_limit: {value!r}")
        return days * 86400 + hours * 3600 + minutes * 60 + seconds

    if second_value is None:
        return first * 60
    if third_value is None:
        seconds = int(second_value)
        if seconds >= 60:
            raise ValueError(f"Invalid Slurm time_limit: {value!r}")
        return first * 60 + seconds

    hours = first
    minutes = int(second_value)
    seconds = int(third_value)
    if minutes >= 60 or seconds >= 60:
        raise ValueError(f"Invalid Slurm time_limit: {value!r}")
    return hours * 3600 + minutes * 60 + seconds


def validate_signal_before_timeout(value: str | None, time_limit: str) -> str | None:
    """Validate a pre-timeout signal against its Slurm walltime."""
    if value is None:
        return None

    normalized, lead_seconds = _parse_signal_before_timeout(value)
    walltime_seconds = _slurm_time_limit_seconds(time_limit)
    if lead_seconds >= walltime_seconds:
        raise ValueError(
            f"signal_before_timeout lead time ({lead_seconds}s) must be shorter "
            f"than time_limit ({time_limit})"
        )
    return normalized


class SlurmQueueConfig(dg.ConfigurableResource):
    """Default Slurm job submission parameters.
    These can be overridden per-asset via metadata or function arguments.
    """

    partition: str = Field(
        default="", description="Slurm partition/queue (empty = cluster default)"
    )
    num_nodes: int = Field(default=1, description="Number of nodes")
    time_limit: str = Field(default="00:30:00", description="Job time limit (HH:MM:SS)")
    signal_before_timeout: Optional[str] = Field(
        default=None,
        description=(
            "Signal sent to the batch shell before walltime, for example 'TERM@120'. "
            "Slurm may deliver it up to 60 seconds early."
        ),
    )
    cpus: int = Field(default=2, description="CPUs per task")
    gpus_per_node: int = Field(default=0, description="GPUs per node")
    mem: Optional[str] = Field(
        default="4096M",
        description="Memory allocation (omit to use partition defaults)",
    )
    mem_per_cpu: Optional[str] = Field(
        default=None,
        description="Memory per CPU (alternative to mem, usually leave empty)",
    )
    qos: Optional[str] = Field(
        default=None, description="Quality of service / service level"
    )
    reservation: Optional[str] = Field(
        default=None, description="Reservation name for scheduled windows"
    )
    account: Optional[str] = Field(
        default=None,
        description="Accounting project/charge code (required on many systems)",
    )

    @model_validator(mode="after")
    def _validate_signal_before_timeout(self) -> "SlurmQueueConfig":
        validate_signal_before_timeout(self.signal_before_timeout, self.time_limit)
        return self


class SlurmResource(ConfigurableResource):
    """Complete Slurm cluster configuration.
    Combines SSH connection, queue defaults, and cluster-specific paths.
    """

    ssh: SSHConnectionResource = Field(description="SSH connection to Slurm cluster")
    queue: SlurmQueueConfig = Field(description="Default queue parameters")
    remote_base: Optional[str] = Field(
        default=None,
        description="Base directory on remote system (default: ~/pipelines/<run_id>)",
    )
    _auth_provider: Optional[object] = PrivateAttr(default=None)

    def set_auth_provider(self, provider: object) -> "SlurmResource":
        self._auth_provider = provider
        return self

    @classmethod
    def from_env_slurm(cls, ssh: SSHConnectionResource) -> "SlurmResource":
        """Create a SlurmResource by populating most fields from environment variables,
        but requires an explicit, pre-configured SSHConnectionResource to be provided.

        Args:
            ssh: A fully configured SSHConnectionResource instance.

        """
        return cls(
            # Use the provided ssh object directly
            ssh=ssh,
            # The rest of the configuration is still loaded from the environment
            queue=SlurmQueueConfig(
                partition=os.getenv("SLURM_PARTITION", "interactive"),
                time_limit=os.getenv("SLURM_TIME", "00:30:00"),
                cpus=int(os.getenv("SLURM_CPUS", "2")),
                mem=_optional_env("SLURM_MEM", "4096M"),
                mem_per_cpu=_optional_env("SLURM_MEM_PER_CPU"),
                num_nodes=int(os.getenv("SLURM_NUM_NODES", "1")),
                gpus_per_node=int(os.getenv("SLURM_GPUS_PER_NODE", "0")),
                qos=_optional_env("SLURM_QOS"),
                reservation=_optional_env("SLURM_RESERVATION"),
                account=_optional_env("SLURM_ACCOUNT"),
                signal_before_timeout=_optional_env("SLURM_SIGNAL_BEFORE_TIMEOUT"),
            ),
            remote_base=os.getenv("SLURM_REMOTE_BASE", "/home/submitter"),
        )

    @classmethod
    def from_env(cls) -> "SlurmResource":
        """Create from environment variables."""
        return cls(
            ssh=SSHConnectionResource.from_env(prefix="SLURM_SSH"),
            queue=SlurmQueueConfig(
                partition=os.getenv("SLURM_PARTITION", ""),
                time_limit=os.getenv("SLURM_TIME", "00:10:00"),
                cpus=int(os.getenv("SLURM_CPUS", "1")),
                mem=_optional_env("SLURM_MEM", "256M"),
                mem_per_cpu=_optional_env("SLURM_MEM_PER_CPU"),
                num_nodes=int(os.getenv("SLURM_NUM_NODES", "1")),
                gpus_per_node=int(os.getenv("SLURM_GPUS_PER_NODE", "0")),
                qos=_optional_env("SLURM_QOS"),
                reservation=_optional_env("SLURM_RESERVATION"),
                account=_optional_env("SLURM_ACCOUNT"),
                signal_before_timeout=_optional_env("SLURM_SIGNAL_BEFORE_TIMEOUT"),
            ),
            remote_base=os.getenv("SLURM_REMOTE_BASE", "/home/submitter"),
        )
