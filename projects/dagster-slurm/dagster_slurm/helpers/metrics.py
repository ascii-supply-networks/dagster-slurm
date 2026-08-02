"""Slurm job metrics collection and extension hooks."""

from __future__ import annotations

import re
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

from dagster import MetadataValue, get_dagster_logger

if TYPE_CHECKING:
    from dagster_slurm.helpers.ssh_pool import SSHConnectionPool
    from dagster_slurm.resources.session import SlurmSessionResource
    from dagster_slurm.resources.slurm import SlurmResource


DEFAULT_SLURM_METADATA_KEYS = frozenset(
    {
        "slurm_job_id",
        "slurm_step_id",
        "slurm_accounting_available",
        "node_hours",
        "cpu_efficiency_pct",
        "max_memory_mb",
        "elapsed_seconds",
        "slurm_state",
        "slurm_exit_code",
        "slurm_gpu_accounting_available",
        "slurm_requested_gpus",
        "slurm_allocated_gpus",
        "slurm_gpu_utilization_avg_pct",
        "slurm_gpu_utilization_max_pct",
        "slurm_gpu_memory_max_mb",
        "slurm_gpu_utilization_max_node",
        "slurm_tres_accounting",
    }
)


class SlurmMetric(str, Enum):
    """Optional built-in Slurm metadata fields."""

    ELAPSED_SECONDS = "elapsed_seconds"
    NODE_HOURS = "node_hours"
    CPU_EFFICIENCY = "cpu_efficiency_pct"
    MAX_MEMORY = "max_memory_mb"
    STATE = "slurm_state"
    EXIT_CODE = "slurm_exit_code"
    GPU_ACCOUNTING_AVAILABLE = "slurm_gpu_accounting_available"
    REQUESTED_GPUS = "slurm_requested_gpus"
    ALLOCATED_GPUS = "slurm_allocated_gpus"
    GPU_UTILIZATION_AVG = "slurm_gpu_utilization_avg_pct"
    GPU_UTILIZATION_MAX = "slurm_gpu_utilization_max_pct"
    GPU_MEMORY_MAX = "slurm_gpu_memory_max_mb"
    GPU_UTILIZATION_MAX_NODE = "slurm_gpu_utilization_max_node"
    TRES_ACCOUNTING = "slurm_tres_accounting"


ALL_SLURM_METRICS = frozenset(SlurmMetric)
SlurmMetricSelection = Collection[SlurmMetric | str]


def normalize_slurm_metrics(
    metrics: SlurmMetricSelection | None,
) -> frozenset[SlurmMetric]:
    """Validate a built-in metric selection; ``None`` enables every metric."""
    if metrics is None:
        return ALL_SLURM_METRICS
    if isinstance(metrics, str):
        raise ValueError("slurm_metrics must be a collection, not a single string")

    try:
        return frozenset(SlurmMetric(metric) for metric in metrics)
    except (TypeError, ValueError) as error:
        valid = ", ".join(metric.value for metric in SlurmMetric)
        raise ValueError(f"Unknown Slurm metric. Valid values: {valid}") from error


@dataclass(frozen=True)
class SlurmJobMetrics:
    """Metrics collected from Slurm accounting."""

    job_id: int
    elapsed_seconds: float
    cpu_time_seconds: float
    max_rss_mb: float
    node_hours: float
    cpu_efficiency: float
    state: str
    exit_code: int
    accounting_available: bool = True
    requested_gpus: int | None = None
    allocated_gpus: int | None = None
    gpu_utilization_avg_pct: float | None = None
    gpu_utilization_max_pct: float | None = None
    gpu_memory_max_mb: float | None = None
    gpu_utilization_max_node: str | None = None
    accounting_rows: tuple[dict[str, str], ...] = field(default_factory=tuple)
    step_id: str | None = None

    @property
    def gpu_accounting_available(self) -> bool:
        """Whether Slurm reported any GPU utilization or memory values."""
        return any(
            value is not None
            for value in (
                self.gpu_utilization_avg_pct,
                self.gpu_utilization_max_pct,
                self.gpu_memory_max_mb,
            )
        )

    def to_metadata(
        self,
        slurm_metrics: SlurmMetricSelection | None = None,
    ) -> dict[str, Any]:
        """Return Dagster metadata for this accounting result."""
        enabled = normalize_slurm_metrics(slurm_metrics)
        metadata: dict[str, Any] = {
            "slurm_job_id": self.job_id,
            "slurm_accounting_available": self.accounting_available,
        }
        if self.step_id is not None:
            metadata["slurm_step_id"] = self.step_id
        if not self.accounting_available:
            return metadata

        values: dict[SlurmMetric, Any] = {
            SlurmMetric.NODE_HOURS: round(self.node_hours, 4),
            SlurmMetric.CPU_EFFICIENCY: round(self.cpu_efficiency * 100, 2),
            SlurmMetric.MAX_MEMORY: round(self.max_rss_mb, 2),
            SlurmMetric.ELAPSED_SECONDS: round(self.elapsed_seconds, 2),
            SlurmMetric.STATE: self.state,
            SlurmMetric.EXIT_CODE: self.exit_code,
            SlurmMetric.GPU_ACCOUNTING_AVAILABLE: self.gpu_accounting_available,
            SlurmMetric.REQUESTED_GPUS: self.requested_gpus,
            SlurmMetric.ALLOCATED_GPUS: self.allocated_gpus,
            SlurmMetric.GPU_UTILIZATION_AVG: self.gpu_utilization_avg_pct,
            SlurmMetric.GPU_UTILIZATION_MAX: self.gpu_utilization_max_pct,
            SlurmMetric.GPU_MEMORY_MAX: self.gpu_memory_max_mb,
            SlurmMetric.GPU_UTILIZATION_MAX_NODE: self.gpu_utilization_max_node,
        }
        metadata.update(
            {
                metric.value: round(value, 2) if isinstance(value, float) else value
                for metric, value in values.items()
                if metric in enabled and value is not None
            }
        )
        if SlurmMetric.TRES_ACCOUNTING in enabled and self.accounting_rows:
            metadata["slurm_tres_accounting"] = MetadataValue.json(
                list(self.accounting_rows)
            )
        return metadata


@dataclass(frozen=True)
class SlurmMetricsContext:
    """Context passed to a custom post-run metrics collector."""

    job_id: int
    ssh_pool: SSHConnectionPool
    slurm_resource: SlurmResource
    session_resource: SlurmSessionResource | None
    default_metrics: SlurmJobMetrics
    step_id: str | None = None


SlurmMetricsCallback = Callable[[SlurmMetricsContext], Mapping[str, Any]]


class SlurmMetricsCollector:
    """Collect CPU, memory, and GPU TRES metrics from completed Slurm jobs."""

    BASE_SACCT_FIELDS = (
        "JobID",
        "Elapsed",
        "TotalCPU",
        "MaxRSS",
        "AllocNodes",
        "AllocCPUS",
        "State",
        "ExitCode",
    )
    GPU_SACCT_FIELDS = (
        "NodeList",
        "ReqTRES",
        "AllocTRES",
        "TRESUsageInAve",
        "TRESUsageInMax",
        "TRESUsageInMaxNode",
        "TRESUsageInMaxTask",
        "TRESUsageInTot",
    )
    SACCT_FIELDS = BASE_SACCT_FIELDS + GPU_SACCT_FIELDS
    BASE_SACCT_FORMAT = ",".join(BASE_SACCT_FIELDS)
    SACCT_FORMAT = ",".join(SACCT_FIELDS)

    def __init__(self) -> None:
        self.logger = get_dagster_logger()

    def collect_job_metrics(
        self,
        job_id: int,
        ssh_pool: SSHConnectionPool,
        sacct_output: str | None = None,
        step_id: str | None = None,
    ) -> SlurmJobMetrics:
        """Query ``sacct`` and parse job-, step-, and node-aware metrics."""
        try:
            if step_id is not None and not re.fullmatch(
                rf"{job_id}\.[A-Za-z0-9_+-]+", step_id
            ):
                raise ValueError(f"Invalid Slurm step ID: {step_id!r}")
            accounting_target = step_id or str(job_id)
            output = sacct_output
            if output is None:
                output = ssh_pool.run(
                    f"sacct -j {accounting_target} -n -P "
                    f"--format={self.SACCT_FORMAT} "
                    "2>/dev/null || true"
                ).strip()
                if not output:
                    output = ssh_pool.run(
                        f"sacct -j {accounting_target} -n -P "
                        f"--format={self.BASE_SACCT_FORMAT} 2>/dev/null || true"
                    ).strip()

            rows = self._parse_accounting_rows(output)
            if not rows:
                self.logger.warning("No sacct metrics found for job %s.", job_id)
                return self._empty_metrics(job_id, step_id=step_id)

            primary = self._select_primary_row(job_id, rows, step_id=step_id)
            elapsed = self._parse_time(primary["Elapsed"])
            cpu_time = self._parse_time(primary["TotalCPU"])
            max_rss = self._parse_memory(primary["MaxRSS"])
            nodes = self._parse_int(primary["AllocNodes"])
            cpus = self._parse_int(primary["AllocCPUS"])
            cpu_efficiency = (
                cpu_time / (elapsed * cpus) if elapsed > 0 and cpus > 0 else 0.0
            )

            return SlurmJobMetrics(
                job_id=job_id,
                elapsed_seconds=elapsed,
                cpu_time_seconds=cpu_time,
                max_rss_mb=max_rss,
                node_hours=(elapsed / 3600) * nodes,
                cpu_efficiency=min(cpu_efficiency, 1.0),
                state=primary["State"],
                exit_code=self._parse_exit_code(primary["ExitCode"]),
                requested_gpus=self._gpu_count(rows, "ReqTRES"),
                allocated_gpus=self._gpu_count(rows, "AllocTRES"),
                gpu_utilization_avg_pct=self._primary_gpu_value(
                    job_id,
                    rows,
                    "TRESUsageInAve",
                    "gres/gpuutil",
                    step_id=step_id,
                ),
                gpu_utilization_max_pct=self._max_gpu_value(
                    rows, "TRESUsageInMax", "gres/gpuutil", memory=False
                ),
                gpu_memory_max_mb=self._max_gpu_value(
                    rows, "TRESUsageInMax", "gres/gpumem", memory=True
                ),
                gpu_utilization_max_node=self._gpu_max_origin(
                    rows,
                    value_field_name="TRESUsageInMax",
                    origin_field_name="TRESUsageInMaxNode",
                    tres_name="gres/gpuutil",
                ),
                accounting_rows=tuple(self._audit_row(row) for row in rows),
                step_id=step_id,
            )
        except Exception as error:
            self.logger.warning(
                "Failed to collect metrics for job %s: %s", job_id, error
            )
            return self._empty_metrics(job_id, step_id=step_id)

    def _parse_accounting_rows(self, output: str | None) -> list[dict[str, str]]:
        """Parse parsable ``sacct`` output, accepting the base format as fallback."""
        if not output:
            return []

        rows: list[dict[str, str]] = []
        for line in output.splitlines():
            values = [value.strip() for value in line.split("|")]
            if len(values) < len(self.BASE_SACCT_FIELDS) or not values[0]:
                continue
            padded_values = values + [""] * (len(self.SACCT_FIELDS) - len(values))
            rows.append(dict(zip(self.SACCT_FIELDS, padded_values)))
        return rows

    @staticmethod
    def _select_primary_row(
        job_id: int,
        rows: list[dict[str, str]],
        *,
        step_id: str | None = None,
    ) -> dict[str, str]:
        expected_ids = (
            (step_id, f"{job_id}.batch", str(job_id))
            if step_id is not None
            else (f"{job_id}.batch", str(job_id))
        )
        for expected_id in expected_ids:
            for row in rows:
                if row["JobID"] == expected_id:
                    return row
        return rows[0]

    @staticmethod
    def _parse_tres(value: str) -> dict[str, str]:
        parsed: dict[str, str] = {}
        for item in value.split(","):
            key, separator, raw_value = item.strip().partition("=")
            if separator and key:
                parsed[key] = raw_value
        return parsed

    def _gpu_count(self, rows: list[dict[str, str]], field_name: str) -> int | None:
        counts: list[int] = []
        for row in rows:
            tres = self._parse_tres(row[field_name])
            if "gres/gpu" in tres:
                counts.append(self._parse_int(tres["gres/gpu"]))
                continue
            typed_counts = [
                self._parse_int(value)
                for key, value in tres.items()
                if key.startswith("gres/gpu:")
            ]
            if typed_counts:
                counts.append(sum(typed_counts))
        return max(counts) if counts else None

    def _primary_gpu_value(
        self,
        job_id: int,
        rows: list[dict[str, str]],
        field_name: str,
        tres_name: str,
        *,
        step_id: str | None = None,
    ) -> float | None:
        primary = self._select_primary_row(job_id, rows, step_id=step_id)
        value = self._find_tres_value(primary[field_name], tres_name)
        if value is not None:
            return self._parse_number(value)
        return self._max_gpu_value(rows, field_name, tres_name, memory=False)

    def _max_gpu_value(
        self,
        rows: list[dict[str, str]],
        field_name: str,
        tres_name: str,
        *,
        memory: bool,
    ) -> float | None:
        values: list[float] = []
        for row in rows:
            raw_value = self._find_tres_value(row[field_name], tres_name)
            if raw_value is None:
                continue
            values.append(
                self._parse_memory(raw_value)
                if memory
                else self._parse_number(raw_value)
            )
        return max(values) if values else None

    def _gpu_max_origin(
        self,
        rows: list[dict[str, str]],
        *,
        value_field_name: str,
        origin_field_name: str,
        tres_name: str,
    ) -> str | None:
        maximum: float | None = None
        maximum_origin: str | None = None
        for row in rows:
            raw_value = self._find_tres_value(row[value_field_name], tres_name)
            origin = self._find_tres_value(row[origin_field_name], tres_name)
            if raw_value is None or origin is None:
                continue
            value = self._parse_number(raw_value)
            if maximum is None or value > maximum:
                maximum = value
                maximum_origin = origin
        return maximum_origin

    def _find_tres_value(self, raw_tres: str, tres_name: str) -> str | None:
        tres = self._parse_tres(raw_tres)
        if tres_name in tres:
            return tres[tres_name]
        typed_values = [
            value for key, value in tres.items() if key.startswith(f"{tres_name}:")
        ]
        return typed_values[0] if typed_values else None

    @staticmethod
    def _audit_row(row: dict[str, str]) -> dict[str, str]:
        """Keep raw scheduler values needed to audit per-step GPU summaries."""
        fields = (
            "JobID",
            "NodeList",
            "ReqTRES",
            "AllocTRES",
            "TRESUsageInAve",
            "TRESUsageInMax",
            "TRESUsageInMaxNode",
            "TRESUsageInMaxTask",
            "TRESUsageInTot",
        )
        return {field_name: row[field_name] for field_name in fields}

    def _parse_time(self, time_str: str) -> float:
        """Parse Slurm time format to seconds, including milliseconds."""
        if not time_str or time_str == "00:00:00":
            return 0.0

        days_seconds = 0.0
        if "-" in time_str:
            try:
                days_str, time_str = time_str.split("-", maxsplit=1)
                days_seconds = float(days_str) * 86400
            except ValueError:
                self.logger.warning("Could not parse Slurm time: %r", time_str)
                return 0.0

        parts = time_str.split(":")
        try:
            if len(parts) == 3:
                hours, minutes, seconds = map(float, parts)
            elif len(parts) == 2:
                hours = 0.0
                minutes, seconds = map(float, parts)
            elif len(parts) == 1:
                hours = minutes = 0.0
                seconds = float(parts[0])
            else:
                return 0.0
            return days_seconds + hours * 3600 + minutes * 60 + seconds
        except ValueError:
            self.logger.warning("Could not parse Slurm time: %r", time_str)
            return 0.0

    def _parse_memory(self, mem_str: str) -> float:
        """Parse a Slurm memory value into MiB."""
        if not mem_str:
            return 0.0
        match = re.fullmatch(r"([\d.]+)([KMGTPE]?)B?", mem_str.strip().upper())
        if not match:
            self.logger.warning("Could not parse Slurm memory: %r", mem_str)
            return 0.0

        value = float(match.group(1))
        unit = match.group(2)
        powers = {"": -1, "K": -1, "M": 0, "G": 1, "T": 2, "P": 3, "E": 4}
        return value * (1024.0 ** powers[unit])

    @staticmethod
    def _parse_number(value: str) -> float:
        match = re.fullmatch(r"([\d.]+)([KMGTPE]?)", value.strip().upper())
        if not match:
            raise ValueError(f"Invalid Slurm numeric value: {value!r}")
        multipliers = {
            "": 1.0,
            "K": 1_000.0,
            "M": 1_000_000.0,
            "G": 1_000_000_000.0,
            "T": 1_000_000_000_000.0,
            "P": 1_000_000_000_000_000.0,
            "E": 1_000_000_000_000_000_000.0,
        }
        return float(match.group(1)) * multipliers[match.group(2)]

    @staticmethod
    def _parse_int(value: str) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _parse_exit_code(exit_str: str) -> int:
        """Parse Slurm exit code format (for example, ``0:0``)."""
        if not exit_str:
            return 0
        try:
            return int(exit_str.split(":", maxsplit=1)[0])
        except (ValueError, IndexError):
            return -1

    @staticmethod
    def _empty_metrics(job_id: int, *, step_id: str | None = None) -> SlurmJobMetrics:
        """Return a result that distinguishes unavailable accounting from zero."""
        return SlurmJobMetrics(
            job_id=job_id,
            elapsed_seconds=0.0,
            cpu_time_seconds=0.0,
            max_rss_mb=0.0,
            node_hours=0.0,
            cpu_efficiency=0.0,
            state="UNKNOWN",
            exit_code=-1,
            accounting_available=False,
            step_id=step_id,
        )
