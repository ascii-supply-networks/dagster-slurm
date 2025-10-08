"""Slurm job metrics collection."""

import re
from dataclasses import dataclass

from dagster import get_dagster_logger


@dataclass
class SlurmJobMetrics:
    """Metrics from sacct."""

    job_id: int
    elapsed_seconds: float
    cpu_time_seconds: float
    max_rss_mb: float
    node_hours: float
    cpu_efficiency: float  # 0.0 to 1.0
    state: str
    exit_code: int


class SlurmMetricsCollector:
    """Collects detailed metrics from Slurm jobs."""

    def __init__(self):
        self.logger = get_dagster_logger()

    def collect_job_metrics(
        self,
        job_id: int,
        ssh_pool,
    ) -> SlurmJobMetrics:
        """Query sacct for detailed job statistics.

        Args:
            job_id: Slurm job ID
            ssh_pool: SSH connection pool

        Returns:
            SlurmJobMetrics with detailed stats

        """
        cmd = (
            f"sacct -j {job_id} -X -n -P "
            f"--format=JobID,Elapsed,TotalCPU,MaxRSS,AllocNodes,AllocCPUS,State,ExitCode"
        )

        try:
            output = ssh_pool.run(cmd)
        except Exception as e:
            self.logger.warning(f"Failed to collect metrics for job {job_id}: {e}")
            return self._empty_metrics(job_id)

        # Parse sacct output
        fields = output.strip().split("|")

        if len(fields) < 8:
            self.logger.warning(f"Incomplete metrics for job {job_id}")
            return self._empty_metrics(job_id)

        try:
            elapsed = self._parse_time(fields[1])
            cpu_time = self._parse_time(fields[2])
            max_rss = self._parse_memory(fields[3])
            nodes = int(fields[4])
            cpus = int(fields[5])
            state = fields[6]
            exit_code = self._parse_exit_code(fields[7])

            node_hours = (elapsed / 3600) * nodes
            cpu_efficiency = (
                (cpu_time / (elapsed * cpus)) if elapsed > 0 and cpus > 0 else 0.0
            )

            return SlurmJobMetrics(
                job_id=job_id,
                elapsed_seconds=elapsed,
                cpu_time_seconds=cpu_time,
                max_rss_mb=max_rss,
                node_hours=node_hours,
                cpu_efficiency=min(cpu_efficiency, 1.0),  # Cap at 100%
                state=state,
                exit_code=exit_code,
            )

        except Exception as e:
            self.logger.error(f"Error parsing metrics for job {job_id}: {e}")
            return self._empty_metrics(job_id)

    def _parse_time(self, time_str: str) -> float:
        """Parse Slurm time format to seconds.
        Formats: MM:SS, HH:MM:SS, DD-HH:MM:SS.
        """
        if not time_str or time_str == "00:00:00":
            return 0.0

        # Handle DD-HH:MM:SS
        if "-" in time_str:
            days, rest = time_str.split("-")
            time_str = rest
            days_seconds = int(days) * 86400
        else:
            days_seconds = 0

        # Handle HH:MM:SS or MM:SS
        parts = time_str.split(":")
        if len(parts) == 3:
            hours, minutes, seconds = map(int, parts)
        elif len(parts) == 2:
            hours = 0
            minutes, seconds = map(int, parts)
        else:
            return 0.0

        total_seconds = days_seconds + hours * 3600 + minutes * 60 + seconds
        return float(total_seconds)

    def _parse_memory(self, mem_str: str) -> float:
        """Parse Slurm memory format to MB.
        Formats: 1234K, 1234M, 1234G, 1234T.
        """
        if not mem_str:
            return 0.0

        # Remove any trailing whitespace
        mem_str = mem_str.strip()

        # Extract number and unit
        match = re.match(r"([\d.]+)([KMGT]?)", mem_str)
        if not match:
            return 0.0

        value = float(match.group(1))
        unit = match.group(2) or "M"  # Default to MB

        # Convert to MB
        multipliers = {
            "K": 1 / 1024,
            "M": 1,
            "G": 1024,
            "T": 1024 * 1024,
        }

        return value * multipliers.get(unit, 1)

    def _parse_exit_code(self, exit_str: str) -> int:
        """Parse Slurm exit code format (e.g., '0:0' -> 0)."""
        if not exit_str:
            return 0

        # Exit code format is typically "exitcode:signal"
        parts = exit_str.split(":")
        try:
            return int(parts[0])
        except (ValueError, IndexError):
            return -1

    def _empty_metrics(self, job_id: int) -> SlurmJobMetrics:
        """Return empty metrics when collection fails."""
        return SlurmJobMetrics(
            job_id=job_id,
            elapsed_seconds=0.0,
            cpu_time_seconds=0.0,
            max_rss_mb=0.0,
            node_hours=0.0,
            cpu_efficiency=0.0,
            state="UNKNOWN",
            exit_code=-1,
        )
