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
        # This provides raw data for the main script step, including memory (MaxRSS).
        cmd = (
            f"sacct -j {job_id}.batch -n -P "
            f"--format=JobID,Elapsed,TotalCPU,MaxRSS,AllocNodes,AllocCPUS,State,ExitCode"
        )

        try:
            output = ssh_pool.run(cmd).strip()

            # Handle cases where the batch step might not exist or returns no output.
            if not output:
                self.logger.warning(
                    f"No sacct output found for job step {job_id}.batch."
                )
                return self._empty_metrics(job_id)

            # Take the first line of output in case of unexpected extra lines.
            line = output.splitlines()[0]
            fields = line.split("|")

            if len(fields) < 8:
                self.logger.warning(f"Incomplete metrics for job {job_id}: {line}")
                return self._empty_metrics(job_id)

            try:
                # The JobID field in the output is now the step ID (e.g., "16.batch")
                # We use the original job_id for consistency in our dataclass.
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
                self.logger.error(
                    f"Error parsing metrics for job {job_id} on data '{line}': {e}"
                )
                return self._empty_metrics(job_id)

        except Exception as e:
            self.logger.warning(f"Failed to collect metrics for job {job_id}: {e}")
            return self._empty_metrics(job_id)

    def _parse_time(self, time_str: str) -> float:
        """Parse Slurm time format to seconds, including milliseconds."""
        # Handle sub-second precision and improve robustness.
        if not time_str or time_str == "00:00:00":
            return 0.0

        days_seconds = 0.0
        if "-" in time_str:
            try:
                days_str, time_str = time_str.split("-")
                days_seconds = float(days_str) * 86400
            except ValueError:
                self.logger.warning(
                    f"Could not parse days from time string: '{time_str}'"
                )
                return 0.0

        parts = time_str.split(":")

        try:
            if len(parts) == 3:  # HH:MM:SS.ms
                h, m = float(parts[0]), float(parts[1])
                s_part = parts[2]
            elif len(parts) == 2:  # MM:SS.ms
                h = 0.0
                m, s_part = float(parts[0]), parts[1]
            elif len(parts) == 1:  # SS.ms
                h, m = 0.0, 0.0
                s_part = parts[0]
            else:
                return 0.0

            # Handle fractional seconds
            if "." in s_part:
                sec, ms = s_part.split(".")
                total_seconds = (
                    days_seconds
                    + h * 3600
                    + m * 60
                    + float(sec)
                    + float(ms.ljust(3, "0")) / 1000.0
                )
            else:
                total_seconds = days_seconds + h * 3600 + m * 60 + float(s_part)

            return total_seconds
        except (ValueError, IndexError):
            self.logger.warning(f"Could not parse time parts from: '{time_str}'")
            return 0.0

    def _parse_memory(self, mem_str: str) -> float:
        """Parse Slurm memory format to MB, correctly handling suffixes and defaults."""
        if not mem_str:
            return 0.0

        mem_str = mem_str.strip().upper()

        # Default to Kilobytes if no suffix is present, which is common for MaxRSS
        match = re.match(r"([\d.]+)([KMGT])?$", mem_str)
        if not match:
            self.logger.warning(f"Could not parse memory string: '{mem_str}'")
            return 0.0

        value = float(match.group(1))
        unit = match.group(2)

        if unit == "G":
            return value * 1024.0
        if unit == "M":
            return value
        if unit == "T":
            return value * 1024.0 * 1024.0

        # Default unit (None or 'K') is Kilobytes, convert to Megabytes
        return value / 1024.0

    def _parse_exit_code(self, exit_str: str) -> int:
        """Parse Slurm exit code format (e.g., '0:0' -> 0)."""
        if not exit_str:
            return 0

        try:
            return int(exit_str.split(":")[0])
        except (ValueError, IndexError):
            return -1  # Return -1 for unparseable exit codes

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
