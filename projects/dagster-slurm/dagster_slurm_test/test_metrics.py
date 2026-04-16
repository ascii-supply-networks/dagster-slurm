"""Tests for metrics collection."""

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

from dagster_slurm import BashLauncher
from dagster_slurm.helpers.metrics import SlurmJobMetrics, SlurmMetricsCollector
from dagster_slurm.pipes_clients.slurm_pipes_client import SlurmPipesClient


def test_parse_time():
    """Test time parsing."""
    collector = SlurmMetricsCollector()

    # Test various formats
    assert collector._parse_time("01:30") == 90.0  # MM:SS
    assert collector._parse_time("02:30:45") == 9045.0  # HH:MM:SS
    assert collector._parse_time("1-02:30:45") == 95445.0  # DD-HH:MM:SS
    assert collector._parse_time("00:00:00") == 0.0


def test_parse_memory():
    """Test memory parsing."""
    collector = SlurmMetricsCollector()

    assert collector._parse_memory("1024K") == 1.0  # 1MB
    assert collector._parse_memory("512M") == 512.0
    assert collector._parse_memory("2G") == 2048.0
    assert collector._parse_memory("1T") == 1048576.0


def test_parse_exit_code():
    """Test exit code parsing."""
    collector = SlurmMetricsCollector()

    assert collector._parse_exit_code("0:0") == 0
    assert collector._parse_exit_code("1:0") == 1
    assert collector._parse_exit_code("127:15") == 127


def test_collect_and_emit_metrics_skips_contexts_without_output_metadata():
    class FakeMetricsCollector(SlurmMetricsCollector):
        def collect_job_metrics(self, job_id: int, ssh_pool) -> SlurmJobMetrics:
            return SlurmJobMetrics(
                job_id=job_id,
                elapsed_seconds=123.0,
                cpu_time_seconds=61.5,
                max_rss_mb=512.0,
                node_hours=1.5,
                cpu_efficiency=0.5,
                state="COMPLETED",
                exit_code=0,
            )

    client = SlurmPipesClient(
        slurm_resource=cast(Any, SimpleNamespace(ssh=None, queue=None)),
        launcher=BashLauncher(),
    )
    client.logger = MagicMock()
    client.metrics_collector = FakeMetricsCollector()

    asset_check_context = SimpleNamespace(op_execution_context=SimpleNamespace())

    client._collect_and_emit_metrics(
        job_id=1234,
        ssh_pool=MagicMock(),
        context=asset_check_context,
    )

    client.logger.warning.assert_not_called()
