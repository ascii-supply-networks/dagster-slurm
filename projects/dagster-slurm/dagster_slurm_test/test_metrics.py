"""Tests for metrics collection."""

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from dagster import FloatMetadataValue

from dagster_slurm import BashLauncher, SlurmMetric, SlurmMetricsContext
from dagster_slurm.helpers.metrics import (
    SlurmJobMetrics,
    SlurmMetricsCollector,
    normalize_slurm_metrics,
)
from dagster_slurm.pipes_clients.slurm_pipes_client import SlurmPipesClient
from dagster_slurm.resources.session import SlurmStepExecutionResult


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


def test_collect_job_metrics_includes_gpu_tres_and_raw_step_rows():
    sacct_output = "\n".join(
        (
            "123|00:10:00|00:40:00|2048M|2|8|COMPLETED|0:0|node[01-02]|"
            "cpu=8,gres/gpu=4|cpu=8,gres/gpu=4|||||",
            "123.batch|00:10:00|00:40:00|2048M|2|8|COMPLETED|0:0|node[01-02]|"
            "cpu=8,gres/gpu=4|cpu=8,gres/gpu=4|"
            "gres/gpumem=4G,gres/gpuutil=37.5|"
            "gres/gpumem=8G,gres/gpuutil=92|"
            "gres/gpuutil=node02|gres/gpuutil=3|"
            "gres/gpumem=12G,gres/gpuutil=150",
        )
    )

    metrics = SlurmMetricsCollector().collect_job_metrics(
        job_id=123,
        ssh_pool=MagicMock(),
        sacct_output=sacct_output,
    )

    assert metrics.accounting_available is True
    assert metrics.requested_gpus == 4
    assert metrics.allocated_gpus == 4
    assert metrics.gpu_utilization_avg_pct == 37.5
    assert metrics.gpu_utilization_max_pct == 92.0
    assert metrics.gpu_memory_max_mb == 8192.0
    assert metrics.gpu_utilization_max_node == "node02"
    assert len(metrics.accounting_rows) == 2
    assert metrics.accounting_rows[1]["TRESUsageInMaxTask"] == "gres/gpuutil=3"

    metadata = metrics.to_metadata()
    assert metadata["slurm_gpu_accounting_available"] is True
    assert metadata["slurm_gpu_utilization_avg_pct"] == 37.5
    assert metadata["slurm_gpu_memory_max_mb"] == 8192.0


def test_slurm_metric_selection_defaults_to_all_and_filters_optional_metadata():
    metrics = SlurmJobMetrics(
        job_id=123,
        step_id="123.7",
        elapsed_seconds=60.0,
        cpu_time_seconds=30.0,
        max_rss_mb=1.0,
        node_hours=1 / 60,
        cpu_efficiency=0.5,
        state="COMPLETED",
        exit_code=0,
        gpu_utilization_avg_pct=40.0,
        gpu_memory_max_mb=3072.0,
    )

    assert "cpu_efficiency_pct" in metrics.to_metadata()
    assert metrics.to_metadata(
        {
            SlurmMetric.GPU_UTILIZATION_AVG,
            "slurm_gpu_memory_max_mb",
        }
    ) == {
        "slurm_job_id": 123,
        "slurm_step_id": "123.7",
        "slurm_accounting_available": True,
        "slurm_gpu_utilization_avg_pct": 40.0,
        "slurm_gpu_memory_max_mb": 3072.0,
    }
    assert metrics.to_metadata([]) == {
        "slurm_job_id": 123,
        "slurm_step_id": "123.7",
        "slurm_accounting_available": True,
    }


def test_slurm_metric_selection_rejects_unknown_values():
    with pytest.raises(ValueError, match="Unknown Slurm metric"):
        normalize_slurm_metrics(["gpu_temperature"])


def test_collect_and_emit_metrics_honors_slurm_metric_selection():
    client = SlurmPipesClient(
        slurm_resource=cast(Any, SimpleNamespace(ssh=None, queue=None)),
        launcher=BashLauncher(),
    )
    add_output_metadata = MagicMock()

    client._collect_and_emit_metrics(
        job_id=123,
        ssh_pool=MagicMock(),
        context=SimpleNamespace(add_output_metadata=add_output_metadata),
        metrics=SlurmJobMetrics(
            job_id=123,
            elapsed_seconds=60.0,
            cpu_time_seconds=30.0,
            max_rss_mb=1.0,
            node_hours=1 / 60,
            cpu_efficiency=0.5,
            state="COMPLETED",
            exit_code=0,
        ),
        slurm_metrics=[SlurmMetric.STATE],
    )

    assert add_output_metadata.call_args.args[0] == {
        "slurm_job_id": 123,
        "slurm_accounting_available": True,
        "slurm_state": "COMPLETED",
    }


def test_collect_job_metrics_distinguishes_unavailable_accounting_from_zero():
    ssh_pool = MagicMock()
    ssh_pool.run.side_effect = ["", ""]

    metrics = SlurmMetricsCollector().collect_job_metrics(123, ssh_pool)

    assert metrics.accounting_available is False
    assert metrics.gpu_utilization_avg_pct is None
    assert metrics.to_metadata() == {
        "slurm_job_id": 123,
        "slurm_accounting_available": False,
    }
    assert ssh_pool.run.call_count == 2


def test_collect_job_metrics_preserves_reported_zero_gpu_utilization():
    sacct_output = (
        "123.batch|00:01:00|00:00:30|1024K|1|1|COMPLETED|0:0|node01|"
        "gres/gpu=1|gres/gpu=1|gres/gpumem=0,gres/gpuutil=0|"
        "gres/gpumem=0,gres/gpuutil=0|gres/gpuutil=node01|"
        "gres/gpuutil=0|gres/gpumem=0,gres/gpuutil=0"
    )

    metrics = SlurmMetricsCollector().collect_job_metrics(
        job_id=123,
        ssh_pool=MagicMock(),
        sacct_output=sacct_output,
    )

    assert metrics.accounting_available is True
    assert metrics.gpu_accounting_available is True
    assert metrics.gpu_utilization_avg_pct == 0.0
    assert metrics.gpu_memory_max_mb == 0.0


def test_collect_job_metrics_accepts_base_sacct_fallback_without_gpu_fields():
    metrics = SlurmMetricsCollector().collect_job_metrics(
        job_id=123,
        ssh_pool=MagicMock(),
        sacct_output="123.batch|00:01:00|00:00:30|1024K|1|1|COMPLETED|0:0",
    )

    assert metrics.accounting_available is True
    assert metrics.max_rss_mb == 1.0
    assert metrics.gpu_accounting_available is False
    assert metrics.requested_gpus is None


def test_collect_session_step_metrics_queries_completed_step():
    client = SlurmPipesClient(
        slurm_resource=cast(Any, SimpleNamespace(ssh=None, queue=None)),
        launcher=BashLauncher(),
    )
    ssh_pool = MagicMock()
    ssh_pool.run.return_value = (
        "123.7|00:01:00|00:00:30|1024K|1|1|COMPLETED|0:0|node01|"
        "gres/gpu=1|gres/gpu=1|gres/gpumem=2G,gres/gpuutil=40|"
        "gres/gpumem=3G,gres/gpuutil=80|gres/gpuutil=node01|"
        "gres/gpuutil=0|gres/gpumem=3G,gres/gpuutil=80"
    )

    metrics = client._collect_session_step_metrics(
        step_result=SlurmStepExecutionResult(
            job_id=123,
            step_id="123.7",
            stdout_path="/tmp/stdout",
            stderr_path="/tmp/stderr",
        ),
        ssh_pool=ssh_pool,
    )

    assert metrics.step_id == "123.7"
    assert metrics.gpu_utilization_avg_pct == 40.0
    assert "sacct -j 123.7" in ssh_pool.run.call_args.args[0]
    assert metrics.to_metadata()["slurm_step_id"] == "123.7"


def test_collect_job_metrics_uses_step_average_in_shared_allocation():
    sacct_output = "\n".join(
        (
            "123|00:10:00|00:40:00|2048M|2|8|RUNNING|0:0|node[01-02]|"
            "gres/gpu=4|gres/gpu=4|gres/gpuutil=10|||||",
            "123.7|00:01:00|00:00:30|1024K|1|1|COMPLETED|0:0|node01|"
            "gres/gpu=1|gres/gpu=1|gres/gpuutil=40|gres/gpuutil=80|"
            "gres/gpuutil=node01|gres/gpuutil=0|gres/gpuutil=40",
        )
    )

    metrics = SlurmMetricsCollector().collect_job_metrics(
        job_id=123,
        step_id="123.7",
        ssh_pool=MagicMock(),
        sacct_output=sacct_output,
    )

    assert metrics.gpu_utilization_avg_pct == 40.0


def test_custom_metrics_collector_receives_context_and_emits_metadata():
    client = SlurmPipesClient(
        slurm_resource=cast(Any, SimpleNamespace(ssh=None, queue=None)),
        launcher=BashLauncher(),
    )
    add_output_metadata = MagicMock()
    context = SimpleNamespace(add_output_metadata=add_output_metadata)
    ssh_pool = MagicMock()
    prefix = "site"

    def collect(metrics_context: SlurmMetricsContext):
        assert metrics_context.job_id == 123
        assert metrics_context.ssh_pool is ssh_pool
        assert metrics_context.slurm_resource is client.slurm
        assert metrics_context.step_id == "123.7"
        return {
            f"{prefix}/energy_joules": 42.5,
            f"{prefix}/scheduler": "slurmdbd",
        }

    client._collect_and_emit_metrics(
        job_id=123,
        ssh_pool=ssh_pool,
        context=context,
        metrics=SlurmMetricsCollector._empty_metrics(123, step_id="123.7"),
        custom_metrics_collector=collect,
    )

    emitted_metadata = add_output_metadata.call_args.args[0]
    assert emitted_metadata["slurm_accounting_available"] is False
    assert emitted_metadata["slurm_step_id"] == "123.7"
    assert emitted_metadata["site/scheduler"].value == "slurmdbd"
    assert isinstance(emitted_metadata["site/energy_joules"], FloatMetadataValue)
    assert emitted_metadata["site/energy_joules"].value == 42.5


def test_invalid_custom_metrics_do_not_hide_default_metrics():
    client = SlurmPipesClient(
        slurm_resource=cast(Any, SimpleNamespace(ssh=None, queue=None)),
        launcher=BashLauncher(),
    )
    client.logger = MagicMock()
    add_output_metadata = MagicMock()

    client._collect_and_emit_metrics(
        job_id=123,
        ssh_pool=MagicMock(),
        context=SimpleNamespace(add_output_metadata=add_output_metadata),
        metrics=SlurmMetricsCollector._empty_metrics(123),
        custom_metrics_collector=lambda _: {
            "slurm_job_id": 999,
            "site/invalid": object(),
        },
    )

    emitted_metadata = add_output_metadata.call_args.args[0]
    assert emitted_metadata["slurm_job_id"] == 123
    assert "site/invalid" not in emitted_metadata
    client.logger.warning.assert_called()


def test_collect_and_emit_metrics_skips_contexts_without_output_metadata():
    class FakeMetricsCollector(SlurmMetricsCollector):
        def collect_job_metrics(
            self,
            job_id: int,
            ssh_pool,
            sacct_output: str | None = None,
            step_id: str | None = None,
        ) -> SlurmJobMetrics:
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
    custom_metrics_collector = MagicMock(return_value={"unused": 1})

    client._collect_and_emit_metrics(
        job_id=1234,
        ssh_pool=MagicMock(),
        context=asset_check_context,
        custom_metrics_collector=custom_metrics_collector,
    )

    client.logger.warning.assert_not_called()
    custom_metrics_collector.assert_not_called()
