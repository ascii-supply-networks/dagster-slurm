"""Tests for poll_timeout parameter propagation.

Verifies that poll_timeout flows through the full call chain:
ComputeResource.run() -> SlurmPipesClient.run() -> _execute_standalone()
-> _wait_for_job_with_streaming()
"""

import inspect
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from dagster import AssetKey

from dagster_slurm import (
    BashLauncher,
    ComputeResource,
    SlurmResource,
    SSHConnectionResource,
    SlurmQueueConfig,
)
from dagster_slurm.config.environment import ExecutionMode
from dagster_slurm.pipes_clients.slurm_pipes_client import SlurmPipesClient


def _make_client() -> SlurmPipesClient:
    ssh = SSHConnectionResource(
        host="localhost",
        port=2223,
        user="testuser",
        password="testpass",
    )
    slurm = SlurmResource(
        ssh=ssh,
        queue=SlurmQueueConfig(
            partition="test",
            time_limit="00:10:00",
            cpus=2,
            mem="1G",
        ),
        remote_base="/tmp/dagster_test",
    )
    return SlurmPipesClient(
        slurm_resource=slurm,
        launcher=BashLauncher(),
    )


# ---------------------------------------------------------------------------
# Signature tests — verify the parameter exists with correct defaults
# ---------------------------------------------------------------------------


def test_run_accepts_poll_timeout():
    """SlurmPipesClient.run() accepts poll_timeout with default 3600."""
    sig = inspect.signature(SlurmPipesClient.run)
    assert "poll_timeout" in sig.parameters
    assert sig.parameters["poll_timeout"].default == 3600


def test_execute_standalone_accepts_poll_timeout():
    """_execute_standalone accepts poll_timeout with default 3600."""
    sig = inspect.signature(SlurmPipesClient._execute_standalone)
    assert "poll_timeout" in sig.parameters
    assert sig.parameters["poll_timeout"].default == 3600


def test_wait_for_job_default_poll_timeout():
    """_wait_for_job_with_streaming defaults to 3600s poll_timeout."""
    sig = inspect.signature(SlurmPipesClient._wait_for_job_with_streaming)
    assert sig.parameters["poll_timeout"].default == 3600


# ---------------------------------------------------------------------------
# Propagation tests — verify poll_timeout flows through the call chain
# ---------------------------------------------------------------------------


def test_execute_standalone_forwards_poll_timeout():
    """_execute_standalone passes poll_timeout to _wait_for_job_with_streaming."""
    client = _make_client()

    mock_ssh_pool = MagicMock()
    mock_ssh_pool.run.return_value = "Submitted batch job 12345"

    with (
        patch.object(client, "_wait_for_job_with_streaming") as mock_wait,
        patch.object(client, "_store_job_tags"),
        patch.object(client, "_log_estimated_start_time"),
        patch.object(client, "_get_asset_key_string", return_value="test_asset"),
        patch.object(client, "_build_sbatch_command", return_value="sbatch job.sh"),
    ):
        # Mock the script writing/upload portion
        mock_execution_plan = MagicMock()
        mock_execution_plan.payload = ["#!/bin/bash", "echo hello"]
        mock_execution_plan.resources = {}
        mock_execution_plan.kind = "bash"

        client._execute_standalone(
            execution_plan=mock_execution_plan,
            run_dir="/tmp/test_run",
            ssh_pool=mock_ssh_pool,
            message_reader=MagicMock(),
            poll_timeout=7200,
        )

        mock_wait.assert_called_once()
        _, kwargs = mock_wait.call_args
        assert kwargs["poll_timeout"] == 7200


def test_execute_standalone_uses_default_poll_timeout():
    """_execute_standalone uses default 3600s when poll_timeout not specified."""
    client = _make_client()

    mock_ssh_pool = MagicMock()
    mock_ssh_pool.run.return_value = "Submitted batch job 12345"

    with (
        patch.object(client, "_wait_for_job_with_streaming") as mock_wait,
        patch.object(client, "_store_job_tags"),
        patch.object(client, "_log_estimated_start_time"),
        patch.object(client, "_get_asset_key_string", return_value="test_asset"),
        patch.object(client, "_build_sbatch_command", return_value="sbatch job.sh"),
    ):
        mock_execution_plan = MagicMock()
        mock_execution_plan.payload = ["#!/bin/bash", "echo hello"]
        mock_execution_plan.resources = {}
        mock_execution_plan.kind = "bash"

        client._execute_standalone(
            execution_plan=mock_execution_plan,
            run_dir="/tmp/test_run",
            ssh_pool=mock_ssh_pool,
            message_reader=MagicMock(),
        )

        mock_wait.assert_called_once()
        _, kwargs = mock_wait.call_args
        assert kwargs["poll_timeout"] == 3600


def test_reattach_path_forwards_poll_timeout():
    """The reattach code path in run() also forwards poll_timeout."""
    client = _make_client()

    # Build a mock context that satisfies the run() method
    mock_context = MagicMock()
    mock_context.run.run_id = "test-run-id"
    mock_op_ctx = MagicMock()
    mock_context.op_execution_context = mock_op_ctx

    # Make _find_reattachable_job return a "running" job to trigger reattach
    reattach_info = {"job_id": "42", "run_dir": "/tmp/old_run"}

    with (
        patch.object(client, "_wait_for_job_with_streaming") as mock_wait,
        patch.object(client, "_execute_standalone") as mock_standalone,
        patch.object(client, "_find_reattachable_job", return_value=reattach_info),
        patch.object(client, "_is_job_still_running", return_value=True),
        patch.object(client, "_get_job_state", return_value="RUNNING"),
        patch.object(client, "_get_asset_key_string", return_value="test_asset"),
        patch.object(client, "_store_job_tags"),
        patch.object(client, "_maybe_emit_final_logs"),
        patch.object(client, "_collect_and_emit_metrics"),
        patch.object(client, "_get_remote_base", return_value="/tmp/dagster_test"),
        patch(
            "dagster_slurm.pipes_clients.slurm_pipes_client.SSHConnectionPool"
        ) as MockSSHPool,
        patch("dagster_slurm.pipes_clients.slurm_pipes_client.SSHMessageReader"),
        patch(
            "dagster_slurm.pipes_clients.slurm_pipes_client.open_pipes_session"
        ) as mock_open_pipes,
    ):
        mock_pool_instance = MagicMock()
        MockSSHPool.return_value = mock_pool_instance
        mock_pool_instance.__enter__ = MagicMock(return_value=mock_pool_instance)
        mock_pool_instance.__exit__ = MagicMock(return_value=False)

        mock_session = MagicMock()
        mock_open_pipes.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_open_pipes.return_value.__exit__ = MagicMock(return_value=False)

        client.run(
            context=mock_context,
            payload_path="test_payload.py",
            poll_timeout=14400,
        )

        # Verify the reattach path was taken, not the standalone path
        mock_standalone.assert_not_called()

        # Verify poll_timeout was forwarded in the reattach path
        mock_wait.assert_called_once()
        _, kwargs = mock_wait.call_args
        assert kwargs["poll_timeout"] == 14400


def test_wait_for_job_respects_custom_poll_timeout():
    """_wait_for_job_with_streaming times out based on poll_timeout value."""
    client = _make_client()

    mock_ssh_pool = MagicMock()
    # Simulate a job that is always PENDING (never completes)
    mock_ssh_pool.run.return_value = "PENDING"

    with pytest.raises(RuntimeError, match=r"Timed out after 1s"):
        client._wait_for_job_with_streaming(
            job_id=99999,
            ssh_pool=mock_ssh_pool,
            run_dir="/tmp/test_run",
            message_reader=MagicMock(),
            poll_timeout=1,
        )


# ---------------------------------------------------------------------------
# ComputeResource.run() -> client.run() propagation test
# ---------------------------------------------------------------------------


class _RecordingClient(SlurmPipesClient):
    """Dummy client that records kwargs passed to run()."""

    def __init__(self):
        self.kwargs = None

    def run(self, context, *, payload_path, **kwargs):
        self.kwargs = kwargs
        return SimpleNamespace()


def test_compute_resource_forwards_poll_timeout(monkeypatch):
    """ComputeResource.run() forwards poll_timeout to client.run() via kwargs."""
    slurm = SlurmResource(
        ssh=SSHConnectionResource(
            host="localhost", port=2222, user="test", password="secret"
        ),
        queue=SlurmQueueConfig(),
        remote_base="/tmp/dagster_test",
    )
    resource = ComputeResource(
        mode=ExecutionMode.SLURM,
        slurm=slurm,
        default_launcher=BashLauncher(),
    )

    fake_client = _RecordingClient()
    monkeypatch.setattr(
        ComputeResource,
        "get_pipes_client",
        lambda self, context, launcher=None: fake_client,
    )

    class _DummyContext:
        def __init__(self):
            self.asset_key = AssetKey("demo")
            self.assets_def = SimpleNamespace(metadata_by_key={})

        def has_assets_def(self):
            return True

    resource.run(
        context=_DummyContext(),
        payload_path="script.py",
        poll_timeout=7200,
    )

    assert fake_client.kwargs is not None
    assert fake_client.kwargs["poll_timeout"] == 7200
