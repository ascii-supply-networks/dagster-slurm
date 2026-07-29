"""Tests for poll_timeout parameter propagation.

Verifies that poll_timeout flows through the full call chain:
ComputeResource.run() -> SlurmPipesClient.run() -> _execute_standalone()
-> _wait_for_job_with_streaming()
"""

import inspect
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from dagster import AssetKey
from dagster._core.errors import DagsterPipesExecutionError

from dagster_slurm import ComputeResource
from dagster_slurm.config.environment import ExecutionMode
from dagster_slurm.helpers import SlurmJobMetrics
from dagster_slurm.pipes_clients.slurm_pipes_client import SlurmPipesClient

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


def test_execute_standalone_forwards_poll_timeout(
    slurm_pipes_client: SlurmPipesClient,
):
    """_execute_standalone passes poll_timeout to _wait_for_job_with_streaming."""
    mock_ssh_pool = MagicMock()
    mock_ssh_pool.run.return_value = "Submitted batch job 12345"

    with (
        patch.object(slurm_pipes_client, "_wait_for_job_with_streaming") as mock_wait,
        patch.object(slurm_pipes_client, "_store_job_tags"),
        patch.object(slurm_pipes_client, "_log_estimated_start_time"),
        patch.object(
            slurm_pipes_client, "_get_asset_key_string", return_value="test_asset"
        ),
        patch.object(
            slurm_pipes_client,
            "_build_sbatch_command",
            return_value="sbatch job.sh",
        ),
    ):
        # Mock the script writing/upload portion
        mock_execution_plan = MagicMock()
        mock_execution_plan.payload = ["#!/bin/bash", "echo hello"]
        mock_execution_plan.resources = {}
        mock_execution_plan.kind = "bash"

        slurm_pipes_client._execute_standalone(
            execution_plan=mock_execution_plan,
            run_dir="/tmp/test_run",
            ssh_pool=mock_ssh_pool,
            message_reader=MagicMock(),
            poll_timeout=7200,
        )

        mock_wait.assert_called_once()
        _, kwargs = mock_wait.call_args
        assert kwargs["poll_timeout"] == 7200


def test_execute_standalone_uses_default_poll_timeout(
    slurm_pipes_client: SlurmPipesClient,
):
    """_execute_standalone uses default 3600s when poll_timeout not specified."""
    mock_ssh_pool = MagicMock()
    mock_ssh_pool.run.return_value = "Submitted batch job 12345"

    with (
        patch.object(slurm_pipes_client, "_wait_for_job_with_streaming") as mock_wait,
        patch.object(slurm_pipes_client, "_store_job_tags"),
        patch.object(slurm_pipes_client, "_log_estimated_start_time"),
        patch.object(
            slurm_pipes_client, "_get_asset_key_string", return_value="test_asset"
        ),
        patch.object(
            slurm_pipes_client,
            "_build_sbatch_command",
            return_value="sbatch job.sh",
        ),
    ):
        mock_execution_plan = MagicMock()
        mock_execution_plan.payload = ["#!/bin/bash", "echo hello"]
        mock_execution_plan.resources = {}
        mock_execution_plan.kind = "bash"

        slurm_pipes_client._execute_standalone(
            execution_plan=mock_execution_plan,
            run_dir="/tmp/test_run",
            ssh_pool=mock_ssh_pool,
            message_reader=MagicMock(),
        )

        mock_wait.assert_called_once()
        _, kwargs = mock_wait.call_args
        assert kwargs["poll_timeout"] == 3600


def test_reattach_path_forwards_poll_timeout(
    slurm_pipes_client: SlurmPipesClient,
):
    """The reattach code path in run() also forwards poll_timeout."""
    # Build a mock context that satisfies the run() method
    mock_context = MagicMock()
    mock_context.run.run_id = "test-run-id"
    mock_op_ctx = MagicMock()
    mock_context.op_execution_context = mock_op_ctx

    # Make _find_reattachable_job return a "running" job to trigger reattach
    reattach_info = {"job_id": "42", "run_dir": "/tmp/old_run"}

    with (
        patch.object(slurm_pipes_client, "_wait_for_job_with_streaming") as mock_wait,
        patch.object(slurm_pipes_client, "_execute_standalone") as mock_standalone,
        patch.object(
            slurm_pipes_client,
            "_find_reattachable_job",
            return_value=reattach_info,
        ),
        patch.object(slurm_pipes_client, "_is_job_still_running", return_value=True),
        patch.object(slurm_pipes_client, "_get_job_state", return_value="RUNNING"),
        patch.object(
            slurm_pipes_client, "_get_asset_key_string", return_value="test_asset"
        ),
        patch.object(slurm_pipes_client, "_store_job_tags"),
        patch.object(slurm_pipes_client, "_maybe_emit_final_logs"),
        patch.object(slurm_pipes_client, "_collect_and_emit_metrics"),
        patch.object(
            slurm_pipes_client,
            "_get_remote_base",
            return_value="/tmp/dagster_test",
        ),
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

        slurm_pipes_client.run(
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


def test_wait_for_job_respects_custom_poll_timeout(
    slurm_pipes_client: SlurmPipesClient,
):
    """_wait_for_job_with_streaming times out based on poll_timeout value."""
    mock_ssh_pool = MagicMock()
    # Simulate a job that is always PENDING (never completes)
    mock_ssh_pool.run.return_value = "PENDING"

    with pytest.raises(RuntimeError, match=r"Timed out after 1s"):
        slurm_pipes_client._wait_for_job_with_streaming(
            job_id=99999,
            ssh_pool=mock_ssh_pool,
            run_dir="/tmp/test_run",
            message_reader=MagicMock(),
            poll_timeout=1,
        )


def test_stream_cleanup_terminates_tail_processes_before_single_join(
    slurm_pipes_client: SlurmPipesClient,
):
    """Blocked tail readers should stop promptly and be joined only once."""
    events = []

    class BlockingTailProcess:
        def __init__(self, stream_key):
            self.stream_key = stream_key
            self.released = threading.Event()
            self.stdout = SimpleNamespace(readline=self._readline)
            self.returncode = None
            self.terminate_calls = 0

        def _readline(self):
            self.released.wait(timeout=5)
            return ""

        def poll(self):
            return self.returncode

        def terminate(self):
            self.terminate_calls += 1
            events.append(f"terminate:{self.stream_key}")
            self.returncode = -15
            self.released.set()

        def wait(self, timeout=None):
            if not self.released.wait(timeout=timeout):
                raise TimeoutError
            return self.returncode

        def kill(self):
            self.returncode = -9
            self.released.set()

    class RecordingThread:
        def __init__(self, stream_key, process):
            self.stream_key = stream_key
            self._thread = threading.Thread(target=process.stdout.readline)
            self.join_calls = 0
            self._thread.start()

        def join(self, timeout=None):
            self.join_calls += 1
            events.append(f"join:{self.stream_key}")
            self._thread.join(timeout=timeout)

        def is_alive(self):
            return self._thread.is_alive()

    stdout_process = BlockingTailProcess("stdout")
    stderr_process = BlockingTailProcess("stderr")
    stdout_thread = RecordingThread("stdout", stdout_process)
    stderr_thread = RecordingThread("stderr", stderr_process)
    stop_streaming = threading.Event()
    stream_processes = {
        "stdout": stdout_process,
        "stderr": stderr_process,
    }

    slurm_pipes_client._stop_streaming_threads(
        stop_streaming=stop_streaming,
        stream_processes=stream_processes,
        stream_processes_lock=threading.Lock(),
        stream_threads=(stdout_thread, stderr_thread),
    )

    assert stop_streaming.is_set()
    assert stdout_process.terminate_calls == 1
    assert stderr_process.terminate_calls == 1
    assert stream_processes == {}
    assert events[:2] == ["terminate:stdout", "terminate:stderr"]
    assert stdout_thread.join_calls == 1
    assert stderr_thread.join_calls == 1
    assert not stdout_thread.is_alive()
    assert not stderr_thread.is_alive()


def test_stream_cleanup_does_not_raise_for_process_errors(
    slurm_pipes_client: SlurmPipesClient,
):
    """Tail cleanup failures should not mask the Slurm job outcome."""
    process = MagicMock()
    process.poll.side_effect = OSError("process disappeared")
    process.wait.side_effect = OSError("process disappeared")
    thread = MagicMock()
    stream_processes = {"stdout": process}

    slurm_pipes_client._stop_streaming_threads(
        stop_streaming=threading.Event(),
        stream_processes=stream_processes,
        stream_processes_lock=threading.Lock(),
        stream_threads=(thread,),
    )

    assert stream_processes == {}
    thread.join.assert_called_once_with(timeout=5)


def test_get_job_state_normalizes_truncated_terminal_state(
    slurm_pipes_client: SlurmPipesClient,
):
    """sacct state truncation should still resolve to a terminal state."""
    mock_ssh_pool = MagicMock()
    mock_ssh_pool.run.side_effect = ["", "OUT_OF_ME+\n"]

    state = slurm_pipes_client._get_job_state(12345, mock_ssh_pool)

    assert state == "OUT_OF_MEMORY"


def test_validate_final_slurm_outcome_rejects_failed_batch_state(
    slurm_pipes_client: SlurmPipesClient,
):
    """Final sacct validation must reject batch-step failures after a transient success."""
    mock_ssh_pool = MagicMock()
    mock_ssh_pool.run.return_value = (
        "12345|TIMEOUT|0:0\n12345.batch|OUT_OF_ME+|0:125\n12345.extern|COMPLETED|0:0\n"
    )

    with patch.object(
        slurm_pipes_client.metrics_collector,
        "collect_job_metrics",
        return_value=SlurmJobMetrics(
            job_id=12345,
            elapsed_seconds=340.0,
            cpu_time_seconds=0.0,
            max_rss_mb=24554.0,
            node_hours=0.0,
            cpu_efficiency=0.0,
            state="OUT_OF_MEMORY",
            exit_code=0,
        ),
    ):
        with pytest.raises(RuntimeError, match=r"12345 did not complete successfully"):
            slurm_pipes_client._validate_final_slurm_outcome(12345, mock_ssh_pool)


def test_validate_final_slurm_outcome_accepts_clean_completed_job(
    slurm_pipes_client: SlurmPipesClient,
):
    """Final sacct validation should accept clean parent and batch completion."""
    mock_ssh_pool = MagicMock()
    mock_ssh_pool.run.return_value = (
        "12345|COMPLETED|0:0\n12345.batch|COMPLETED|0:0\n12345.extern|COMPLETED|0:0\n"
    )
    expected_metrics = SlurmJobMetrics(
        job_id=12345,
        elapsed_seconds=120.0,
        cpu_time_seconds=240.0,
        max_rss_mb=512.0,
        node_hours=0.033,
        cpu_efficiency=1.0,
        state="COMPLETED",
        exit_code=0,
    )

    with patch.object(
        slurm_pipes_client.metrics_collector,
        "collect_job_metrics",
        return_value=expected_metrics,
    ):
        actual_metrics = slurm_pipes_client._validate_final_slurm_outcome(
            12345, mock_ssh_pool
        )

    assert actual_metrics == expected_metrics


def test_raise_if_pipes_process_failed_raises(
    slurm_pipes_client: SlurmPipesClient,
):
    message_reader = SimpleNamespace(
        closed_exception={
            "name": "RuntimeError",
            "message": "remote boom",
        }
    )

    with pytest.raises(
        DagsterPipesExecutionError,
        match="RuntimeError: remote boom",
    ):
        slurm_pipes_client._raise_if_pipes_process_failed(message_reader, job_id=12345)


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


def test_compute_resource_forwards_poll_timeout(
    monkeypatch,
    slurm_pipes_client: SlurmPipesClient,
):
    """ComputeResource.run() forwards poll_timeout to client.run() via kwargs."""
    resource = ComputeResource(
        mode=ExecutionMode.SLURM,
        slurm=slurm_pipes_client.slurm,
        default_launcher=slurm_pipes_client.launcher,
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
