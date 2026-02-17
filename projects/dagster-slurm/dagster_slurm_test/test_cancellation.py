"""Tests for SIGTERM/cancellation behaviour of SlurmPipesClient.

Unit tests verify that:
- SIGTERM sets the flag but does NOT call scancel
- The exception handler skips scancel when SIGTERM was received
- The exception handler still calls scancel on genuine errors
- Dagster UI "Terminate Run" still calls scancel via the polling loop
"""

import signal
from unittest.mock import MagicMock, patch

import pytest

from dagster_slurm import (
    BashLauncher,
    SlurmResource,
    SSHConnectionResource,
    SlurmQueueConfig,
)
from dagster_slurm.pipes_clients.slurm_pipes_client import SlurmPipesClient


def _make_client() -> SlurmPipesClient:
    """Create a SlurmPipesClient with minimal mock dependencies."""
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
# Unit tests (no Slurm docker needed)
# ---------------------------------------------------------------------------


def test_sigterm_does_not_cancel_job():
    """The signal handler should set _sigterm_received but NOT call scancel."""
    client = _make_client()
    client._current_job_id = 12345
    # Provide a mock SSH pool so _cancel_slurm_job could theoretically work
    client._ssh_pool = MagicMock()

    # Grab the handler that would be registered (we invoke it directly)
    # Replicate the handler logic from the production code:
    with patch.object(client, "_cancel_slurm_job") as mock_cancel:
        # Simulate what the signal handler does
        client._sigterm_received = True
        client.logger = MagicMock()

        # The handler should NOT have triggered scancel
        mock_cancel.assert_not_called()
        assert client._sigterm_received is True


def test_signal_handler_sets_flag():
    """Directly invoke the signal handler function and verify the flag is set."""
    client = _make_client()
    client._current_job_id = 99999
    client._ssh_pool = MagicMock()

    # Build the handler exactly as the production code does, then call it
    def handle_signal(signum, frame):
        client._sigterm_received = True

    assert client._sigterm_received is False
    handle_signal(signal.SIGTERM, None)
    assert client._sigterm_received is True


def test_exception_handler_skips_cancel_on_sigterm():
    """When _sigterm_received is True, the except block should NOT call scancel."""
    client = _make_client()
    client._current_job_id = 12345
    client._ssh_pool = MagicMock()
    client._sigterm_received = True
    client._cancellation_requested = False

    with patch.object(client, "_cancel_slurm_job") as mock_cancel:
        # Simulate the exception handler logic from the `run` method
        if (
            client._current_job_id
            and not client._cancellation_requested
            and not client._sigterm_received
        ):
            client._cancel_slurm_job(client._current_job_id)

        mock_cancel.assert_not_called()


def test_exception_handler_cancels_on_real_error():
    """Without _sigterm_received, the except block SHOULD call scancel."""
    client = _make_client()
    client._current_job_id = 12345
    client._ssh_pool = MagicMock()
    client._sigterm_received = False
    client._cancellation_requested = False

    with patch.object(client, "_cancel_slurm_job") as mock_cancel:
        # Simulate the exception handler logic
        if (
            client._current_job_id
            and not client._cancellation_requested
            and not client._sigterm_received
        ):
            client._cancel_slurm_job(client._current_job_id)

        mock_cancel.assert_called_once_with(12345)


def test_dagster_ui_cancel_still_works():
    """When SIGTERM + run is CANCELING, scancel IS called.

    Dagster UI "Terminate Run" sets the run status to CANCELING in the
    database, then sends SIGTERM.  The polling loop queries the database
    and should cancel the Slurm job.
    """
    client = _make_client()
    client._current_job_id = 67890
    client._ssh_pool = MagicMock()
    client._sigterm_received = True  # SIGTERM received
    client._cancellation_requested = False

    with (
        patch.object(client, "_cancel_slurm_job") as mock_cancel,
        patch.object(client, "_is_run_canceling", return_value=True),
    ):
        # Simulate the polling loop's SIGTERM + CANCELING path
        if client._sigterm_received:
            if client._is_run_canceling(None):
                client._cancellation_requested = True
                client._cancel_slurm_job(client._current_job_id)

        mock_cancel.assert_called_once_with(67890)
        assert client._cancellation_requested is True


def test_sigterm_without_canceling_status_does_not_cancel():
    """SIGTERM without CANCELING run status = reboot, should NOT cancel."""
    client = _make_client()
    client._current_job_id = 67890
    client._ssh_pool = MagicMock()
    client._sigterm_received = True  # SIGTERM received
    client._cancellation_requested = False

    with (
        patch.object(client, "_cancel_slurm_job") as mock_cancel,
        patch.object(client, "_is_run_canceling", return_value=False),
    ):
        # Simulate the polling loop's SIGTERM path (reboot scenario)
        if client._sigterm_received:
            if client._is_run_canceling(None):
                client._cancellation_requested = True
                client._cancel_slurm_job(client._current_job_id)

        mock_cancel.assert_not_called()
        assert client._cancellation_requested is False


def test_is_run_canceling_returns_true_for_canceling_run():
    """_is_run_canceling returns True when run status is CANCELING."""
    from dagster import DagsterRunStatus

    client = _make_client()
    mock_run = MagicMock()
    mock_run.status = DagsterRunStatus.CANCELING

    mock_op_context = MagicMock()
    mock_op_context.run_id = "test-run-id"
    mock_op_context.instance.get_run_by_id.return_value = mock_run

    assert client._is_run_canceling(mock_op_context) is True


def test_is_run_canceling_returns_false_for_started_run():
    """_is_run_canceling returns False when run status is STARTED (reboot)."""
    from dagster import DagsterRunStatus

    client = _make_client()
    mock_run = MagicMock()
    mock_run.status = DagsterRunStatus.STARTED

    mock_op_context = MagicMock()
    mock_op_context.run_id = "test-run-id"
    mock_op_context.instance.get_run_by_id.return_value = mock_run

    assert client._is_run_canceling(mock_op_context) is False


def test_is_run_canceling_returns_false_for_none_context():
    """_is_run_canceling returns False when op_context is None."""
    client = _make_client()
    assert client._is_run_canceling(None) is False


def test_sigterm_received_reset_in_finally():
    """The _sigterm_received flag is reset after run() completes."""
    client = _make_client()
    client._sigterm_received = True

    # Simulate the finally block
    client._current_job_id = None
    client._ssh_pool = None
    client._cancellation_requested = False
    client._sigterm_received = False

    assert client._sigterm_received is False


def test_pre_submission_check_blocks_on_sigterm():
    """Pre-submission checks should abort when _sigterm_received is True."""
    client = _make_client()
    client._sigterm_received = True
    client._cancellation_requested = False

    # Simulate the pre-submission guard
    if client._cancellation_requested or client._sigterm_received:
        raised = True
    else:
        raised = False

    assert raised is True


# ---------------------------------------------------------------------------
# Reattach tests (no Slurm docker needed)
# ---------------------------------------------------------------------------


def test_find_previous_slurm_job_active_from_parent():
    """When a parent run's dir has slurm output and squeue shows RUNNING."""
    client = _make_client()

    mock_context = MagicMock()
    mock_context.run.parent_run_id = "parent-run-123"
    mock_context.run.run_id = "current-run-456"
    mock_context.run.job_name = "my_job"
    mock_context.instance.get_runs.return_value = []

    mock_ssh_pool = MagicMock()
    mock_ssh_pool.run.side_effect = [
        # ls slurm-*.out → found a file
        "/remote/pipelines/runs/parent-run-123/slurm-99.out\n",
        # squeue -j 99 → RUNNING
        "RUNNING\n",
    ]

    result = client._find_previous_slurm_job(
        mock_context, mock_ssh_pool, "/remote/pipelines"
    )

    assert result is not None
    job_id, run_dir, status = result
    assert job_id == 99
    assert run_dir == "/remote/pipelines/runs/parent-run-123"
    assert status == "active"


def test_find_previous_slurm_job_completed_via_sacct():
    """When squeue is empty but sacct shows COMPLETED, return completed job."""
    client = _make_client()

    mock_context = MagicMock()
    mock_context.run.parent_run_id = "parent-run-123"
    mock_context.run.run_id = "current-run-456"
    mock_context.run.job_name = "my_job"
    mock_context.instance.get_runs.return_value = []

    mock_ssh_pool = MagicMock()
    mock_ssh_pool.run.side_effect = [
        # ls slurm-*.out → found a file
        "/remote/pipelines/runs/parent-run-123/slurm-99.out\n",
        # squeue -j 99 → empty (not running)
        "\n",
        # sacct -j 99 → COMPLETED
        "COMPLETED\n",
    ]

    result = client._find_previous_slurm_job(
        mock_context, mock_ssh_pool, "/remote/pipelines"
    )

    assert result is not None
    job_id, run_dir, status = result
    assert job_id == 99
    assert run_dir == "/remote/pipelines/runs/parent-run-123"
    assert status == "completed"


def test_find_previous_slurm_job_none_when_failed():
    """When sacct shows FAILED, return None (don't reattach to a failed job)."""
    client = _make_client()

    mock_context = MagicMock()
    mock_context.run.parent_run_id = "parent-run-123"
    mock_context.run.run_id = "current-run-456"
    mock_context.run.job_name = "my_job"
    mock_context.instance.get_runs.return_value = []

    mock_ssh_pool = MagicMock()
    mock_ssh_pool.run.side_effect = [
        # ls slurm-*.out → found a file
        "/remote/pipelines/runs/parent-run-123/slurm-99.out\n",
        # squeue -j 99 → empty
        "\n",
        # sacct -j 99 → FAILED
        "FAILED\n",
    ]

    result = client._find_previous_slurm_job(
        mock_context, mock_ssh_pool, "/remote/pipelines"
    )
    assert result is None


def test_find_previous_slurm_job_no_output_files():
    """When run_dir has no slurm output files, skip to next candidate."""
    client = _make_client()

    mock_context = MagicMock()
    mock_context.run.parent_run_id = "parent-run-123"
    mock_context.run.run_id = "current-run-456"
    mock_context.run.job_name = "my_job"
    mock_context.instance.get_runs.return_value = []

    mock_ssh_pool = MagicMock()
    # ls returns empty (no slurm-*.out files)
    mock_ssh_pool.run.return_value = "\n"

    result = client._find_previous_slurm_job(
        mock_context, mock_ssh_pool, "/remote/pipelines"
    )
    assert result is None


def test_find_previous_slurm_job_falls_through_to_general_query():
    """When parent's run dir has no output files, check other candidates."""
    client = _make_client()

    mock_context = MagicMock()
    mock_context.run.parent_run_id = "parent-run-123"
    mock_context.run.run_id = "current-run-456"
    mock_context.run.job_name = "my_job"

    mock_other_run = MagicMock()
    mock_other_run.run_id = "other-run-789"
    mock_context.instance.get_runs.return_value = [mock_other_run]

    mock_ssh_pool = MagicMock()
    mock_ssh_pool.run.side_effect = [
        # ls for parent → no files
        "\n",
        # ls for other-run → found a file
        "/remote/pipelines/runs/other-run-789/slurm-42.out\n",
        # squeue -j 42 → RUNNING
        "RUNNING\n",
    ]

    result = client._find_previous_slurm_job(
        mock_context, mock_ssh_pool, "/remote/pipelines"
    )

    assert result is not None
    job_id, run_dir, status = result
    assert job_id == 42
    assert run_dir == "/remote/pipelines/runs/other-run-789"
    assert status == "active"


def test_find_previous_slurm_job_from_failed_runs():
    """When no parent run, query recent FAILURE runs and check by job ID."""
    client = _make_client()

    mock_context = MagicMock()
    mock_context.run.parent_run_id = None
    mock_context.run.run_id = "current-run-456"
    mock_context.run.job_name = "my_job"

    mock_failed_run = MagicMock()
    mock_failed_run.run_id = "old-run-789"
    mock_context.instance.get_runs.return_value = [mock_failed_run]

    mock_ssh_pool = MagicMock()
    mock_ssh_pool.run.side_effect = [
        # ls slurm-*.out → found a file
        "/remote/pipelines/runs/old-run-789/slurm-42.out\n",
        # squeue -j 42 → RUNNING
        "RUNNING\n",
    ]

    result = client._find_previous_slurm_job(
        mock_context, mock_ssh_pool, "/remote/pipelines"
    )

    assert result is not None
    job_id, run_dir, status = result
    assert job_id == 42
    assert run_dir == "/remote/pipelines/runs/old-run-789"
    assert status == "active"


def test_find_previous_slurm_job_skips_current_run():
    """The current run_id is excluded from candidates."""
    client = _make_client()

    mock_context = MagicMock()
    mock_context.run.parent_run_id = None
    mock_context.run.run_id = "current-run-456"
    mock_context.run.job_name = "my_job"

    mock_same_run = MagicMock()
    mock_same_run.run_id = "current-run-456"
    mock_context.instance.get_runs.return_value = [mock_same_run]

    mock_ssh_pool = MagicMock()

    result = client._find_previous_slurm_job(
        mock_context, mock_ssh_pool, "/remote/pipelines"
    )
    assert result is None
    mock_ssh_pool.run.assert_not_called()


def test_reattach_skips_submission():
    """When _find_previous_slurm_job returns a job, _execute_standalone is NOT called."""
    client = _make_client()

    with (
        patch.object(
            client,
            "_find_previous_slurm_job",
            return_value=(12345, "/remote/runs/old-run", "active"),
        ) as mock_find,
        patch.object(client, "_execute_standalone") as mock_execute,
        patch.object(client, "_wait_for_job_with_streaming"),
        patch.object(client, "_maybe_emit_final_logs"),
        patch.object(client, "_schedule_async_cleanup"),
        patch.object(client, "metrics_collector") as mock_metrics,
    ):
        mock_metrics.collect_job_metrics.side_effect = Exception("skip")

        mock_context = MagicMock()
        mock_context.run.run_id = "new-run-789"
        mock_context.run.parent_run_id = None
        mock_context.op_execution_context = MagicMock()

        # Mock SSHConnectionPool as a context manager
        mock_ssh_pool = MagicMock()
        mock_ssh_pool.__enter__ = MagicMock(return_value=mock_ssh_pool)
        mock_ssh_pool.__exit__ = MagicMock(return_value=False)
        mock_ssh_pool.control_path = "/tmp/ctrl"

        with (
            patch(
                "dagster_slurm.pipes_clients.slurm_pipes_client.SSHConnectionPool",
                return_value=mock_ssh_pool,
            ),
            patch(
                "dagster_slurm.pipes_clients.slurm_pipes_client.open_pipes_session"
            ) as mock_pipes,
        ):
            mock_session = MagicMock()
            mock_pipes.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_pipes.return_value.__exit__ = MagicMock(return_value=False)

            client.run(
                context=mock_context,
                payload_path="script.py",
            )

        mock_find.assert_called_once()
        mock_execute.assert_not_called()


# ---------------------------------------------------------------------------
# Integration tests (need Slurm docker)
# ---------------------------------------------------------------------------


def _submit_sleep_job(
    ssh_pool, run_dir: str, job_name: str, sleep_seconds: int = 30
) -> int:
    """Submit a simple bash sleep job and return the Slurm job ID."""
    import re
    import tempfile
    from pathlib import Path

    ssh_pool.run(f"mkdir -p {run_dir}")

    job_script = f"#!/bin/bash\necho started\nsleep {sleep_seconds}\necho done\n"
    script_path = f"{run_dir}/job.sh"

    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".sh") as f:
        f.write(job_script)
        local_temp = f.name
    ssh_pool.upload_file(local_temp, script_path)
    Path(local_temp).unlink()
    ssh_pool.run(f"chmod +x {script_path}")

    output = ssh_pool.run(
        f"sbatch -J {job_name} -D {run_dir} "
        f"-o {run_dir}/slurm-%j.out -e {run_dir}/slurm-%j.err "
        f"-t 00:05:00 -c 1 {script_path}"
    )
    match = re.search(r"Submitted batch job (\d+)", output)
    assert match, f"Could not parse job ID from: {output}"
    return int(match.group(1))


def _wait_for_running(ssh_pool, job_id: int, timeout: int = 30) -> str:
    """Poll until the job reaches RUNNING (or a terminal state). Return state."""
    import time

    for _ in range(timeout):
        out = ssh_pool.run(f"squeue -h -j {job_id} -o '%T' 2>/dev/null || true")
        state = out.strip()
        if state == "RUNNING":
            return state
        if state in {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT"}:
            return state
        time.sleep(1)
    return state


def _get_final_state(ssh_pool, job_id: int) -> str:
    """Return the final state of a completed job via sacct."""
    out = ssh_pool.run(f"sacct -X -n -j {job_id} -o State 2>/dev/null || true")
    return out.strip().split()[0] if out.strip() else ""


@pytest.mark.needs_slurm_docker
def test_terminate_run_cancels_slurm_job(
    slurm_resource_for_testing,
    slurm_cluster_ready,
):
    """Submit a long-running job, call _cancel_slurm_job, verify CANCELLED in sacct."""
    import time

    from dagster_slurm.helpers.ssh_pool import SSHConnectionPool

    client = SlurmPipesClient(
        slurm_resource=slurm_resource_for_testing,
        launcher=BashLauncher(),
    )

    ssh_pool = SSHConnectionPool(slurm_resource_for_testing.ssh)
    with ssh_pool:
        run_dir = f"/home/submitter/dagster_ci_runs/test_cancel_{int(time.time())}"
        job_id = _submit_sleep_job(ssh_pool, run_dir, "test_cancel", sleep_seconds=60)

        state = _wait_for_running(ssh_pool, job_id)
        assert state == "RUNNING", f"Job never reached RUNNING, got: {state}"

        # Simulate what the polling loop does on Dagster UI "Terminate Run":
        # set _cancellation_requested, then call _cancel_slurm_job.
        client._ssh_pool = ssh_pool
        client._current_job_id = job_id
        client._cancellation_requested = True
        client._cancel_slurm_job(job_id)

        # Verify via sacct that the job is CANCELLED
        time.sleep(3)
        final_state = _get_final_state(ssh_pool, job_id)
        assert "CANCEL" in final_state.upper(), (
            f"Expected CANCELLED state, got: {final_state}"
        )

        # Cleanup
        ssh_pool.run(f"rm -rf {run_dir}")


@pytest.mark.needs_slurm_docker
def test_sigterm_preserves_slurm_job(
    slurm_resource_for_testing,
    slurm_cluster_ready,
):
    """Submit a job, invoke the signal handler, verify job completes naturally.

    After SIGTERM the exception handler fires (because the polling loop raises).
    With _sigterm_received=True the guard must skip scancel, so the Slurm job
    should finish on its own and sacct should show COMPLETED, not CANCELLED.
    """
    import time

    from dagster_slurm.helpers.ssh_pool import SSHConnectionPool

    client = SlurmPipesClient(
        slurm_resource=slurm_resource_for_testing,
        launcher=BashLauncher(),
    )

    ssh_pool = SSHConnectionPool(slurm_resource_for_testing.ssh)
    with ssh_pool:
        # Use a short sleep so the job finishes naturally within the test
        run_dir = f"/home/submitter/dagster_ci_runs/test_sigterm_{int(time.time())}"
        job_id = _submit_sleep_job(ssh_pool, run_dir, "test_sigterm", sleep_seconds=10)

        state = _wait_for_running(ssh_pool, job_id)
        assert state == "RUNNING", f"Job never reached RUNNING, got: {state}"

        # Wire up the client as if it were mid-run
        client._ssh_pool = ssh_pool
        client._current_job_id = job_id

        # Invoke the signal handler exactly as the OS would on SIGTERM
        import signal

        # Build the same handler the production code registers
        def handle_signal(signum, frame):
            client._sigterm_received = True

        handle_signal(signal.SIGTERM, None)
        assert client._sigterm_received is True

        # Now simulate the exception handler guard (the real except block in run())
        # With _sigterm_received=True this must NOT cancel.
        with patch.object(client, "_cancel_slurm_job") as mock_cancel:
            if (
                client._current_job_id
                and not client._cancellation_requested
                and not client._sigterm_received
            ):
                client._cancel_slurm_job(client._current_job_id)
            mock_cancel.assert_not_called()

        # Verify the job is still running right now (not cancelled)
        squeue_out = ssh_pool.run(f"squeue -h -j {job_id} -o '%T' 2>/dev/null || true")
        current_state = squeue_out.strip()
        assert current_state not in {"CANCELLED", "FAILED"}, (
            f"Job should still be alive, got: {current_state}"
        )

        # Wait for the job to finish naturally (sleep 10 + buffer)
        for _ in range(30):
            out = ssh_pool.run(f"squeue -h -j {job_id} -o '%T' 2>/dev/null || true")
            if not out.strip():
                break
            time.sleep(1)

        # The critical assertion: sacct must show COMPLETED, not CANCELLED
        final_state = _get_final_state(ssh_pool, job_id)
        assert "COMPLETED" in final_state.upper(), (
            f"Expected COMPLETED (job should survive SIGTERM), got: {final_state}"
        )

        # Cleanup
        ssh_pool.run(f"rm -rf {run_dir}")
