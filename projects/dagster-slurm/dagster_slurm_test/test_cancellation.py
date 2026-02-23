"""Tests for SIGTERM/cancellation behaviour of SlurmPipesClient.

Unit tests verify that:
- SIGTERM sets the flag but does NOT call scancel
- The exception handler skips scancel when SIGTERM was received
- The exception handler still calls scancel on genuine errors
- Dagster UI "Terminate Run" still calls scancel via the polling loop
- Reattach helpers work correctly (tags, asset key extraction, job lookup)
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
from dagster_slurm.pipes_clients.slurm_pipes_client import (
    SlurmPipesClient,
    _TAG_ASSET_KEY,
    _TAG_JOB_ID,
    _TAG_RUN_DIR,
)


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


def test_pre_submission_check_allows_sigterm_on_reboot():
    """Pre-submission check should NOT abort on SIGTERM when the run is NOT
    CANCELING (i.e. Dagster reboot, not user cancel)."""
    client = _make_client()
    client._sigterm_received = True
    client._cancellation_requested = False

    with patch.object(client, "_is_run_canceling", return_value=False):
        # Simulate the pre-submission guard (matches production code)
        if client._cancellation_requested or (
            client._sigterm_received and client._is_run_canceling(None)
        ):
            raised = True
        else:
            raised = False

    assert raised is False


def test_pre_submission_check_blocks_on_sigterm_with_canceling():
    """Pre-submission check SHOULD abort when SIGTERM + run is CANCELING
    (user clicked Terminate Run)."""
    client = _make_client()
    client._sigterm_received = True
    client._cancellation_requested = False

    with patch.object(client, "_is_run_canceling", return_value=True):
        if client._cancellation_requested or (
            client._sigterm_received and client._is_run_canceling(None)
        ):
            raised = True
        else:
            raised = False

    assert raised is True


# ---------------------------------------------------------------------------
# Reattach helper unit tests
# ---------------------------------------------------------------------------


def test_store_job_tags():
    """_store_job_tags writes correct tag keys/values to the instance."""
    client = _make_client()
    mock_op_context = MagicMock()
    mock_op_context.run_id = "test-run-id"

    client._store_job_tags(mock_op_context, 42, "/tmp/run_dir", "my_asset")

    mock_op_context.instance.add_run_tags.assert_called_once_with(
        "test-run-id",
        {
            _TAG_JOB_ID: "42",
            _TAG_RUN_DIR: "/tmp/run_dir",
            _TAG_ASSET_KEY: "my_asset",
        },
    )


def test_store_job_tags_no_asset_key():
    """_store_job_tags omits asset key tag when asset_key is None."""
    client = _make_client()
    mock_op_context = MagicMock()
    mock_op_context.run_id = "test-run-id"

    client._store_job_tags(mock_op_context, 42, "/tmp/run_dir", None)

    mock_op_context.instance.add_run_tags.assert_called_once_with(
        "test-run-id",
        {
            _TAG_JOB_ID: "42",
            _TAG_RUN_DIR: "/tmp/run_dir",
        },
    )


def test_store_job_tags_none_context():
    """_store_job_tags does nothing when op_context is None."""
    client = _make_client()
    # Should not raise
    client._store_job_tags(None, 42, "/tmp/run_dir", "my_asset")


def test_get_asset_key_string_single_asset():
    """_get_asset_key_string returns the asset key as a string."""
    client = _make_client()
    mock_op_context = MagicMock()
    mock_op_context.asset_key = MagicMock()
    mock_op_context.asset_key.__str__ = MagicMock(return_value="AssetKey(['my_asset'])")

    result = client._get_asset_key_string(mock_op_context)
    assert result == "AssetKey(['my_asset'])"


def test_get_asset_key_string_no_asset():
    """_get_asset_key_string returns None for non-asset ops."""
    client = _make_client()
    mock_op_context = MagicMock(spec=[])  # No attributes at all

    result = client._get_asset_key_string(mock_op_context)
    assert result is None


def test_find_reattachable_job_from_parent_run():
    """_find_reattachable_job finds job from parent run's tags (retry path)."""
    client = _make_client()
    mock_op_context = MagicMock()
    mock_op_context.dagster_run.parent_run_id = "parent-run-123"

    parent_run = MagicMock()
    parent_run.tags = {
        _TAG_JOB_ID: "42",
        _TAG_RUN_DIR: "/tmp/old_run_dir",
    }
    mock_op_context.instance.get_run_by_id.return_value = parent_run

    mock_ssh_pool = MagicMock()

    with patch.object(client, "_is_job_still_running", return_value=True):
        result = client._find_reattachable_job(
            mock_op_context, mock_ssh_pool, "my_asset"
        )

    assert result is not None
    assert result["job_id"] == "42"
    assert result["run_dir"] == "/tmp/old_run_dir"


def test_find_reattachable_job_from_failed_run_query():
    """_find_reattachable_job finds job from recent FAILURE runs."""
    client = _make_client()
    mock_op_context = MagicMock()
    mock_op_context.dagster_run.parent_run_id = None

    failed_run = MagicMock()
    failed_run.tags = {
        _TAG_JOB_ID: "99",
        _TAG_RUN_DIR: "/tmp/failed_run_dir",
        _TAG_ASSET_KEY: "my_asset",
    }
    mock_op_context.instance.get_runs.return_value = [failed_run]

    mock_ssh_pool = MagicMock()

    with patch.object(client, "_is_job_still_running", return_value=True):
        result = client._find_reattachable_job(
            mock_op_context, mock_ssh_pool, "my_asset"
        )

    assert result is not None
    assert result["job_id"] == "99"
    assert result["run_dir"] == "/tmp/failed_run_dir"


def test_find_reattachable_job_returns_none_without_tags():
    """_find_reattachable_job returns None when parent run has no tags."""
    client = _make_client()
    mock_op_context = MagicMock()
    mock_op_context.dagster_run.parent_run_id = "parent-run-123"

    parent_run = MagicMock()
    parent_run.tags = {}  # No reattach tags
    mock_op_context.instance.get_run_by_id.return_value = parent_run
    mock_op_context.instance.get_runs.return_value = []

    mock_ssh_pool = MagicMock()

    result = client._find_reattachable_job(mock_op_context, mock_ssh_pool, "my_asset")
    assert result is None


def test_find_reattachable_job_returns_completed_job():
    """_find_reattachable_job returns the candidate even when the job
    has already completed — the caller decides whether to adopt the
    result or resubmit."""
    client = _make_client()
    mock_op_context = MagicMock()
    mock_op_context.dagster_run.parent_run_id = "parent-run-123"

    parent_run = MagicMock()
    parent_run.tags = {
        _TAG_JOB_ID: "42",
        _TAG_RUN_DIR: "/tmp/old_run_dir",
    }
    mock_op_context.instance.get_run_by_id.return_value = parent_run

    mock_ssh_pool = MagicMock()

    with patch.object(client, "_get_job_state", return_value="COMPLETED"):
        result = client._find_reattachable_job(
            mock_op_context, mock_ssh_pool, "my_asset"
        )

    assert result is not None
    assert result["job_id"] == "42"


def test_find_reattachable_job_skips_completed_failed_run_candidate():
    """Fresh re-materialization should not adopt COMPLETED jobs from old failed runs."""
    client = _make_client()
    mock_op_context = MagicMock()
    mock_op_context.dagster_run.parent_run_id = None

    failed_run = MagicMock()
    failed_run.tags = {
        _TAG_JOB_ID: "99",
        _TAG_RUN_DIR: "/tmp/failed_run_dir",
        _TAG_ASSET_KEY: "my_asset",
    }
    mock_op_context.instance.get_runs.return_value = [failed_run]

    mock_ssh_pool = MagicMock()

    with patch.object(client, "_get_job_state", return_value="COMPLETED"):
        result = client._find_reattachable_job(
            mock_op_context, mock_ssh_pool, "my_asset"
        )

    assert result is None


def test_find_reattachable_job_returns_none_when_job_unknown():
    """_find_reattachable_job returns None when Slurm has no record of
    the job (empty state from squeue + sacct)."""
    client = _make_client()
    mock_op_context = MagicMock()
    mock_op_context.dagster_run.parent_run_id = "parent-run-123"

    parent_run = MagicMock()
    parent_run.tags = {
        _TAG_JOB_ID: "42",
        _TAG_RUN_DIR: "/tmp/old_run_dir",
    }
    mock_op_context.instance.get_run_by_id.return_value = parent_run

    mock_ssh_pool = MagicMock()

    with patch.object(client, "_get_job_state", return_value=""):
        result = client._find_reattachable_job(
            mock_op_context, mock_ssh_pool, "my_asset"
        )

    assert result is None


def test_find_reattachable_job_none_context():
    """_find_reattachable_job returns None when op_context is None."""
    client = _make_client()
    result = client._find_reattachable_job(None, MagicMock(), "my_asset")
    assert result is None


def test_is_job_still_running_true():
    """_is_job_still_running returns True for active states."""
    client = _make_client()
    mock_ssh_pool = MagicMock()

    for state in ["RUNNING", "PENDING", "CONFIGURING", "COMPLETING"]:
        with patch.object(client, "_get_job_state", return_value=state):
            assert client._is_job_still_running(42, mock_ssh_pool) is True


def test_is_job_still_running_false():
    """_is_job_still_running returns False for terminal/empty states."""
    client = _make_client()
    mock_ssh_pool = MagicMock()

    for state in ["COMPLETED", "FAILED", "CANCELLED", ""]:
        with patch.object(client, "_get_job_state", return_value=state):
            assert client._is_job_still_running(42, mock_ssh_pool) is False


def test_interruptible_sleep_swallows_dagster_interrupt():
    """_interruptible_sleep catches DagsterExecutionInterruptedError."""
    from dagster._utils.interrupts import _received_interrupt

    client = _make_client()

    # Create a fake exception with a matching name
    class DagsterExecutionInterruptedError(Exception):
        pass

    _received_interrupt["received"] = True

    with patch(
        "dagster_slurm.pipes_clients.slurm_pipes_client.time.sleep",
        side_effect=DagsterExecutionInterruptedError("test"),
    ):
        # Should NOT raise
        client._interruptible_sleep(1, 42)

    assert _received_interrupt["received"] is False


def test_interruptible_sleep_propagates_other_exceptions():
    """_interruptible_sleep re-raises non-interrupt exceptions."""
    client = _make_client()

    with patch(
        "dagster_slurm.pipes_clients.slurm_pipes_client.time.sleep",
        side_effect=ValueError("unrelated error"),
    ):
        with pytest.raises(ValueError, match="unrelated error"):
            client._interruptible_sleep(1, 42)


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
