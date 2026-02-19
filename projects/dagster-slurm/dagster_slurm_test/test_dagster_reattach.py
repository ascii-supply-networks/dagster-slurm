"""Dagster-level integration tests for SIGTERM behaviour and reattach.

These tests use ``dagster.materialize()`` with real DagsterInstance /
AssetExecutionContext.  Only the SSH layer (``SSHConnectionPool``) is
mocked so no Docker cluster is needed.

Crucially, ``_execute_standalone`` and ``_wait_for_job_with_streaming``
are NOT mocked – the real code paths execute so that the polling loop's
SIGTERM handling is fully exercised.
"""

import threading
from unittest.mock import MagicMock, patch

from dagster import (
    AssetExecutionContext,
    DagsterInstance,
    asset,
    materialize,
)

from dagster_slurm import (
    BashLauncher,
    ComputeResource,
    SlurmQueueConfig,
    SlurmResource,
    SSHConnectionResource,
)
from dagster_slurm.config.environment import ExecutionMode
from dagster_slurm.pipes_clients.slurm_pipes_client import (
    SlurmPipesClient,
    _TAG_JOB_ID,
    _TAG_RUN_DIR,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_slurm_compute() -> ComputeResource:
    """ComputeResource in SLURM mode with a dummy SSH target."""
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
    return ComputeResource(
        mode=ExecutionMode.SLURM,
        default_launcher=BashLauncher(),
        slurm=slurm,
        cleanup_on_failure=False,
    )


def _mock_ssh_pool():
    """Create a mock SSHConnectionPool that behaves as a context manager."""
    pool = MagicMock()
    pool.__enter__ = MagicMock(return_value=pool)
    pool.__exit__ = MagicMock(return_value=False)
    pool.control_path = "/tmp/ctrl"
    return pool


class _SshRouter:
    """Route ``ssh_pool.run(cmd)`` calls to appropriate fake responses.

    This lets the real ``_execute_standalone`` and
    ``_wait_for_job_with_streaming`` run against fake SSH output.
    """

    def __init__(self):
        self.submitted_job_id = 42
        # Track poll count to fire SIGTERM at the right time
        self._poll_count = 0
        self._sigterm_target: SlurmPipesClient | None = None
        self._sigterm_after_polls = 1  # fire SIGTERM after first poll
        # After this many polls the job transitions to COMPLETED
        self._complete_after_polls = 3

    def __call__(self, cmd: str) -> str:
        # Remote base / home detection
        if cmd.startswith("echo $HOME"):
            return "/tmp/dagster_test"

        # mkdir
        if cmd.startswith("mkdir"):
            return ""

        # File upload verification (test -f ... && wc -l)
        if "test -f" in cmd and "wc -l" in cmd:
            return "10 /tmp/job.sh"

        # chmod
        if cmd.startswith("chmod"):
            return ""

        # sbatch submission
        if "sbatch" in cmd:
            return f"Submitted batch job {self.submitted_job_id}"

        # squeue poll (job state check)
        if "squeue" in cmd and f"-j {self.submitted_job_id}" in cmd:
            self._poll_count += 1
            if self._sigterm_target and self._poll_count >= self._sigterm_after_polls:
                self._sigterm_target._sigterm_received = True
            # After enough polls the job disappears from squeue (completed)
            if self._poll_count >= self._complete_after_polls:
                return ""
            return "RUNNING"

        # sacct (fallback state check) — once job is done, report COMPLETED
        if "sacct" in cmd:
            if self._poll_count >= self._complete_after_polls:
                return "COMPLETED"
            return ""

        # scontrol
        if "scontrol" in cmd:
            return ""

        # Log streaming tail fallback (polling)
        if "tail -n +" in cmd:
            return ""

        # cat stderr for failed jobs
        if "cat" in cmd:
            return ""

        # Catch-all
        return ""


def _failure_events(result):
    """Extract failure messages from a materialize result for diagnostics."""
    events = []
    for event in result.all_events:
        if hasattr(event, "event_type_value") and "FAILURE" in str(
            event.event_type_value
        ):
            msg = getattr(event, "message", str(event))
            events.append(msg)
    return f"Materialization failed: {events}"


# ---------------------------------------------------------------------------
# Test: SIGTERM during monitoring continues polling (reboot case)
# ---------------------------------------------------------------------------


def test_sigterm_during_monitoring_continues_polling():
    """Exercise the REAL _execute_standalone → _wait_for_job_with_streaming
    code path.  Simulate SIGTERM during monitoring and verify that the
    polling loop continues (instead of raising) and the run succeeds
    once the job completes.

    This test does NOT mock _execute_standalone or _wait_for_job_with_streaming.
    Only the SSH layer and env preparation are mocked.
    """

    compute = _make_slurm_compute()
    mock_pool = _mock_ssh_pool()
    router = _SshRouter()
    # SIGTERM fires after poll 1, job completes after poll 3
    router._sigterm_after_polls = 1
    router._complete_after_polls = 3
    mock_pool.run.side_effect = router

    sigterm_logged = {}

    @asset
    def monitor_asset(context: AssetExecutionContext, compute: ComputeResource):
        compute.run(
            context=context,
            payload_path="script.py",
        )

    # Intercept logger.info to verify the "continuing to monitor" message
    _original_info: list = []

    def capturing_info(msg, *args, **kwargs):
        if "continuing to monitor" in str(msg):
            sigterm_logged["seen"] = True
        _original_info[0](msg, *args, **kwargs)

    with (
        patch(
            "dagster_slurm.pipes_clients.slurm_pipes_client.SSHConnectionPool",
            return_value=mock_pool,
        ),
        patch.object(
            SlurmPipesClient,
            "_prepare_environment",
            return_value=("/activate.sh", "/usr/bin/python"),
        ),
        patch.object(
            SlurmPipesClient,
            "_build_tail_command",
            return_value=None,
        ),
        patch(
            "dagster_slurm.pipes_clients.slurm_pipes_client.open_pipes_session"
        ) as mock_pipes,
        patch("dagster_slurm.pipes_clients.slurm_pipes_client.time.sleep"),
    ):
        mock_session = MagicMock()
        mock_pipes.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_pipes.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.get_bootstrap_env_vars.return_value = {}

        original_init = SlurmPipesClient.__init__

        def capturing_init(self, *a, **kw):
            original_init(self, *a, **kw)
            router._sigterm_target = self
            _original_info.append(self.logger.info)
            self.logger.info = capturing_info

        with patch.object(SlurmPipesClient, "__init__", capturing_init):
            result = materialize(
                [monitor_asset],
                resources={"compute": compute},
                raise_on_error=False,
            )

    # The run should SUCCEED — SIGTERM did not interrupt the polling loop
    assert result.success, _failure_events(result)
    # The "continuing to monitor" log message was emitted
    assert sigterm_logged.get("seen"), (
        "Expected 'continuing to monitor' log after SIGTERM"
    )


# ---------------------------------------------------------------------------
# Test: pre-submission SIGTERM + CANCELING aborts
# ---------------------------------------------------------------------------


def test_sigterm_with_canceling_status_aborts_submission():
    """When SIGTERM is received AND the run is CANCELING (user clicked
    Terminate), submission should be aborted."""

    compute = _make_slurm_compute()
    mock_pool = _mock_ssh_pool()

    @asset
    def cancel_asset(context: AssetExecutionContext, compute: ComputeResource):
        compute.run(
            context=context,
            payload_path="script.py",
        )

    def fake_prepare_environment(self, **kwargs):
        self._sigterm_received = True
        return ("/activate.sh", "/usr/bin/python")

    with (
        patch(
            "dagster_slurm.pipes_clients.slurm_pipes_client.SSHConnectionPool",
            return_value=mock_pool,
        ),
        patch.object(
            SlurmPipesClient,
            "_prepare_environment",
            fake_prepare_environment,
        ),
        patch.object(
            SlurmPipesClient,
            "_is_run_canceling",
            return_value=True,
        ),
        patch.object(
            SlurmPipesClient,
            "_execute_standalone",
            return_value=42,
        ) as mock_exec,
        patch.object(SlurmPipesClient, "_maybe_emit_final_logs"),
        patch.object(SlurmPipesClient, "_schedule_async_cleanup"),
        patch(
            "dagster_slurm.pipes_clients.slurm_pipes_client.open_pipes_session"
        ) as mock_pipes,
    ):
        mock_session = MagicMock()
        mock_pipes.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_pipes.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.get_bootstrap_env_vars.return_value = {}

        mock_pool.run.return_value = "/tmp/dagster_test"

        result = materialize(
            [cancel_asset],
            resources={"compute": compute},
            raise_on_error=False,
        )

    assert not result.success
    mock_exec.assert_not_called()


# ---------------------------------------------------------------------------
# Test: SIGTERM during env prep allows submission (reboot case)
# ---------------------------------------------------------------------------


def test_sigterm_during_env_prep_allows_submission():
    """When SIGTERM arrives during _prepare_environment (reboot, not
    user cancel), submission should still proceed."""

    compute = _make_slurm_compute()
    mock_pool = _mock_ssh_pool()
    sigterm_fired = threading.Event()

    @asset
    def reboot_asset(context: AssetExecutionContext, compute: ComputeResource):
        compute.run(
            context=context,
            payload_path="script.py",
        )

    def fake_prepare_environment(self, **kwargs):
        self._sigterm_received = True
        sigterm_fired.set()
        return ("/activate.sh", "/usr/bin/python")

    with (
        patch(
            "dagster_slurm.pipes_clients.slurm_pipes_client.SSHConnectionPool",
            return_value=mock_pool,
        ),
        patch.object(
            SlurmPipesClient,
            "_prepare_environment",
            fake_prepare_environment,
        ),
        patch.object(
            SlurmPipesClient,
            "_execute_standalone",
            return_value=42,
        ) as mock_exec,
        patch.object(SlurmPipesClient, "_maybe_emit_final_logs"),
        patch.object(SlurmPipesClient, "_schedule_async_cleanup"),
        patch(
            "dagster_slurm.pipes_clients.slurm_pipes_client.open_pipes_session"
        ) as mock_pipes,
    ):
        mock_session = MagicMock()
        mock_pipes.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_pipes.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.get_bootstrap_env_vars.return_value = {}

        mock_pool.run.return_value = "/tmp/dagster_test"

        materialize(
            [reboot_asset],
            resources={"compute": compute},
            raise_on_error=False,
        )

    assert sigterm_fired.is_set(), "SIGTERM was not simulated"
    mock_exec.assert_called_once()


# ---------------------------------------------------------------------------
# Test: Tags are stored after sbatch
# ---------------------------------------------------------------------------


def test_tags_stored_after_sbatch():
    """After a successful materialize, run tags should include job_id and
    run_dir for reattach support."""

    compute = _make_slurm_compute()
    mock_pool = _mock_ssh_pool()
    router = _SshRouter()
    # No SIGTERM, job completes after 3 polls
    router._sigterm_after_polls = 999
    router._complete_after_polls = 3
    mock_pool.run.side_effect = router

    @asset
    def tag_asset(context: AssetExecutionContext, compute: ComputeResource):
        compute.run(
            context=context,
            payload_path="script.py",
        )

    with DagsterInstance.ephemeral() as instance:
        with (
            patch(
                "dagster_slurm.pipes_clients.slurm_pipes_client.SSHConnectionPool",
                return_value=mock_pool,
            ),
            patch.object(
                SlurmPipesClient,
                "_prepare_environment",
                return_value=("/activate.sh", "/usr/bin/python"),
            ),
            patch.object(
                SlurmPipesClient,
                "_build_tail_command",
                return_value=None,
            ),
            patch.object(
                SlurmPipesClient,
                "_find_reattachable_job",
                return_value=None,
            ),
            patch(
                "dagster_slurm.pipes_clients.slurm_pipes_client.open_pipes_session"
            ) as mock_pipes,
            patch("dagster_slurm.pipes_clients.slurm_pipes_client.time.sleep"),
        ):
            mock_session = MagicMock()
            mock_pipes.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_pipes.return_value.__exit__ = MagicMock(return_value=False)
            mock_session.get_bootstrap_env_vars.return_value = {}

            result = materialize(
                [tag_asset],
                resources={"compute": compute},
                instance=instance,
                raise_on_error=False,
            )

        assert result.success, _failure_events(result)

        # The run was successful, but tags were stored during execution
        # and then cleared on success.  Verify the tags were written
        # by checking the instance's run tags.  After success, tags are
        # cleared (set to ""), so verify they exist as empty strings.
        run = instance.get_run_by_id(result.run_id)
        assert run is not None
        # Tags should have been cleared (empty) on success
        assert _TAG_JOB_ID in run.tags


# ---------------------------------------------------------------------------
# Test: Reattach after a failed run
# ---------------------------------------------------------------------------


def test_reattach_after_failed_run():
    """Two materializations sharing an instance.  The first run submits a
    job and then fails due to SIGTERM.  The second run detects the
    still-running job and reattaches instead of submitting a new one.
    """

    compute = _make_slurm_compute()

    @asset
    def reattach_asset(context: AssetExecutionContext, compute: ComputeResource):
        compute.run(
            context=context,
            payload_path="script.py",
        )

    with DagsterInstance.ephemeral() as instance:
        # --- First run: submits job, then SIGTERM causes failure ---
        mock_pool_1 = _mock_ssh_pool()
        router_1 = _SshRouter()
        # SIGTERM fires immediately, and the run is not canceling so the
        # except handler fires and the run FAILS.  We need the job to
        # "submit" but then have the exception handler fire.
        router_1._sigterm_after_polls = 1
        # Job never completes in this run (we'll get interrupted)
        router_1._complete_after_polls = 999
        mock_pool_1.run.side_effect = router_1

        # For the first run, we force a failure by raising in the polling loop
        # after SIGTERM is set.  The real code would continue polling, but with
        # the multiprocess executor, an async exception is injected.
        def failing_wait(self, job_id, ssh_pool, run_dir, **kwargs):
            # Simulate: SIGTERM was received, and then the parent executor
            # kills us.  But tags are already stored.
            self._sigterm_received = True
            raise RuntimeError("Simulated process termination after SIGTERM")

        with (
            patch(
                "dagster_slurm.pipes_clients.slurm_pipes_client.SSHConnectionPool",
                return_value=mock_pool_1,
            ),
            patch.object(
                SlurmPipesClient,
                "_prepare_environment",
                return_value=("/activate.sh", "/usr/bin/python"),
            ),
            patch.object(
                SlurmPipesClient,
                "_build_tail_command",
                return_value=None,
            ),
            patch.object(
                SlurmPipesClient,
                "_find_reattachable_job",
                return_value=None,
            ),
            patch.object(
                SlurmPipesClient,
                "_wait_for_job_with_streaming",
                failing_wait,
            ),
            patch(
                "dagster_slurm.pipes_clients.slurm_pipes_client.open_pipes_session"
            ) as mock_pipes_1,
            patch("dagster_slurm.pipes_clients.slurm_pipes_client.time.sleep"),
        ):
            mock_session_1 = MagicMock()
            mock_pipes_1.return_value.__enter__ = MagicMock(return_value=mock_session_1)
            mock_pipes_1.return_value.__exit__ = MagicMock(return_value=False)
            mock_session_1.get_bootstrap_env_vars.return_value = {}

            result_1 = materialize(
                [reattach_asset],
                resources={"compute": compute},
                instance=instance,
                raise_on_error=False,
            )

        # First run should FAIL
        assert not result_1.success
        # Tags should be stored on the failed run
        run_1 = instance.get_run_by_id(result_1.run_id)
        assert run_1 is not None
        assert run_1.tags.get(_TAG_JOB_ID) == "42"
        assert _TAG_RUN_DIR in run_1.tags

        # --- Second run: should reattach ---
        mock_pool_2 = _mock_ssh_pool()
        router_2 = _SshRouter()
        # No SIGTERM, job completes quickly
        router_2._sigterm_after_polls = 999
        router_2._complete_after_polls = 3
        mock_pool_2.run.side_effect = router_2

        with (
            patch(
                "dagster_slurm.pipes_clients.slurm_pipes_client.SSHConnectionPool",
                return_value=mock_pool_2,
            ),
            patch.object(
                SlurmPipesClient,
                "_build_tail_command",
                return_value=None,
            ),
            patch.object(
                SlurmPipesClient,
                "_is_job_still_running",
                return_value=True,
            ),
            patch.object(
                SlurmPipesClient,
                "_execute_standalone",
                return_value=42,
            ) as mock_exec_2,
            patch(
                "dagster_slurm.pipes_clients.slurm_pipes_client.open_pipes_session"
            ) as mock_pipes_2,
            patch("dagster_slurm.pipes_clients.slurm_pipes_client.time.sleep"),
        ):
            mock_session_2 = MagicMock()
            mock_pipes_2.return_value.__enter__ = MagicMock(return_value=mock_session_2)
            mock_pipes_2.return_value.__exit__ = MagicMock(return_value=False)
            mock_session_2.get_bootstrap_env_vars.return_value = {}

            result_2 = materialize(
                [reattach_asset],
                resources={"compute": compute},
                instance=instance,
                raise_on_error=False,
            )

        # Reattach run should succeed
        assert result_2.success, _failure_events(result_2)
        # _execute_standalone should NOT have been called (reattach path)
        mock_exec_2.assert_not_called()


# ---------------------------------------------------------------------------
# Test: Reattach skipped when job has completed
# ---------------------------------------------------------------------------


def test_reattach_skipped_when_job_completed():
    """When the Slurm job has already finished, reattach is skipped and
    normal submission proceeds."""

    compute = _make_slurm_compute()

    @asset
    def normal_asset(context: AssetExecutionContext, compute: ComputeResource):
        compute.run(
            context=context,
            payload_path="script.py",
        )

    with DagsterInstance.ephemeral() as instance:
        mock_pool = _mock_ssh_pool()
        router = _SshRouter()
        router._sigterm_after_polls = 999
        router._complete_after_polls = 3
        mock_pool.run.side_effect = router

        # _find_reattachable_job returns a candidate but _is_job_still_running
        # returns False → no reattach, normal path taken.
        with (
            patch(
                "dagster_slurm.pipes_clients.slurm_pipes_client.SSHConnectionPool",
                return_value=mock_pool,
            ),
            patch.object(
                SlurmPipesClient,
                "_prepare_environment",
                return_value=("/activate.sh", "/usr/bin/python"),
            ),
            patch.object(
                SlurmPipesClient,
                "_build_tail_command",
                return_value=None,
            ),
            patch.object(
                SlurmPipesClient,
                "_find_reattachable_job",
                return_value={"job_id": "42", "run_dir": "/tmp/old"},
            ),
            patch.object(
                SlurmPipesClient,
                "_is_job_still_running",
                return_value=False,
            ),
            patch(
                "dagster_slurm.pipes_clients.slurm_pipes_client.open_pipes_session"
            ) as mock_pipes,
            patch("dagster_slurm.pipes_clients.slurm_pipes_client.time.sleep"),
        ):
            mock_session = MagicMock()
            mock_pipes.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_pipes.return_value.__exit__ = MagicMock(return_value=False)
            mock_session.get_bootstrap_env_vars.return_value = {}

            result = materialize(
                [normal_asset],
                resources={"compute": compute},
                instance=instance,
                raise_on_error=False,
            )

        # Should succeed via normal submission path
        assert result.success, _failure_events(result)


# ---------------------------------------------------------------------------
# Test: Exception handler preserves tags on SIGTERM
# ---------------------------------------------------------------------------


def test_exception_handler_preserves_tags_on_sigterm():
    """When SIGTERM causes the except handler to fire, run tags
    (job_id, run_dir) should remain in the instance for reattach."""

    compute = _make_slurm_compute()

    @asset
    def preserved_asset(context: AssetExecutionContext, compute: ComputeResource):
        compute.run(
            context=context,
            payload_path="script.py",
        )

    with DagsterInstance.ephemeral() as instance:
        mock_pool = _mock_ssh_pool()
        router = _SshRouter()
        router._sigterm_after_polls = 999
        router._complete_after_polls = 999
        mock_pool.run.side_effect = router

        # Simulate: job is submitted, tags stored, then SIGTERM kills us
        def failing_wait(self, job_id, ssh_pool, run_dir, **kwargs):
            self._sigterm_received = True
            raise RuntimeError("Simulated SIGTERM death")

        with (
            patch(
                "dagster_slurm.pipes_clients.slurm_pipes_client.SSHConnectionPool",
                return_value=mock_pool,
            ),
            patch.object(
                SlurmPipesClient,
                "_prepare_environment",
                return_value=("/activate.sh", "/usr/bin/python"),
            ),
            patch.object(
                SlurmPipesClient,
                "_build_tail_command",
                return_value=None,
            ),
            patch.object(
                SlurmPipesClient,
                "_find_reattachable_job",
                return_value=None,
            ),
            patch.object(
                SlurmPipesClient,
                "_wait_for_job_with_streaming",
                failing_wait,
            ),
            patch(
                "dagster_slurm.pipes_clients.slurm_pipes_client.open_pipes_session"
            ) as mock_pipes,
            patch("dagster_slurm.pipes_clients.slurm_pipes_client.time.sleep"),
        ):
            mock_session = MagicMock()
            mock_pipes.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_pipes.return_value.__exit__ = MagicMock(return_value=False)
            mock_session.get_bootstrap_env_vars.return_value = {}

            result = materialize(
                [preserved_asset],
                resources={"compute": compute},
                instance=instance,
                raise_on_error=False,
            )

        # Run should fail
        assert not result.success

        # But tags should be preserved (not cleared — cleanup is skipped on SIGTERM)
        run = instance.get_run_by_id(result.run_id)
        assert run is not None
        assert run.tags.get(_TAG_JOB_ID) == "42"
        assert run.tags.get(_TAG_RUN_DIR) != ""
