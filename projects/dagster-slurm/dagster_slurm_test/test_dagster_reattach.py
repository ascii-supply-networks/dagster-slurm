"""Dagster-level integration tests for SIGTERM behaviour.

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
from dagster_slurm.pipes_clients.slurm_pipes_client import SlurmPipesClient


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
