"""Tests for Slurm orphan run reconciliation."""

from unittest.mock import MagicMock, patch

import dagster as dg

from dagster_slurm import SlurmQueueConfig, SlurmResource, SSHConnectionResource
from dagster_slurm.pipes_clients.slurm_pipes_client import (
    _TAG_ASSET_KEY,
    _TAG_JOB_ID,
    _TAG_LAST_SUPERVISOR_HEARTBEAT,
    _TAG_ORPHAN_RETRY_OF,
    _TAG_RUN_DIR,
    SlurmPipesClient,
)
from dagster_slurm.sensors import reconcile_orphaned_slurm_runs


def _slurm_resource() -> SlurmResource:
    ssh = SSHConnectionResource(
        host="localhost",
        port=2223,
        user="submitter",
        password="submitter",
    )
    return SlurmResource(
        ssh=ssh,
        queue=SlurmQueueConfig(partition="normal"),
        remote_base="/home/submitter/dagster_ci_runs",
    )


class _FakeSshPool:
    control_path = "/tmp/control"

    def __init__(self, state: str):
        self.state = state

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def run(self, cmd: str) -> str:
        if "squeue" in cmd:
            return self.state if self.state in {"PENDING", "RUNNING"} else ""
        if "sacct" in cmd:
            return self.state
        return ""


def _add_started_run(
    instance: dg.DagsterInstance,
    *,
    run_id: str = "orphan-run",
    heartbeat: str | None = None,
    asset_key: dg.AssetKey = dg.AssetKey("orphan_asset"),
) -> dg.DagsterRun:
    tags = {
        _TAG_JOB_ID: "42",
        _TAG_RUN_DIR: "/remote/run-dir",
        _TAG_ASSET_KEY: asset_key.to_user_string(),
    }
    if heartbeat is not None:
        tags[_TAG_LAST_SUPERVISOR_HEARTBEAT] = heartbeat

    run = dg.DagsterRun(
        job_name="__ASSET_JOB",
        run_id=run_id,
        run_config={"ops": {}},
        asset_selection={asset_key},
        status=dg.DagsterRunStatus.STARTED,
        tags=tags,
    )
    instance.add_run(run)
    return run


def test_reconcile_terminal_orphan_marks_failed_and_requests_reattach():
    with dg.DagsterInstance.ephemeral() as instance:
        run = _add_started_run(instance)

        with patch(
            "dagster_slurm.sensors.SSHConnectionPool",
            return_value=_FakeSshPool("COMPLETED"),
        ):
            requests = reconcile_orphaned_slurm_runs(
                instance,
                _slurm_resource(),
                now=1_000,
            )

        stored_run = instance.get_run_by_id(run.run_id)
        assert stored_run is not None
        assert stored_run.status == dg.DagsterRunStatus.FAILURE
        assert len(requests) == 1
        request = requests[0]
        assert request.job_name == "__ASSET_JOB"
        assert request.asset_selection == [dg.AssetKey("orphan_asset")]
        assert request.tags[_TAG_JOB_ID] == "42"
        assert request.tags[_TAG_RUN_DIR] == "/remote/run-dir"
        assert request.tags[_TAG_ORPHAN_RETRY_OF] == run.run_id


def test_reconcile_active_run_with_fresh_heartbeat_skips_retry():
    with dg.DagsterInstance.ephemeral() as instance:
        run = _add_started_run(instance, heartbeat="990")

        with patch(
            "dagster_slurm.sensors.SSHConnectionPool",
            return_value=_FakeSshPool("RUNNING"),
        ):
            requests = reconcile_orphaned_slurm_runs(
                instance,
                _slurm_resource(),
                stale_after_seconds=120,
                now=1_000,
            )

        assert requests == []
        stored_run = instance.get_run_by_id(run.run_id)
        assert stored_run is not None
        assert stored_run.status == dg.DagsterRunStatus.STARTED


def test_reconcile_active_run_with_stale_heartbeat_requests_reattach():
    with dg.DagsterInstance.ephemeral() as instance:
        run = _add_started_run(instance, heartbeat="800")

        with patch(
            "dagster_slurm.sensors.SSHConnectionPool",
            return_value=_FakeSshPool("RUNNING"),
        ):
            requests = reconcile_orphaned_slurm_runs(
                instance,
                _slurm_resource(),
                stale_after_seconds=120,
                now=1_000,
            )

        assert len(requests) == 1
        stored_run = instance.get_run_by_id(run.run_id)
        assert stored_run is not None
        assert stored_run.status == dg.DagsterRunStatus.FAILURE
        assert requests[0].tags[_TAG_ORPHAN_RETRY_OF] == run.run_id


def test_find_reattachable_job_from_current_retry_tags():
    client = SlurmPipesClient(
        slurm_resource=_slurm_resource(),
        launcher=MagicMock(),
    )
    op_context = MagicMock()
    op_context.dagster_run.tags = {
        _TAG_JOB_ID: "42",
        _TAG_RUN_DIR: "/remote/run-dir",
    }
    op_context.dagster_run.parent_run_id = None

    with patch.object(client, "_get_job_state", return_value="COMPLETED"):
        result = client._find_reattachable_job(
            op_context,
            MagicMock(),
            "orphan_asset",
        )

    assert result == {"job_id": "42", "run_dir": "/remote/run-dir"}
