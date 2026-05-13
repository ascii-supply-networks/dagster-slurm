"""Dagster sensors and helpers for reconciling orphaned Slurm runs."""

from __future__ import annotations

import time
from collections.abc import Sequence
from typing import Any

import dagster as dg

from dagster_slurm.helpers.ssh_helpers import TERMINAL_STATES, normalize_slurm_state
from dagster_slurm.helpers.ssh_pool import SSHConnectionPool
from dagster_slurm.pipes_clients.slurm_pipes_client import (
    _TAG_ASSET_KEY,
    _TAG_JOB_ID,
    _TAG_LAST_SUPERVISOR_HEARTBEAT,
    _TAG_ORPHAN_RECONCILED_AT,
    _TAG_ORPHAN_RETRY_OF,
    _TAG_RUN_DIR,
)
from dagster_slurm.resources.slurm import SlurmResource

ACTIVE_STATES = {"PENDING", "RUNNING", "CONFIGURING", "COMPLETING"}
PARTITION_NAME_TAG = "dagster/partition"


def _parse_job_id(value: str | None) -> int | None:
    if value is None:
        return None
    value = value.strip()
    if not value.isdigit():
        return None
    return int(value)


def _heartbeat_age_seconds(run: dg.DagsterRun, *, now: float) -> float | None:
    value = run.tags.get(_TAG_LAST_SUPERVISOR_HEARTBEAT)
    if value is None:
        return None
    try:
        return now - float(value)
    except ValueError:
        return None


def _get_slurm_job_state(job_id: int, ssh_pool: SSHConnectionPool) -> str:
    output = ssh_pool.run(f"squeue -h -j {job_id} -o '%T' 2>/dev/null || true")
    for line in output.splitlines():
        parts = line.strip().split()
        if not parts:
            continue
        state = normalize_slurm_state(parts[0])
        if state:
            return state

    output = ssh_pool.run(f"sacct -X -n -j {job_id} -o State%20 2>/dev/null || true")
    for line in output.splitlines():
        parts = line.strip().split()
        if not parts:
            continue
        state = normalize_slurm_state(parts[0])
        if state:
            return state

    return ""


def _should_reconcile(
    run: dg.DagsterRun,
    *,
    slurm_state: str,
    now: float,
    stale_after_seconds: float,
) -> bool:
    if slurm_state in TERMINAL_STATES:
        return True

    if slurm_state not in ACTIVE_STATES:
        return False

    heartbeat_age = _heartbeat_age_seconds(run, now=now)
    return heartbeat_age is not None and heartbeat_age > stale_after_seconds


def _build_reattach_run_request(
    run: dg.DagsterRun,
    *,
    job_id: int,
    run_dir: str,
    slurm_state: str,
    now: float,
) -> dg.RunRequest:
    tags = {
        _TAG_JOB_ID: str(job_id),
        _TAG_RUN_DIR: run_dir,
        _TAG_ORPHAN_RETRY_OF: run.run_id,
        _TAG_ORPHAN_RECONCILED_AT: str(int(now)),
        "dagster_slurm/orphan_slurm_state": slurm_state,
    }
    asset_key = run.tags.get(_TAG_ASSET_KEY)
    if asset_key:
        tags[_TAG_ASSET_KEY] = asset_key

    partition_key = run.tags.get(PARTITION_NAME_TAG)
    if partition_key:
        tags[PARTITION_NAME_TAG] = partition_key

    asset_selection = None
    if run.asset_selection:
        asset_selection = sorted(
            run.asset_selection,
            key=lambda asset_key: asset_key.to_user_string(),
        )

    return dg.RunRequest(
        run_key=f"dagster-slurm-orphan-{run.run_id}-{job_id}",
        run_config=run.run_config,
        tags=tags,
        job_name=run.job_name,
        asset_selection=asset_selection,
        partition_key=partition_key,
    )


def reconcile_orphaned_slurm_runs(
    instance: dg.DagsterInstance,
    slurm_resource: SlurmResource,
    *,
    stale_after_seconds: float = 120.0,
    limit: int = 50,
    now: float | None = None,
) -> list[dg.RunRequest]:
    """Mark dead-supervisor runs failed and return reattach retry requests.

    The returned ``RunRequest`` objects carry the original Slurm job id and
    run directory. ``SlurmPipesClient`` consumes those tags on the retry and
    replays the remote ``messages.jsonl`` file instead of submitting another job.
    """
    timestamp = time.time() if now is None else now
    retry_requests: list[dg.RunRequest] = []
    runs = instance.get_runs(
        filters=dg.RunsFilter(statuses=[dg.DagsterRunStatus.STARTED]),
        limit=limit,
    )

    with SSHConnectionPool(slurm_resource.ssh) as ssh_pool:
        for run in runs:
            if run.tags.get(_TAG_ORPHAN_RECONCILED_AT):
                continue

            job_id = _parse_job_id(run.tags.get(_TAG_JOB_ID))
            run_dir = run.tags.get(_TAG_RUN_DIR)
            if job_id is None or not run_dir:
                continue

            slurm_state = _get_slurm_job_state(job_id, ssh_pool)
            if not slurm_state:
                continue

            if not _should_reconcile(
                run,
                slurm_state=slurm_state,
                now=timestamp,
                stale_after_seconds=stale_after_seconds,
            ):
                continue

            instance.add_run_tags(
                run.run_id,
                {
                    _TAG_ORPHAN_RECONCILED_AT: str(int(timestamp)),
                    "dagster_slurm/orphan_slurm_state": slurm_state,
                },
            )
            instance.report_run_failed(
                run,
                message=(
                    "Slurm supervisor heartbeat is stale or missing and Slurm job "
                    f"{job_id} is {slurm_state}; scheduling a reattach retry for "
                    f"{run_dir}."
                ),
            )
            retry_requests.append(
                _build_reattach_run_request(
                    run,
                    job_id=job_id,
                    run_dir=run_dir,
                    slurm_state=slurm_state,
                    now=timestamp,
                )
            )

    return retry_requests


def build_slurm_orphan_reconcile_sensor(
    slurm_resource: SlurmResource,
    *,
    name: str = "slurm_orphan_reconcile_sensor",
    jobs: Sequence[Any] | None = None,
    target: Any | None = None,
    stale_after_seconds: float = 120.0,
    limit: int = 50,
    minimum_interval_seconds: int = 30,
    default_status: dg.DefaultSensorStatus = dg.DefaultSensorStatus.STOPPED,
) -> dg.SensorDefinition:
    """Build a sensor that retries orphaned Slurm-backed Dagster runs."""
    if jobs is not None and target is not None:
        raise ValueError("Pass either jobs or target, not both.")

    sensor_kwargs: dict[str, Any] = {
        "name": name,
        "minimum_interval_seconds": minimum_interval_seconds,
        "default_status": default_status,
    }
    if jobs is not None:
        sensor_kwargs["jobs"] = jobs
    if target is not None:
        sensor_kwargs["target"] = target

    @dg.sensor(**sensor_kwargs)
    def _slurm_orphan_reconcile_sensor(
        context: dg.SensorEvaluationContext,
    ) -> dg.SkipReason | list[dg.RunRequest]:
        retry_requests = reconcile_orphaned_slurm_runs(
            context.instance,
            slurm_resource,
            stale_after_seconds=stale_after_seconds,
            limit=limit,
        )
        if not retry_requests:
            return dg.SkipReason("No orphaned Slurm runs found.")
        return retry_requests

    return _slurm_orphan_reconcile_sensor
