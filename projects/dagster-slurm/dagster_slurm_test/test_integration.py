"""Integration tests."""

import json
import shlex
import subprocess
import time
import uuid
from concurrent.futures import ProcessPoolExecutor
from pathlib import PurePosixPath
from types import SimpleNamespace
from typing import Any, cast

import dagster as dg
import pytest
from dagster import AssetExecutionContext, asset, materialize
from dagster_slurm import (
    BashLauncher,
    ComputeResource,
    RayLauncher,
    SlurmAllocationScope,
    SlurmQueueConfig,
    SlurmResource,
    SlurmRunAllocationConfig,
    SSHConnectionResource,
)
from dagster_slurm.helpers.ssh_pool import SSHConnectionPool
from dagster_slurm.config.runtime import RuntimeVariant
from dagster_slurm.launchers.base import ExecutionPlan
from dagster_slurm.pipes_clients.slurm_pipes_client import SlurmPipesClient
from dagster_slurm.resources.session import SlurmSessionResource


class _PartitionedOpContextProxy:
    """Proxy direct asset contexts into a backfill-like partitioned step."""

    def __init__(self, wrapped: Any, *, run_id: str, step_key: str, partition_key: str):
        self._wrapped = wrapped
        self.run_id = run_id
        self.step_key = step_key
        self.run_tags = {"dagster/partition": partition_key}

    @property
    def has_assets_def(self) -> bool:
        return False

    @property
    def has_partition_key(self) -> bool:
        return False

    @property
    def job_name(self) -> None:
        return None

    @property
    def retry_number(self) -> int:
        return 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._wrapped, name)


def _create_docker_python_env(remote_env_path: str) -> str:
    """Create a minimal Docker-shared Python env for the Slurm launcher."""
    quoted_env_path = shlex.quote(remote_env_path)
    quoted_env_parent = shlex.quote(str(PurePosixPath(remote_env_path).parent))
    quoted_env_grandparent = shlex.quote(
        str(PurePosixPath(remote_env_path).parent.parent)
    )
    activation_script = f"{remote_env_path}/activate.sh"
    python_executable = f"{remote_env_path}/env/bin/python"

    subprocess.run(
        [
            "docker",
            "exec",
            "slurmctld",
            "bash",
            "-lc",
            (
                f"rm -rf {quoted_env_path} && "
                f"mkdir -p {quoted_env_path}/env/bin && "
                f"ln -sf /usr/bin/python3 {shlex.quote(python_executable)} && "
                f"cat > {shlex.quote(activation_script)} <<'EOF'\n"
                f"export PATH={remote_env_path}/env/bin:${{PATH}}\n"
                "EOF\n"
                f"chmod +x {shlex.quote(activation_script)}"
            ),
        ],
        check=True,
    )
    subprocess.run(
        [
            "docker",
            "exec",
            "slurmctld",
            "bash",
            "-lc",
            (
                f"chown -R submitter:submitter {quoted_env_path} && "
                f"chown submitter:submitter {quoted_env_grandparent} && "
                f"chown submitter:submitter {quoted_env_parent} && "
                f"source {shlex.quote(activation_script)} && "
                f"{shlex.quote(python_executable)} --version"
            ),
        ],
        check=True,
    )

    return remote_env_path


def _run_partitioned_slurm_invocation(
    *,
    payload_path: str,
    remote_base: str,
    remote_env_path: str,
    run_id: str,
    partition_key: str,
    ssh_host: str,
    ssh_port: int,
    ssh_user: str,
    ssh_password: str | None,
) -> str:
    """Submit one partitioned Slurm invocation from a child process."""
    ssh = SSHConnectionResource(
        host=ssh_host,
        port=ssh_port,
        user=ssh_user,
        password=ssh_password,
    )
    slurm = SlurmResource(
        ssh=ssh,
        queue=SlurmQueueConfig(partition="normal"),
        remote_base=remote_base,
    )
    asset_context = dg.build_asset_context(partition_key=partition_key)
    op_context = _PartitionedOpContextProxy(
        asset_context.op_execution_context,
        run_id=run_id,
        step_key="partitioned_asset",
        partition_key=partition_key,
    )
    client = SlurmPipesClient(
        slurm_resource=slurm,
        launcher=BashLauncher(),
        pre_deployed_env_path=remote_env_path,
        debug_mode=True,
    )
    run_dir = client._get_remote_run_dir(remote_base, run_id, cast(Any, op_context))
    context = SimpleNamespace(
        run=SimpleNamespace(run_id=run_id),
        op_execution_context=op_context,
        add_output_metadata=lambda *args, **kwargs: None,
    )
    client.run(
        context=cast(Any, context),
        payload_path=payload_path,
        extra_env={"TEST_PARTITION": partition_key},
        extra_slurm_opts={
            "time_limit": "00:02:00",
            "cpus_per_task": 1,
            "partition": "normal",
        },
        poll_timeout=180,
    )
    return run_dir


def _run_shared_session_srun_invocation(
    *,
    remote_base: str,
    run_id: str,
    worker_name: str,
    marker_dir: str,
    ssh_host: str,
    ssh_port: int,
    ssh_user: str,
    ssh_password: str | None,
) -> dict[str, str]:
    """Execute one shared-allocation srun step from a child process."""
    ssh = SSHConnectionResource(
        host=ssh_host,
        port=ssh_port,
        user=ssh_user,
        password=ssh_password,
    )
    slurm = SlurmResource(
        ssh=ssh,
        queue=SlurmQueueConfig(
            partition="normal",
            num_nodes=1,
            cpus=1,
            mem="1G",
            time_limit="00:05:00",
        ),
        remote_base=remote_base,
    )
    session = SlurmSessionResource(
        slurm=slurm,
        num_nodes=1,
        time_limit="00:05:00",
        partition="normal",
        cpus_per_task=1,
        mem="1G",
        enable_health_checks=False,
    )
    object.__setattr__(session, "_shared_lifecycle", True)
    context = SimpleNamespace(run=SimpleNamespace(run_id=run_id))
    session.setup_for_execution(cast(Any, context))
    try:
        run_dir = f"{remote_base}/runs/{run_id}/{worker_name}"
        ssh_pool = session._require_ssh_pool()
        ssh_pool.run(f"mkdir -p {shlex.quote(run_dir)} {shlex.quote(marker_dir)}")
        marker_path = f"{marker_dir}/{worker_name}.json"
        plan = ExecutionPlan(
            kind=RuntimeVariant.SHELL,
            payload=[
                "#!/bin/bash",
                "set -euo pipefail",
                "sleep 2",
                f'printf \'{{"worker":"{worker_name}","slurm_job_id":"%s"}}\\n\' "$SLURM_JOB_ID" > {shlex.quote(marker_path)}',
            ],
            environment={},
            resources={},
        )
        result = session.execute_in_session(
            execution_plan=plan,
            asset_key=f"shared/{worker_name}",
            run_dir=run_dir,
        )
        return {
            "worker": worker_name,
            "job_id": str(result.job_id),
            "stdout_path": result.stdout_path,
            "stderr_path": result.stderr_path,
        }
    finally:
        session.teardown_after_execution(cast(Any, context))


def _wait_for_slurm_job_to_leave_queue(
    ssh_pool: SSHConnectionPool,
    job_id: str,
    *,
    timeout: int = 60,
) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        state = ssh_pool.run(f"squeue -h -j {shlex.quote(job_id)} -o '%T' || true")
        if not state.strip():
            return
        time.sleep(2)
    pytest.fail(f"Slurm job {job_id} was still queued after {timeout}s")


def test_local_asset_execution(temp_dir, local_compute_resource):
    """Test complete local asset execution."""
    # Create test payload
    payload = temp_dir / "test_payload.py"
    payload.write_text("""
from dagster_pipes import open_dagster_pipes

with open_dagster_pipes() as context:
    context.log.info("Test execution")
    context.report_asset_materialization(metadata={"test": "value"})
""")

    @asset
    def test_asset(context: AssetExecutionContext, compute: ComputeResource):
        yield from compute.run(
            context=context,
            payload_path=str(payload),
        ).get_results()

    result = materialize(
        [test_asset],
        resources={"compute": local_compute_resource},
    )

    assert result.success


def test_bash_launcher_integration(temp_dir, local_compute_resource):
    """Test bash launcher integration."""
    payload = temp_dir / "bash_payload.py"
    payload.write_text("""
import os
from dagster_pipes import open_dagster_pipes

with open_dagster_pipes() as context:
    test_var = os.environ.get("TEST_VAR", "not_set")
    context.log.info(f"TEST_VAR={test_var}")
    context.report_asset_materialization(metadata={"test_var": test_var})
""")

    @asset
    def bash_asset(context: AssetExecutionContext, compute: ComputeResource):
        yield from compute.run(
            context=context,
            payload_path=str(payload),
            extra_env={"TEST_VAR": "test_value"},
        ).get_results()

    result = materialize(
        [bash_asset],
        resources={"compute": local_compute_resource},
    )

    assert result.success


def test_local_asset_check_execution(temp_dir, local_compute_resource):
    """Test local asset checks reported via Dagster Pipes."""
    payload = temp_dir / "asset_check_payload.py"
    payload.write_text("""
from dagster_pipes import open_dagster_pipes

with open_dagster_pipes() as context:
    context.report_asset_check(
        asset_key="orders",
        check_name="orders_are_non_empty",
        passed=True,
    )
""")

    @dg.asset
    def orders():
        return 1

    @dg.asset_check(asset=orders)
    def orders_are_non_empty(
        context: dg.AssetCheckExecutionContext,
        compute: ComputeResource,
    ):
        return compute.run(
            context=context,
            payload_path=str(payload),
        ).get_asset_check_result()

    job = dg.define_asset_job("asset_check_job")
    defs = dg.Definitions(
        assets=[orders],
        asset_checks=[orders_are_non_empty],
        jobs=[job],
        resources={"compute": local_compute_resource},
    )

    result = defs.resolve_job_def("asset_check_job").execute_in_process()

    assert result.success
    evaluations = result.get_asset_check_evaluations()
    assert len(evaluations) == 1
    assert evaluations[0].check_name == "orders_are_non_empty"
    assert evaluations[0].passed is True


@pytest.mark.needs_slurm_docker
def test_parallel_partitioned_slurm_invocations_use_distinct_message_files(
    temp_dir,
    slurm_resource_for_testing,
    slurm_cluster_ready,
):
    """Docker SLURM regression for backfill-style same-asset parallel instances."""
    payload = temp_dir / "partitioned_payload.py"
    payload.write_text(
        """
import base64
import json
import os
import time
import zlib


def _decode_pipes_param(value):
    return json.loads(zlib.decompress(base64.b64decode(value)).decode("utf-8"))


messages_path = _decode_pipes_param(os.environ["DAGSTER_PIPES_MESSAGES"])["path"]


def _write_message(method, params):
    with open(messages_path, "a", encoding="utf-8") as messages_file:
        messages_file.write(
            json.dumps(
                {
                    "__dagster_pipes_version": "0.1",
                    "method": method,
                    "params": params,
                }
            )
            + "\\n"
        )


partition = os.environ["TEST_PARTITION"]
_write_message("opened", {"extras": {}})
_write_message(
    "log_external_stream",
    {"stream": "stdout", "text": f"partition={partition} started\\n", "extras": {}},
)
time.sleep(2)
_write_message(
    "log_external_stream",
    {"stream": "stdout", "text": f"partition={partition} done\\n", "extras": {}},
)
_write_message("closed", {})
""",
        encoding="utf-8",
    )

    run_id = f"channel_backfill_{uuid.uuid4().hex}"
    remote_run_root = f"{slurm_resource_for_testing.remote_base}/runs/{run_id}"
    remote_env_path = _create_docker_python_env(f"{remote_run_root}/_test_env")

    partitions = ["alpha", "beta"]
    try:
        with ProcessPoolExecutor(max_workers=len(partitions)) as executor:
            futures = [
                executor.submit(
                    _run_partitioned_slurm_invocation,
                    payload_path=str(payload),
                    remote_base=str(slurm_resource_for_testing.remote_base),
                    remote_env_path=remote_env_path,
                    run_id=run_id,
                    partition_key=partition_key,
                    ssh_host=slurm_resource_for_testing.ssh.host,
                    ssh_port=slurm_resource_for_testing.ssh.port,
                    ssh_user=slurm_resource_for_testing.ssh.user,
                    ssh_password=slurm_resource_for_testing.ssh.password,
                )
                for partition_key in partitions
            ]
            run_dirs = [future.result(timeout=240) for future in futures]

        assert len(run_dirs) == len(set(run_dirs)) == 2

        with SSHConnectionPool(slurm_resource_for_testing.ssh) as ssh_pool:
            quoted_root = shlex.quote(remote_run_root)
            messages_output = ssh_pool.run(
                f"find {quoted_root} -path '*/messages.jsonl' -type f | sort 2>/dev/null || true"
            )
            message_paths = [
                line.strip() for line in messages_output.splitlines() if line.strip()
            ]

            assert message_paths == [
                f"{run_dir}/messages.jsonl" for run_dir in run_dirs
            ]
            assert (
                ssh_pool.run(
                    f"test ! -f {quoted_root}/messages.jsonl && echo ok"
                ).strip()
                == "ok"
            )

            for partition_key, run_dir in zip(partitions, run_dirs):
                assert f"partition={partition_key}" in run_dir
                content = ssh_pool.run(
                    f"cat {shlex.quote(run_dir)}/messages.jsonl 2>/dev/null || true"
                )
                assert f"partition={partition_key} started" in content
                other_partition = next(p for p in partitions if p != partition_key)
                assert f"partition={other_partition} started" not in content
    finally:
        subprocess.run(
            [
                "docker",
                "exec",
                "slurmctld",
                "bash",
                "-lc",
                f"rm -rf {shlex.quote(remote_run_root)}",
            ],
            check=False,
        )


@pytest.mark.needs_slurm_docker
def test_run_scoped_allocation_is_shared_across_parallel_processes(
    slurm_resource_for_testing,
    slurm_cluster_ready,
):
    """Docker SLURM regression for run allocation setup from separate processes."""
    run_id = f"shared_session_{uuid.uuid4().hex}"
    remote_run_root = f"{slurm_resource_for_testing.remote_base}/runs/{run_id}"
    marker_dir = f"{remote_run_root}/markers"
    workers = ["alpha", "beta"]

    try:
        with ProcessPoolExecutor(max_workers=len(workers)) as executor:
            futures = [
                executor.submit(
                    _run_shared_session_srun_invocation,
                    remote_base=str(slurm_resource_for_testing.remote_base),
                    run_id=run_id,
                    worker_name=worker_name,
                    marker_dir=marker_dir,
                    ssh_host=slurm_resource_for_testing.ssh.host,
                    ssh_port=slurm_resource_for_testing.ssh.port,
                    ssh_user=slurm_resource_for_testing.ssh.user,
                    ssh_password=slurm_resource_for_testing.ssh.password,
                )
                for worker_name in workers
            ]
            results = [future.result(timeout=240) for future in futures]

        assert len({result["job_id"] for result in results}) == 1
        job_id = results[0]["job_id"]

        with SSHConnectionPool(slurm_resource_for_testing.ssh) as ssh_pool:
            records = [
                json.loads(
                    ssh_pool.run(
                        f"cat {shlex.quote(marker_dir)}/{shlex.quote(worker)}.json"
                    )
                )
                for worker in workers
            ]
            assert {record["worker"] for record in records} == set(workers)
            assert {record["slurm_job_id"] for record in records} == {job_id}

            allocation_metadata = json.loads(
                ssh_pool.run(
                    "cat "
                    f"{shlex.quote(slurm_resource_for_testing.remote_base)}/"
                    f"allocations/dagster_{run_id}/allocation.json"
                )
            )
            assert str(allocation_metadata["slurm_job_id"]) == job_id

            allocation_log_count = ssh_pool.run(
                "find "
                f"{shlex.quote(slurm_resource_for_testing.remote_base)}/"
                f"allocations/dagster_{run_id} "
                "-name 'allocation_*.log' -type f | wc -l"
            ).strip()
            assert allocation_log_count == "1"

            _wait_for_slurm_job_to_leave_queue(ssh_pool, job_id, timeout=90)
    finally:
        subprocess.run(
            [
                "docker",
                "exec",
                "slurmctld",
                "bash",
                "-lc",
                f"rm -rf {shlex.quote(remote_run_root)} "
                f"{shlex.quote(slurm_resource_for_testing.remote_base)}/"
                f"allocations/dagster_{run_id}",
            ],
            check=False,
        )


@pytest.mark.needs_slurm_docker
def test_run_scoped_ray_allocation_reuses_one_docker_slurm_job(
    temp_dir,
    slurm_resource_for_testing,
    deployment_metadata,
    slurm_cluster_ready,
):
    """Docker SLURM regression for allocation_scope='run' with Ray assets."""
    run_token = uuid.uuid4().hex
    marker_dir = (
        f"{slurm_resource_for_testing.remote_base}/run_scope_markers/{run_token}"
    )
    payload = temp_dir / "run_scoped_ray_payload.py"
    payload.write_text(
        """
import json
import os
from pathlib import Path

import ray
from dagster_pipes import open_dagster_pipes


asset_name = os.environ["ASSET_NAME"]
marker_dir = Path(os.environ["MARKER_DIR"])
marker_dir.mkdir(parents=True, exist_ok=True)

ray.init(address=os.environ["RAY_ADDRESS"])


@ray.remote
def report_runtime():
    import os
    import socket

    return {
        "hostname": socket.gethostname(),
        "slurm_job_id": os.environ.get("SLURM_JOB_ID"),
        "ray_address": os.environ.get("RAY_ADDRESS"),
    }


runtime = ray.get(report_runtime.remote())
record = {
    "asset": asset_name,
    "runtime": runtime,
    "payload_slurm_job_id": os.environ.get("SLURM_JOB_ID"),
    "run_allocation_job_id": os.environ.get("SLURM_RUN_ALLOCATION_JOB_ID"),
    "ray_address": os.environ.get("RAY_ADDRESS"),
}
(marker_dir / f"{asset_name}.json").write_text(json.dumps(record), encoding="utf-8")

with open_dagster_pipes() as context:
    context.report_asset_materialization(
        metadata={
            "asset": asset_name,
            "payload_slurm_job_id": record["payload_slurm_job_id"],
            "run_allocation_job_id": record["run_allocation_job_id"],
            "ray_address": record["ray_address"],
            "runtime": json.dumps(runtime),
        }
    )
""",
        encoding="utf-8",
    )

    slurm = SlurmResource(
        ssh=slurm_resource_for_testing.ssh,
        queue=SlurmQueueConfig(
            partition="normal",
            num_nodes=1,
            time_limit="00:05:00",
            cpus=2,
            mem="2G",
        ),
        remote_base=slurm_resource_for_testing.remote_base,
    )
    compute = ComputeResource(
        mode="slurm",
        slurm=slurm,
        default_launcher=RayLauncher(
            num_gpus_per_node=0,
            head_startup_timeout=120,
        ),
        allocation_scope=SlurmAllocationScope.RUN,
        run_allocation=SlurmRunAllocationConfig(
            num_nodes=1,
            cpus_per_task=2,
            mem="2G",
            time_limit="00:05:00",
            partition="normal",
        ),
        pre_deployed_env_path=deployment_metadata["deployment_path"],
        debug_mode=True,
    )

    @asset
    def run_scoped_ray_first(context: AssetExecutionContext, compute: ComputeResource):
        yield from compute.run(
            context=context,
            payload_path=str(payload),
            extra_env={"ASSET_NAME": "first", "MARKER_DIR": marker_dir},
            poll_timeout=240,
        ).get_results()

    @asset(deps=[run_scoped_ray_first])
    def run_scoped_ray_second(context: AssetExecutionContext, compute: ComputeResource):
        yield from compute.run(
            context=context,
            payload_path=str(payload),
            extra_env={"ASSET_NAME": "second", "MARKER_DIR": marker_dir},
            poll_timeout=240,
        ).get_results()

    try:
        result = materialize(
            [run_scoped_ray_first, run_scoped_ray_second],
            resources={"compute": compute},
        )
        assert result.success

        with SSHConnectionPool(slurm.ssh) as ssh_pool:
            first = json.loads(
                ssh_pool.run(f"cat {shlex.quote(marker_dir)}/first.json")
            )
            second = json.loads(
                ssh_pool.run(f"cat {shlex.quote(marker_dir)}/second.json")
            )

            assert first["run_allocation_job_id"]
            assert first["run_allocation_job_id"] == second["run_allocation_job_id"]
            assert first["payload_slurm_job_id"] == first["run_allocation_job_id"]
            assert second["payload_slurm_job_id"] == second["run_allocation_job_id"]
            assert first["ray_address"] == second["ray_address"]
            assert first["runtime"]["ray_address"] == second["runtime"]["ray_address"]

            _wait_for_slurm_job_to_leave_queue(
                ssh_pool,
                first["run_allocation_job_id"],
            )

            remote_run_root = (
                f"{slurm_resource_for_testing.remote_base}/runs/{result.run_id}"
            )
            job_id = first["run_allocation_job_id"]
            log_output = ssh_pool.run(
                "find "
                f"{shlex.quote(remote_run_root)} "
                r"\( -name "
                f"{shlex.quote(f'slurm-{job_id}-step-*.out')} "
                r"-o -name "
                f"{shlex.quote(f'slurm-{job_id}-step-*.err')} "
                r"\) -type f | sort 2>/dev/null || true"
            )
            log_paths = [
                line.strip() for line in log_output.splitlines() if line.strip()
            ]
            assert len(log_paths) == 4
            assert any(
                path.endswith(".out") and "run_scoped_ray_first" in path
                for path in log_paths
            )
            assert any(
                path.endswith(".out") and "run_scoped_ray_second" in path
                for path in log_paths
            )
    finally:
        subprocess.run(
            [
                "docker",
                "exec",
                "slurmctld",
                "bash",
                "-lc",
                f"rm -rf {shlex.quote(marker_dir)}",
            ],
            check=False,
        )
