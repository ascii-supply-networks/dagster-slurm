# slurm_assets.py
from typing import Optional, Iterable, Dict
from dagster import asset, AssetExecutionContext, Output

from .slurm_pipes_client import _PipesBaseSlurmClient


def make_slurm_pipes_asset(
    *,
    name: str,
    local_payload: Optional[str] = None,
    job_name: str = "pipes_ext",
    time_limit: Optional[str] = None,
    cpus: Optional[str] = None,
    mem: Optional[str] = None,
    mem_per_cpu: Optional[str] = None,
    partition: Optional[str] = None,
    extra_sbatch_args: Optional[Iterable[str]] = None,
    extra_env: Optional[Dict[str, str]] = None,
    client: Optional[_PipesBaseSlurmClient] = None,
):
    client = client or _PipesBaseSlurmClient()

    @asset(name=name)
    def _asset(context: AssetExecutionContext):
        for ev in client.run(
            context,
            local_payload=local_payload,
            job_name=job_name,
            time_limit=time_limit,
            cpus=cpus,
            mem=mem,
            mem_per_cpu=mem_per_cpu,
            partition=partition,
            extra_sbatch_args=extra_sbatch_args,
            extra_env=extra_env,
        ):
            yield ev

        yield Output(
            {"job_id": client.last_job_id, "remote_run_dir": client.last_remote_run_dir},
            metadata={"job_id": client.last_job_id, "remote_run_dir": client.last_remote_run_dir},
        )

    return _asset
