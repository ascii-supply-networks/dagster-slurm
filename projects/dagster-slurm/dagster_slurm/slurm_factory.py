# slurm_assets.py
from typing import Optional, Iterable, Dict, Any, Sequence
from dagster import asset, AssetExecutionContext, Output, AssetKey

import inspect
from .slurm_pipes_client import _PipesBaseSlurmClient

from pathlib import Path

def _resolve_payload_path(payload: str | Path, caller_file: str) -> str:
    p = Path(payload)
    if not p.is_absolute():
        p = Path(caller_file).parent / p
    p = p.resolve()
    if not p.exists():
        raise FileNotFoundError(f"Payload not found: {p} (from '{payload}')")
    return str(p)

def make_slurm_pipes_asset(
    *,  
    name: str,
    local_payload: str,
    job_name: str = "pipes_ext",
    asset_key: Optional[Sequence[str]] = None,
    time_limit: Optional[str] = None,
    cpus: Optional[str] = None,
    mem: Optional[str] = None,
    mem_per_cpu: Optional[str] = None,
    partition: Optional[str] = None,
    extra_sbatch_args: Optional[Iterable[str]] = None,
    extra_env: Optional[Dict[str, str]] = None,
    client: Optional[_PipesBaseSlurmClient] = None,
    slurm_template_path: Optional[str] = None,
    template_params: Optional[Dict[str, str]] = None,
    extras: Optional[Dict[str, Any]] = None,
):
    if client is None:
        client = _PipesBaseSlurmClient(
            default_partition=partition,
            default_time_limit=time_limit,
            default_cpus=cpus,
            default_mem=mem,
            default_mem_per_cpu=mem_per_cpu
        )
    caller_file = inspect.stack()[1].filename
    payload_path = _resolve_payload_path(local_payload, caller_file)
    resolved_template = None
    if slurm_template_path:
        resolved_template = _resolve_payload_path(slurm_template_path, caller_file)

    dec_kwargs = {}
    if asset_key is not None:
        dec_kwargs["key"] = AssetKey(list(asset_key))
    else:
        dec_kwargs["name"] = name  
        
    @asset(**dec_kwargs)
    def _asset(context: AssetExecutionContext):
        for ev in client.run(
            context,
            local_payload=payload_path,
            job_name=job_name,
            time_limit=time_limit,
            cpus=cpus,
            mem=mem,
            mem_per_cpu=mem_per_cpu,
            partition=partition,
            extra_sbatch_args=extra_sbatch_args,
            extra_env=extra_env,
            slurm_template_path=resolved_template,
            template_params=template_params,
            extras=extras,
        ):
            if isinstance(ev, str):
                context.log.info(ev)
            elif hasattr(ev, "to_observation"):
                yield ev.to_observation()

        yield Output(
            {"job_id": client.last_job_id, "remote_run_dir": client.last_remote_run_dir},
            metadata={
                "job_id": client.last_job_id,
                "remote_run_dir": client.last_remote_run_dir,
            },
        )

    return _asset
