import os

import dagster as dg
from dagster_ray import LocalRay
from dagster_slurm import (
    BashLauncher,
    ComputeResource,
    SlurmResource,
    SlurmSessionResource,
)
from dagster_slurm.config.environment import Environment, ExecutionMode

from .lazy_local_ray import (
    PipesRayJobClientLazyLocalResource,
)


def get_dagster_deployment_environment(
    deployment_key: str = "DAGSTER_DEPLOYMENT", default_value="development"
):
    deployment = os.environ.get(deployment_key, default_value).lower()
    try:
        environment = Environment(deployment)
        dg.get_dagster_logger().debug("dagster deployment environment: %s", deployment)
        return environment
    except ValueError as e:
        # 4. Handle the case where the string is not a valid environment
        valid_envs = [e.value for e in Environment]
        raise ValueError(
            f"'{deployment}' is not a valid environment. "
            f"Please set {deployment_key} to one of: {valid_envs}"
        ) from e


def get_resources():
    """Build resource dict based on DAGSTER_DEPLOYMENT."""
    deployment = get_dagster_deployment_environment()

    pipes_subprocess_client = dg.PipesSubprocessClient()

    if deployment == Environment.DEVELOPMENT:
        # Local mode: no SSH, no Slurm
        local_ray = LocalRay(
            ray_init_options={
                "ignore_reinit_error": False,
                "dashboard_host": "127.0.0.1",
                "dashboard_port": 8265,
            }
        )
        return {
            "compute": ComputeResource(
                mode=ExecutionMode.LOCAL,
                default_launcher=BashLauncher(),
            ),
            "pipes_subprocess_client": pipes_subprocess_client,
            # TODO: Can we remove these eventually?
            "ray_cluster": local_ray,
            "pipes_ray_job_client": PipesRayJobClientLazyLocalResource(
                ray_cluster=local_ray
            ),
        }
    elif deployment == Environment.STAGING:
        # Slurm per-asset: each asset = separate sbatch
        slurm = SlurmResource.from_env()
        # Override for staging
        slurm.queue.partition = "interactive"
        slurm.queue.time_limit = "01:00:00"
        slurm.queue.cpus = 4
        slurm.queue.gpus_per_node = 0
        slurm.queue.num_nodes = 1
        slurm.queue.mem = "4G"
        return {
            "compute": ComputeResource(
                mode=ExecutionMode.SLURM,
                slurm=slurm,
                default_launcher=BashLauncher(activate_sh=slurm.activate_sh),
            ),
            "pipes_subprocess_client": pipes_subprocess_client,
        }
    elif deployment == Environment.PRODUCTION:
        # Slurm session: shared allocation, operator fusion
        slurm = SlurmResource.from_env()
        # Override for prod
        slurm.queue.partition = "batch"
        slurm.queue.time_limit = "24:00:00"
        slurm.queue.cpus = 32
        slurm.queue.mem = "64G"
        slurm.queue.num_nodes = 4
        slurm.queue.gpus_per_node = 1
        session = SlurmSessionResource(
            slurm=slurm,
            num_nodes=4,
            time_limit="04:00:00",
            enable_session=True,
        )
        return {
            "compute": ComputeResource(
                mode=ExecutionMode.SLURM_SESSION,
                slurm=slurm,
                session=session,
                default_launcher=BashLauncher(activate_sh=slurm.activate_sh),
            ),
            "pipes_subprocess_client": pipes_subprocess_client,
        }
    else:
        raise ValueError(f"Unknown DAGSTER_DEPLOYMENT: {deployment}")
