import os

import dagster as dg
from dagster_slurm import (
    BashLauncher,
    ComputeResource,
    RayLauncher,
    SlurmQueueConfig,
    SlurmResource,
    # SlurmSessionResource,
    SparkLauncher,
    SSHConnectionResource,
)
from dagster_slurm.config.environment import Environment, ExecutionMode
from loguru import logger


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
        valid_envs = [e.value for e in Environment]  # type: ignore
        raise ValueError(
            f"'{deployment}' is not a valid environment. "
            f"Please set {deployment_key} to one of: {valid_envs}"
        ) from e


def get_resources():
    """Build resource dict based on DAGSTER_DEPLOYMENT."""
    deployment = get_dagster_deployment_environment()

    if deployment == Environment.DEVELOPMENT:
        # Local mode: no SSH, no Slurm
        return {
            "compute": ComputeResource(
                mode=ExecutionMode.LOCAL,
                default_launcher=BashLauncher(),
            ),
            "compute_ray": ComputeResource(
                mode=ExecutionMode.LOCAL,
                default_launcher=RayLauncher(
                    num_gpus_per_node=0,
                    dashboard_port=8265,
                ),
            ),
            "compute_spark": ComputeResource(
                mode=ExecutionMode.LOCAL,
                default_launcher=SparkLauncher(
                    driver_memory="2g",
                    executor_memory="4g",
                ),
            ),
        }
    elif deployment == Environment.STAGING_DOCKER:
        # Slurm per-asset: each asset = separate sbatch
        ssh_connection = SSHConnectionResource(
            host="localhost",
            port=2223,
            user=dg.EnvVar("SLURM_EDGE_NODE_USER"),
            password=dg.EnvVar("SLURM_EDGE_NODE_PASSWORD"),
        )
        slurm = SlurmResource(
            ssh=ssh_connection,
            queue=SlurmQueueConfig(
                # partition="interactive",
                num_nodes=2,
                time_limit="00:30:00",
                cpus=2,
                gpus_per_node=0,
                mem="4096M",
                mem_per_cpu="",
                # partition="gpu",  # Alternative: GPU partition
                # num_nodes=4,  # For multi-node jobs
                # time_limit="12:00:00",  # Longer jobs
                # cpus=16,  # More CPUs per task
                # gpus_per_node=2,  # Request GPUs
                # mem="64G",  # More memory
                # mem_per_cpu="4G",  # Alternative: memory per CPU instead of total mem
            ),
            # remote_base="/home/submitter/dagster",
        )

        return {
            "compute": ComputeResource(
                mode=ExecutionMode.SLURM,
                slurm=slurm,
                default_launcher=BashLauncher(),
                # debug_mode=True,  # NEVER cleanup files
                auto_detect_platform=True,  # Auto-detect ARM vs x86
                # pack_platform="linux-aarch64",  # Or explicitly override for testing
            ),
            "compute_ray": ComputeResource(
                mode=ExecutionMode.SLURM,
                slurm=slurm,
                default_launcher=RayLauncher(
                    num_gpus_per_node=0,
                    dashboard_port=8265,
                    head_startup_timeout=60,
                ),
                # debug_mode=True,  # NEVER cleanup files
                auto_detect_platform=True,  # Auto-detect ARM vs x86
            ),
            "compute_spark": ComputeResource(
                mode=ExecutionMode.SLURM,
                slurm=slurm,
                default_launcher=SparkLauncher(
                    driver_memory="8g",
                    executor_memory="16g",
                ),
                # debug_mode=True,  # NEVER cleanup files
                auto_detect_platform=True,  # Auto-detect ARM vs x86
            ),
        }
    elif deployment == Environment.PRODUCTION_DOCKER:
        # TODO: make other run variants!
        # Slurm session: shared allocation, operator fusion
        pre_deployed_env_path = os.environ["DAGSTER_PROD_ENV_PATH"]
        logger.info(
            f"Production mode configured with pre-deployed env: {pre_deployed_env_path}"
        )

        # Override for prod
        ssh_connection = SSHConnectionResource(
            host=os.environ["SLURM_EDGE_NODE"],
            port=int(os.environ["SLURM_EDGE_NODE_PORT"]),
            user=os.environ["SLURM_EDGE_NODE_USER"],
            password=os.environ["SLURM_EDGE_NODE_PASSWORD"],
        )
        slurm_base = SlurmResource(
            ssh=ssh_connection,
            queue=SlurmQueueConfig(
                # partition="interactive",
                num_nodes=2,
                time_limit="00:30:00",
                cpus=2,
                gpus_per_node=0,
                mem="4096M",
                mem_per_cpu="",
                # partition="gpu",  # Alternative: GPU partition
                # num_nodes=4,  # For multi-node jobs
                # time_limit="12:00:00",  # Longer jobs
                # cpus=16,  # More CPUs per task
                # gpus_per_node=2,  # Request GPUs
                # mem="64G",  # More memory
                # mem_per_cpu="4G",  # Alternative: memory per CPU instead of total mem
            ),
            # remote_base="/home/submitter/dagster",
        )
        slurm = slurm_base.model_copy(
            update={
                "ssh": ssh_connection,
                "queue": slurm_base.queue.model_copy(
                    update={
                        # "partition": "batch",
                        "time_limit": "4:00:00",
                        "cpus": 8,
                        "mem": "8G",
                        "num_nodes": 2,
                        "gpus_per_node": 0,
                        # mem_per_cpu="",  # default (use mem instead)
                    }
                ),
            }
        )

        # session = SlurmSessionResource(
        #     slurm=slurm,
        #     num_nodes=4,
        #     time_limit="04:00:00",
        #     enable_session=True,
        #     # partition=None,  # default (uses queue.partition)
        #     # max_concurrent_jobs=10,  # default
        #     # enable_health_checks=True,  # default
        # )
        return {
            "compute": ComputeResource(
                mode=ExecutionMode.SLURM,
                # mode=ExecutionMode.SLURM_SESSION,
                slurm=slurm,
                # session=session,
                default_launcher=BashLauncher(),
                # enable_cluster_reuse=True,
                # cluster_reuse_tolerance=0.2,
                # debug_mode=True,  # NEVER cleanup files
                # ONLY ENABLE this for local docker runs!
                auto_detect_platform=True,  # Auto-detect ARM vs x86
                # pack_platform="linux-aarch64",  # Or explicitly override for testing
                pre_deployed_env_path=pre_deployed_env_path,
            ),
            "compute_ray": ComputeResource(
                mode=ExecutionMode.SLURM,
                # mode=ExecutionMode.SLURM_SESSION,
                slurm=slurm,
                # session=session,
                default_launcher=RayLauncher(
                    # num_gpus_per_node=2,
                    dashboard_port=8265,
                    head_startup_timeout=60,
                ),
                # enable_cluster_reuse=True,  # Reuse Ray clusters!
                # cluster_reuse_tolerance=0.2,
                debug_mode=True,  # NEVER cleanup files
                auto_detect_platform=True,  # Auto-detect ARM vs x86
                pre_deployed_env_path=pre_deployed_env_path,
            ),
            "compute_spark": ComputeResource(
                mode=ExecutionMode.SLURM,
                # mode=ExecutionMode.SLURM_SESSION,
                slurm=slurm,
                # session=session,
                default_launcher=SparkLauncher(
                    driver_memory="16g",
                    executor_memory="32g",
                ),
                # enable_cluster_reuse=True,  # Reuse Spark clusters!
                # cluster_reuse_tolerance=0.2,
                debug_mode=True,  # NEVER cleanup files
                auto_detect_platform=True,  # Auto-detect ARM vs x86
                pre_deployed_env_path=pre_deployed_env_path,
            ),
        }
    # TODO: Add other environments (session, cluster reuse, hetjob; and real supercomputer)
    else:
        raise ValueError(f"Unknown DAGSTER_DEPLOYMENT: {deployment}")
