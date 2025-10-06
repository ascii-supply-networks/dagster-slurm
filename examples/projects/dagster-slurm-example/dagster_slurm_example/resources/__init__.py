import copy
import os
from typing import Any, Dict

import dagster as dg
from dagster_slurm import (
    BashLauncher,
    ComputeResource,
    RayLauncher,
    SlurmQueueConfig,
    SlurmResource,
    SlurmSessionResource,
    SparkLauncher,
    SSHConnectionResource,
)
from dagster_slurm.config.environment import Environment, ExecutionMode

LOCAL_RESOURCES_CONFIG: Dict[str, Any] = {
    "mode": ExecutionMode.LOCAL,
    "launchers": {
        "bash": {},
        "ray": {"num_gpus_per_node": 0, "dashboard_port": 8265},
        "spark": {"driver_memory": "2g", "executor_memory": "4g"},
    },
}

# --- Docker-based Slurm (Staging/Test) ---
DOCKER_SLURM_BASE_CONFIG: Dict[str, Any] = {
    "mode": ExecutionMode.SLURM,  # Default mode, can be overridden
    "ssh_config": {
        "host": "localhost",
        "port": 2223,
        "user": dg.EnvVar("SLURM_EDGE_NODE_USER"),
        "password": dg.EnvVar("SLURM_EDGE_NODE_PASSWORD"),
    },
    "slurm_queue_config": {
        "num_nodes": 2,
        "time_limit": "00:30:00",
        "cpus": 2,
        "gpus_per_node": 0,
        "mem": "4096M",
    },
    "slurm_session_config": {
        "num_nodes": 2,
        "time_limit": "01:00:00",
    },
    "compute_config": {
        "auto_detect_platform": True,  # Critical for local docker runs on ARM macs
        "debug_mode": False,
    },
    "launchers": {
        "bash": {},
        "ray": {
            "num_gpus_per_node": 0,
            "dashboard_port": 8265,
            "head_startup_timeout": 60,
        },
        "spark": {"driver_memory": "8g", "executor_memory": "16g"},
    },
}

# --- Supercomputer Slurm (Production) ---
SUPERCOMPUTER_SLURM_BASE_CONFIG: Dict[str, Any] = {
    "mode": ExecutionMode.SLURM,  # Default mode, can be overridden
    "ssh_config": {
        "host": dg.EnvVar("SLURM_EDGE_NODE"),
        "port": dg.EnvVar("SLURM_EDGE_NODE_PORT"),
        "user": dg.EnvVar("SLURM_EDGE_NODE_USER"),
        "password": dg.EnvVar("SLURM_EDGE_NODE_PASSWORD"),
    },
    "slurm_queue_config": {
        "partition": "batch",
        "num_nodes": 2,
        "time_limit": "04:00:00",
        "cpus": 4,
        "gpus_per_node": 0,  # Default to no GPUs, can be overridden in assets
        "mem": "4G",
    },
    "slurm_session_config": {
        "num_nodes": 2,
        "time_limit": "08:00:00",
        "partition": "batch",
    },
    "compute_config": {
        "auto_detect_platform": False,
        "pack_platform": "linux-64",
        "debug_mode": False,
    },
    "launchers": {
        "bash": {},
        "ray": {
            "num_gpus_per_node": 0,
            "dashboard_port": 8265,
            "head_startup_timeout": 120,
        },
        "spark": {"driver_memory": "8g", "executor_memory": "16g"},
    },
}


def build_slurm_resources(config: Dict[str, Any]) -> Dict[str, Any]:
    """Builds Slurm-related Dagster resources from a configuration dictionary."""
    ssh = SSHConnectionResource(**config["ssh_config"])
    queue = SlurmQueueConfig(**config["slurm_queue_config"])
    slurm = SlurmResource(ssh=ssh, queue=queue, remote_base="$HOME/dagster_runs")

    session = None
    if "session" in config["mode"].value:
        session = SlurmSessionResource(slurm=slurm, **config["slurm_session_config"])

    return {"slurm": slurm, "session": session}


def build_compute_resources(
    config: Dict[str, Any], slurm_resources: Dict[str, Any]
) -> Dict[str, ComputeResource]:
    """Builds the final dictionary of ComputeResources."""
    resources = {}
    launcher_map = {
        "bash": BashLauncher,
        "ray": RayLauncher,
        "spark": SparkLauncher,
    }

    compute_resource_keys = {
        "bash": "compute",
        "ray": "compute_ray",
        "spark": "compute_spark",
    }

    for key, LauncherClass in launcher_map.items():
        resource_key = compute_resource_keys[key]
        launcher_config = config["launchers"].get(key, {})

        resources[resource_key] = ComputeResource(
            mode=config["mode"],
            default_launcher=LauncherClass(**launcher_config),
            **slurm_resources,
            **config.get("compute_config", {}),
        )
    return resources


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


def get_resources() -> Dict[str, ComputeResource]:  # noqa: C901
    """
    Builds the Dagster resource dictionary based on the DAGSTER_DEPLOYMENT environment variable.
    This function selects a base configuration and applies modifiers based on the environment name,
    making it highly reusable and easy to extend.
    """
    deployment = get_dagster_deployment_environment()
    deployment_name = deployment.value

    # --- Case 1: Local Development ---
    if deployment == Environment.DEVELOPMENT:
        # For local, we don't need the full builder logic.
        return {
            "compute": ComputeResource(
                mode=ExecutionMode.LOCAL,
                default_launcher=BashLauncher(
                    **LOCAL_RESOURCES_CONFIG["launchers"]["bash"]
                ),
            ),
            "compute_ray": ComputeResource(
                mode=ExecutionMode.LOCAL,
                default_launcher=RayLauncher(
                    **LOCAL_RESOURCES_CONFIG["launchers"]["ray"]
                ),
            ),
            "compute_spark": ComputeResource(
                mode=ExecutionMode.LOCAL,
                default_launcher=SparkLauncher(
                    **LOCAL_RESOURCES_CONFIG["launchers"]["spark"]
                ),
            ),
        }

    # --- Case 2: All Slurm-based environments ---

    # Step 2.1: Select the appropriate base configuration.
    if "supercomputer" in deployment_name:
        config = copy.deepcopy(SUPERCOMPUTER_SLURM_BASE_CONFIG)
    else:  # All "docker" variants use the docker base
        config = copy.deepcopy(DOCKER_SLURM_BASE_CONFIG)
        # As per the original request, 'production_docker' implies a session.
        # 'staging_docker' uses the default per-asset SLURM mode.
        if deployment == Environment.PRODUCTION_DOCKER:
            config["mode"] = ExecutionMode.SLURM_SESSION

    # Step 2.2: Apply modifiers based on the environment name.
    # The elif chain ensures only the most specific modifier is applied.
    if "hetjob" in deployment_name:
        config["mode"] = ExecutionMode.SLURM_HETJOB
        # Cluster reuse is not compatible with HETJOB mode
        if "enable_cluster_reuse" in config.get("compute_config", {}):
            del config["compute_config"]["enable_cluster_reuse"]

    elif "cluster_reuse" in deployment_name:
        config["mode"] = ExecutionMode.SLURM_SESSION  # Cluster reuse implies session
        config.setdefault("compute_config", {})
        config["compute_config"]["enable_cluster_reuse"] = True
        config["compute_config"]["cluster_reuse_tolerance"] = 0.2

    elif "session" in deployment_name:
        config["mode"] = ExecutionMode.SLURM_SESSION

    # For debugging, you can force debug_mode for a specific environment
    # if "debug" in deployment_name:
    #     config["compute_config"]["debug_mode"] = True

    # Step 2.3: Build the resources from the final config spec.
    slurm_resources = build_slurm_resources(config)

    return build_compute_resources(config, slurm_resources)
