import os

import dagster as dg

RESOURCES_LOCAL = {}

RESOURCES_STAGING = {}

RESOURCES_PROD = {}
resource_defs_by_deployment_name = {
    "dev-local": RESOURCES_LOCAL,
    "staging-local-run-cluster": RESOURCES_STAGING,
    "prod": RESOURCES_PROD,
}


def get_dagster_deployment_environment(
    deployment_key: str = "DAGSTER_DEPLOYMENT", default_value="dev"
):
    deplyoment = os.environ.get(deployment_key, default_value)
    dg.get_dagster_logger().debug("dagster deployment environment: %s", deplyoment)
    return deplyoment


def get_resources_for_deployment(log_env: bool = True):
    deployment_name = get_dagster_deployment_environment()
    resources = resource_defs_by_deployment_name[deployment_name]
    if log_env:
        dg.get_dagster_logger().info(f"Using deployment of: {deployment_name}")

    return resources
