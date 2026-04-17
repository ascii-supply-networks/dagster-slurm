import copy
import os
import shlex
from datetime import datetime, timezone
from typing import Any, Dict, Optional

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
from dagster_slurm.auth.step_oidc import StepOIDCAuthProvider
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
        # Hash local source trees, not built artifacts, so source edits trigger a cache
        # miss before the pack step decides whether artifacts need rebuilding.
        "cache_inject_globs": [
            "../projects/dagster-slurm/dagster_slurm/**/*.py",
            "../projects/dagster-slurm/pyproject.toml",
            "projects/dagster-slurm-example-shared/dagster_slurm_example_shared/**/*.py",
            "projects/dagster-slurm-example-shared/pyproject.toml",
            "projects/dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/**/*.py",
            "projects/dagster-slurm-example-hpc-workload/pyproject.toml",
            "projects/dagster-slurm-example/dagster_slurm_example/**/*.py",
            "projects/dagster-slurm-example/pyproject.toml",
        ],
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
PRODUCTION_DOCKER_OVERRIDES: Dict[str, Any] = {
    "slurm_queue_config": {
        "time_limit": "04:00:00",
        "cpus": 2,
        "mem": "4G",
    },
    "compute_config": {
        "debug_mode": False,
    },
}

# --- Supercomputer Slurm (Production) ---
SUPERCOMPUTER_SLURM_BASE_CONFIG: Dict[str, Any] = {
    "mode": ExecutionMode.SLURM,  # Default mode, can be overridden
    "ssh_config": {
        "host": str(dg.EnvVar("SLURM_EDGE_NODE_HOST").get_value(default="127.0.0.1")),
        "port": int(str(dg.EnvVar("SLURM_EDGE_NODE_PORT").get_value(default="2223"))),
        "user": str(dg.EnvVar("SLURM_EDGE_NODE_USER").get_value(default="submitter")),
        "password": dg.EnvVar("SLURM_EDGE_NODE_PASSWORD").get_value(default=None),
        "key_path": dg.EnvVar("SLURM_EDGE_NODE_KEY_PATH").get_value(default=None),
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
        # Hash local source trees, not built artifacts, so source edits trigger a cache
        # miss before the pack step decides whether artifacts need rebuilding.
        "cache_inject_globs": [
            "../projects/dagster-slurm/dagster_slurm/**/*.py",
            "../projects/dagster-slurm/pyproject.toml",
            "projects/dagster-slurm-example-shared/dagster_slurm_example_shared/**/*.py",
            "projects/dagster-slurm-example-shared/pyproject.toml",
            "projects/dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/**/*.py",
            "projects/dagster-slurm-example-hpc-workload/pyproject.toml",
            "projects/dagster-slurm-example/dagster_slurm_example/**/*.py",
            "projects/dagster-slurm-example/pyproject.toml",
        ],
    },
    "launchers": {
        "bash": {},
        "ray": {
            "num_gpus_per_node": 0,
            "head_startup_timeout": 120,
        },
        "spark": {"driver_memory": "8g", "executor_memory": "16g"},
    },
}

SUPERCOMPUTER_SITE_OVERRIDES: Dict[str, Dict[str, Any]] = {
    # Austrian Scientific Computing (ASC) (VSC-5) queue defaults.
    # Off-hackathon CPU slots (active)
    # "vsc5": {
    #     "slurm_queue_config": {
    #         # sinfo | grep idle
    #         "partition": "zen2_0256_a40x2",
    #         "qos": "zen2_0256_a40x2",
    #         # "partition": "zen3_0512",
    #         # "qos": "zen3_0512_devel",
    #         "time_limit": "00:10:00",
    #         "num_nodes": 1,
    #         "gpus_per_node": 0,
    #     },
    #     "slurm_session_config": {
    #         "partition": "zen3_0512",
    #         "qos": "zen3_0512_devel",
    #         "time_limit": "00:10:00",
    #         "num_nodes": 1,
    #         "gpus_per_node": 0,
    #     },
    # },
    # Hackathon GPU reservation (toggle manually when the reservation is active)
    "vsc5": {
        "slurm_queue_config": {
            "partition": "zen3_0512_a100x2",
            "qos": "zen3_0512_a100x2",
            "reservation": "dagster-slurm_23",
            "gpus_per_node": 1,
            "num_nodes": 1,
            "mem": None,
        },
        "slurm_session_config": {
            "partition": "zen3_0512_a100x2",
            "qos": "zen3_0512_a100x2",
            "reservation": "dagster-slurm_23",
            "gpus_per_node": 1,
            "num_nodes": 1,
        },
    },
    "musica": {
        "slurm_queue_config": {
            # Default MUSICA profile: CPU dev queue for quick single-node tests.
            "partition": "zen4_0768",
            "gpus_per_node": 0,
            "mem": "64G",
            # "reservation": "dagster_test",  # April 16, 2026 test window
            # "reservation": "dagster",  # April 17, 2026 webinar window
            # GPU alternatives:
            # "partition": "zen4_0768_h100x4",
            # "qos": "zen4_0768_h100x4",
            "qos": "dev_zen4_0768",
            "time_limit": "00:10:00",
            # "gpus_per_node": 1,
            "num_nodes": 1,
        },
        "slurm_session_config": {
            "partition": "zen4_0768",
            "qos": "dev_zen4_0768",
            "time_limit": "00:10:00",
            "gpus_per_node": 0,
            "num_nodes": 1,
        },
        "launchers": {
            "ray": {
                "pre_start_commands": ["ulimit -n 65536"],
                # "ray_start_args": ["--include-dashboard=false"],
            }
        },
    },
    # Leonardo (CINECA) runs directly on the edge node without an extra hop.
    "leonardo": {
        "slurm_queue_config": {
            "partition": "boost_usr_prod",
            "qos": "boost_qos_dbg",
            "account": "EUHPC_D20_063",
            "time_limit": "00:05:00",
        },
        "slurm_session_config": {
            "partition": "boost_usr_prod",
            "qos": "boost_qos_dbg",
            "account": "EUHPC_D20_063",
            "time_limit": "00:05:00",
        },
        "launchers": {
            "ray": {
                # Use an unambiguous string that won't be coerced to None
                "worker_cpu_bind": "_none_",
            }
        },
    },
    "datalab": {
        "ssh_config": {
            "host": "cluster.datalab.tuwien.ac.at",
            "port": 22,
            "user": "georg.heiler",
            "key_path": os.path.expanduser("~/.ssh/id_datalab"),
        },
        "slurm_queue_config": {
            "partition": "GPU-a100s",
            "time_limit": "00:10:00",
            "num_nodes": 1,
            "gpus_per_node": 1,
        },
        "slurm_session_config": {
            "partition": "GPU-a100s",
            "time_limit": "00:10:00",
            "num_nodes": 1,
            "gpus_per_node": 1,
        },
        "launchers": {
            "ray": {
                "pre_start_commands": ["ulimit -n 65536"],
            }
        },
    },
}


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge override into base, modifying base in-place."""
    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
    return base


def _is_docker_based(environment: Environment) -> bool:
    """Check if environment uses docker (local slurm)."""
    return "_docker" in environment.value


def _is_supercomputer_based(environment: Environment) -> bool:
    """Check if environment uses supercomputer."""
    return "_supercomputer" in environment.value


def _is_production(environment: Environment) -> bool:
    """Check if environment is production-based."""
    return environment.value.startswith("production_")


def _build_musica_auth_provider() -> StepOIDCAuthProvider:
    return StepOIDCAuthProvider(
        token_url=os.environ.get(
            "ASC_OIDC_TOKEN_URL",
            "https://auth.asc.ac.at/application/o/token/",
        ),
        client_id=os.environ.get("ASC_OIDC_CLIENT_ID", ""),
        username=os.environ.get("ASC_OIDC_USERNAME", ""),
        app_password=os.environ.get("ASC_OIDC_APP_PASSWORD"),
        scope=os.environ.get("ASC_OIDC_SCOPE", "profile"),
        token_field=os.environ.get("ASC_OIDC_TOKEN_FIELD", "access_token"),
        context=os.environ.get("ASC_STEP_CONTEXT", "asc"),
        refresh_skew_minutes=int(os.environ.get("ASC_STEP_REFRESH_SKEW", "30")),
        cert_path=os.environ.get("ASC_STEP_CERT_PATH"),
        ssh_key_path=os.environ.get("SLURM_EDGE_NODE_KEY_PATH"),
        bootstrap_ca_url=os.environ.get("ASC_STEP_CA_URL"),
        bootstrap_fingerprint=os.environ.get("ASC_STEP_FINGERPRINT"),
        authentik_api_base=os.environ.get("ASC_AUTHENTIK_API_BASE"),
        authentik_api_token=os.environ.get("ASC_AUTHENTIK_API_TOKEN"),
        authentik_token_identifier=os.environ.get(
            "ASC_AUTHENTIK_TOKEN_IDENTIFIER", "dagster-slurm"
        ),
        authentik_token_expires_days=int(
            os.environ.get("ASC_AUTHENTIK_TOKEN_EXPIRES_DAYS", "30")
        ),
        authentik_credentials_file=os.environ.get("ASC_AUTHENTIK_CREDENTIALS_FILE"),
        authentik_refresh_skew_days=int(
            os.environ.get("ASC_AUTHENTIK_REFRESH_SKEW_DAYS", "7")
        ),
    )


def _musica_provider_is_configured(provider: StepOIDCAuthProvider) -> bool:
    return bool(
        provider.client_id
        and provider.username
        and (
            provider.app_password
            or provider.authentik_credentials_file
            or (provider.authentik_api_base and provider.authentik_api_token)
        )
    )


def _is_future_timestamp(value: Optional[datetime]) -> bool:
    if value is None:
        return False
    normalized = (
        value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    )
    return normalized > datetime.now(timezone.utc)


def _musica_provider_has_usable_app_password(
    provider: StepOIDCAuthProvider,
) -> bool:
    current_credentials = provider._current_credentials()
    if provider.app_password:
        return True
    return bool(
        current_credentials.app_password
        and (
            current_credentials.app_password_expires_at is None
            or _is_future_timestamp(current_credentials.app_password_expires_at)
        )
    )


def _musica_provider_has_usable_api_token(provider: StepOIDCAuthProvider) -> bool:
    if not provider.authentik_api_base:
        return False
    if provider.authentik_api_token:
        return True

    current_credentials = provider._current_credentials()
    return bool(
        current_credentials.api_token
        and (
            current_credentials.api_token_expires_at is None
            or _is_future_timestamp(current_credentials.api_token_expires_at)
        )
    )


def _musica_provider_can_refresh_certificate(provider: StepOIDCAuthProvider) -> bool:
    if not provider.client_id or not provider.username:
        return False

    if _musica_provider_has_usable_app_password(provider):
        return True
    return _musica_provider_has_usable_api_token(provider)


def _musica_existing_certificate_is_usable(provider: StepOIDCAuthProvider) -> bool:
    cert_valid_until = provider._read_cert_valid_until()
    if cert_valid_until is None:
        return False
    return not provider._is_expiring_soon(cert_valid_until)


def _validate_musica_auth_bootstrap(provider: StepOIDCAuthProvider) -> None:
    if _musica_provider_can_refresh_certificate(provider):
        return
    if _musica_existing_certificate_is_usable(provider):
        return

    raise RuntimeError(
        "MUSICA authentication is not bootstrapped. No usable Step SSH certificate "
        "was found, and the current OIDC settings are not enough to mint one "
        "automatically. Configure one of: "
        "(1) an existing valid Step SSH certificate via ASC_STEP_CERT_PATH or "
        "SLURM_EDGE_NODE_KEY_PATH, "
        "(2) ASC_OIDC_CLIENT_ID + ASC_OIDC_USERNAME + ASC_OIDC_APP_PASSWORD, "
        "(3) ASC_OIDC_CLIENT_ID + ASC_OIDC_USERNAME + ASC_AUTHENTIK_CREDENTIALS_FILE "
        "containing current credentials, or "
        "(4) ASC_OIDC_CLIENT_ID + ASC_OIDC_USERNAME + ASC_AUTHENTIK_API_BASE + "
        "ASC_AUTHENTIK_API_TOKEN to bootstrap or recover the credentials file. "
        "See docs/how-to/hpc-musica.md for the MUSICA bootstrap flow."
    )


def build_slurm_resources(config: Dict[str, Any]) -> Dict[str, Any]:
    """Builds Slurm-related Dagster resources from a configuration dictionary."""
    ssh = SSHConnectionResource(**config["ssh_config"])
    queue = SlurmQueueConfig(**config["slurm_queue_config"])
    slurm = SlurmResource(
        ssh=ssh,
        queue=queue,
        remote_base="$HOME/dagster_runs",
    )
    slurm_cfg = config.get("slurm_config", {})
    if "auth_provider" in slurm_cfg:
        slurm.set_auth_provider(slurm_cfg["auth_provider"])

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
        dg.get_dagster_logger().info(f"Dagster deployment environment: {deployment}")
        return environment
    except ValueError as e:
        # 4. Handle the case where the string is not a valid environment
        valid_envs = [e.value for e in Environment]
        raise ValueError(
            f"'{deployment}' is not a valid environment. "
            f"Please set {deployment_key} to one of: {valid_envs}"
        ) from e


def get_resources() -> Dict[str, ComputeResource]:  # noqa: C901
    """
    Builds the Dagster resource dictionary based on the DAGSTER_DEPLOYMENT environment variable.

    This function selects between Docker and Supercomputer base configs, then applies
    staging/production and execution mode modifiers.
    """
    deployment = get_dagster_deployment_environment()
    deployment_name = deployment.value

    # --- Case 1: Local Development ---
    if deployment == Environment.DEVELOPMENT:
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

    # Step 2.1: Select base config based on docker vs supercomputer
    if _is_docker_based(deployment):
        config = copy.deepcopy(DOCKER_SLURM_BASE_CONFIG)
        # Apply production overrides if needed
        if _is_production(deployment):
            _deep_merge(config, PRODUCTION_DOCKER_OVERRIDES)
    elif _is_supercomputer_based(deployment):
        config = copy.deepcopy(SUPERCOMPUTER_SLURM_BASE_CONFIG)
        site_key = os.environ.get("SLURM_SUPERCOMPUTER_SITE", "").strip().lower()
        if site_key:
            site_override = SUPERCOMPUTER_SITE_OVERRIDES.get(site_key)
            if not site_override:
                available_sites = ", ".join(sorted(SUPERCOMPUTER_SITE_OVERRIDES.keys()))
                raise ValueError(
                    f"Unknown SLURM_SUPERCOMPUTER_SITE '{site_key}'. "
                    f"Available options: {available_sites}"
                )
            _deep_merge(config, copy.deepcopy(site_override))
        if site_key == "musica":
            auth_provider = _build_musica_auth_provider()
            _validate_musica_auth_bootstrap(auth_provider)
            if _musica_provider_is_configured(auth_provider):
                config.setdefault("slurm_config", {})
                config["slurm_config"]["auth_provider"] = auth_provider
    else:
        raise ValueError(f"Unexpected environment: {deployment_name}")

    explicit_pack_platform = os.environ.get("SLURM_PACK_PLATFORM", "").strip()
    if explicit_pack_platform:
        compute_cfg = config.setdefault("compute_config", {})
        compute_cfg["auto_detect_platform"] = False
        compute_cfg["pack_platform"] = explicit_pack_platform

    # Allow explicit env overrides for partition / QoS / reservation so they can be matched
    # to whatever window is currently available on the supercomputer (e.g. hackathon slots).
    queue_cfg = config.setdefault("slurm_queue_config", {})
    session_cfg = config.setdefault("slurm_session_config", {})

    def _apply_queue_field(env_var: str, field: str):
        value = os.environ.get(env_var)
        if value is None:
            return
        value = value.strip()
        if value:
            queue_cfg[field] = value
            session_cfg[field] = value
        else:
            queue_cfg.pop(field, None)
            session_cfg.pop(field, None)

    _apply_queue_field("SLURM_SUPERCOMPUTER_PARTITION", "partition")
    _apply_queue_field("SLURM_SUPERCOMPUTER_QOS", "qos")
    _apply_queue_field("SLURM_SUPERCOMPUTER_RESERVATION", "reservation")

    # Apply environment overrides for the SSH connection settings.
    ssh_cfg = config.setdefault("ssh_config", {})
    ssh_cfg["host"] = os.environ.get(
        "SLURM_EDGE_NODE_HOST", ssh_cfg.get("host", "127.0.0.1")
    )
    ssh_cfg["port"] = int(
        os.environ.get("SLURM_EDGE_NODE_PORT", str(ssh_cfg.get("port", 2223)))
    )
    ssh_cfg["user"] = os.environ.get(
        "SLURM_EDGE_NODE_USER", ssh_cfg.get("user", "submitter")
    )
    password_value = os.environ.get("SLURM_EDGE_NODE_PASSWORD")
    if password_value:
        ssh_cfg["password"] = password_value
    else:
        ssh_cfg["password"] = None

    key_value = os.environ.get("SLURM_EDGE_NODE_KEY_PATH")
    if key_value:
        ssh_cfg["key_path"] = key_value
        ssh_cfg["password"] = None
    else:
        ssh_cfg["key_path"] = ssh_cfg.get("key_path")

    # Optional: configure a jump host using SLURM_EDGE_NODE_JUMP_* variables.
    target_host = ssh_cfg.get("host")
    jump_host_env = os.environ.get("SLURM_EDGE_NODE_JUMP_HOST")
    if jump_host_env and target_host not in {"localhost", "127.0.0.1"}:
        jump_config: Dict[str, Any] = {
            "host": jump_host_env,
            "port": int(os.environ.get("SLURM_EDGE_NODE_JUMP_PORT", "22")),
            "user": os.environ.get("SLURM_EDGE_NODE_JUMP_USER", ssh_cfg["user"]),
        }
        jump_key = os.environ.get("SLURM_EDGE_NODE_JUMP_KEY")
        jump_password = os.environ.get("SLURM_EDGE_NODE_JUMP_PASSWORD")
        if jump_key and jump_password:
            raise ValueError(
                "SLURM_EDGE_NODE_JUMP_KEY and SLURM_EDGE_NODE_JUMP_PASSWORD cannot both be set."
            )
        if jump_key:
            jump_config["key_path"] = jump_key
        if jump_password:
            jump_config["password"] = jump_password
        extra_opts = shlex.split(os.environ.get("SLURM_EDGE_NODE_JUMP_OPTS_EXTRA", ""))
        if extra_opts:
            jump_config["extra_opts"] = extra_opts
        jump_force_tty = os.environ.get("SLURM_EDGE_NODE_JUMP_FORCE_TTY")
        if jump_force_tty:
            jump_config["force_tty"] = jump_force_tty.lower() in {"1", "true", "yes"}

        ssh_cfg["jump_host"] = jump_config
    else:
        ssh_cfg.pop("jump_host", None)

    # Jump host obviates any post-login command/forced TTY.
    ssh_cfg.pop("post_login_command", None)
    ssh_cfg.pop("force_tty", None)

    # Step 2.2: Handle pre_deployed_env_path requirement for production
    if _is_production(deployment):
        pre_deployed_env_path = os.environ.get("CI_DEPLOYED_ENVIRONMENT_PATH")
        if not pre_deployed_env_path:
            raise ValueError(
                f"Production environment '{deployment_name}' requires "
                "CI_DEPLOYED_ENVIRONMENT_PATH to be set"
            )
        config["compute_config"]["pre_deployed_env_path"] = pre_deployed_env_path

        # In production, both environment AND payloads are pre-deployed via CI/CD
        # Skip payload upload - paths will be derived as {pre_deployed_env_path}/scripts/{filename}
        config["compute_config"]["default_skip_payload_upload"] = True

    # Step 2.3: Apply execution mode modifiers based on environment name
    # Priority order: hetjob > cluster_reuse > session > default

    if "hetjob" in deployment_name:
        config["mode"] = ExecutionMode.SLURM_HETJOB
        # Cluster reuse is incompatible with HETJOB
        config.get("compute_config", {}).pop("enable_cluster_reuse", None)
        config.get("compute_config", {}).pop("cluster_reuse_tolerance", None)

    elif "cluster_reuse" in deployment_name:
        config["mode"] = ExecutionMode.SLURM_SESSION
        config.setdefault("compute_config", {})
        config["compute_config"]["enable_cluster_reuse"] = True
        config["compute_config"]["cluster_reuse_tolerance"] = 0.2

    elif "session" in deployment_name:
        config["mode"] = ExecutionMode.SLURM_SESSION

    # Default mode is already set to SLURM in base configs

    # Step 2.4: Build the resources from the final config
    slurm_resources = build_slurm_resources(config)
    return build_compute_resources(config, slurm_resources)
