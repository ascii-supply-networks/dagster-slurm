"""Environment-specific path configuration for dagster-slurm deployments.

This module provides location-aware base paths for data storage based on:
- DAGSTER_DEPLOYMENT: deployment environment (development/docker/supercomputer)
- SLURM_SUPERCOMPUTER_SITE: specific HPC site (datalab/musica/vsc5/leonardo)
- DATA environment variable: site-specific data directory

Usage:
    from dagster_slurm_example_shared.paths import get_base_data_path

    # In your asset or config
    base_path = get_base_data_path()  # Returns appropriate path for current environment
"""

import os


def get_base_data_path() -> str:
    """Get the base data path for the current deployment environment.

    Returns appropriate base path based on deployment and site:
    - Local/development: /tmp/dagster_data
    - Docker: $DATA or /data_and_logs
    - Datalab: $HOME/data
    - musica/vsc5/leonardo: $DATA or /scratch (fallback)

    Returns:
        str: Base path for data storage
    """
    deployment = os.getenv("DAGSTER_DEPLOYMENT", "development")
    site = os.getenv("SLURM_SUPERCOMPUTER_SITE", "").lower()

    # Local development
    if deployment == "development":
        return "/tmp/dagster_data"

    # Docker deployments
    if "docker" in deployment:
        return os.getenv("DATA", "/data_and_logs")

    # Supercomputer deployments
    if "supercomputer" in deployment:
        # TU Wien datalab uses $HOME/data
        if site == "datalab":
            home = os.getenv("HOME", "/home/unknown")
            return f"{home}/data"
        # musica, vsc5, leonardo, etc. use $DATA or fallback to /scratch
        else:
            return os.getenv("DATA", "/scratch")

    # Unknown deployment - use temp
    return "/tmp/dagster_data"


def get_base_input_path() -> str:
    """Get the base input path for reading source data.

    Returns appropriate input path based on deployment:
    - Local/development: ./data (relative to project)
    - Docker: $DATA/input or /data_and_logs/input
    - Supercomputer sites: Varies by site

    Returns:
        str: Base path for input data
    """
    deployment = os.getenv("DAGSTER_DEPLOYMENT", "development")
    site = os.getenv("SLURM_SUPERCOMPUTER_SITE", "").lower()

    # Local development - use relative path
    if deployment == "development":
        return "./data"

    # Docker deployments
    if "docker" in deployment:
        data_dir = os.getenv("DATA", "/data_and_logs")
        return f"{data_dir}/input"

    # Supercomputer deployments
    if "supercomputer" in deployment:
        if site == "datalab":
            home = os.getenv("HOME", "/home/unknown")
            return f"{home}/input_data"
        else:
            data_dir = os.getenv("DATA", "/scratch")
            return f"{data_dir}/input"

    # Fallback
    return "./data"


def get_base_output_path() -> str:
    """Get the base output path for writing results.

    Returns appropriate output path based on deployment:
    - Local/development: /tmp/dagster_output
    - Docker: $DATA/output or /data_and_logs/output
    - Supercomputer sites: Varies by site

    Returns:
        str: Base path for output data
    """
    deployment = os.getenv("DAGSTER_DEPLOYMENT", "development")
    site = os.getenv("SLURM_SUPERCOMPUTER_SITE", "").lower()

    # Local development
    if deployment == "development":
        return "/tmp/dagster_output"

    # Docker deployments
    if "docker" in deployment:
        data_dir = os.getenv("DATA", "/data_and_logs")
        return f"{data_dir}/output"

    # Supercomputer deployments
    if "supercomputer" in deployment:
        if site == "datalab":
            home = os.getenv("HOME", "/home/unknown")
            return f"{home}/output"
        else:
            data_dir = os.getenv("DATA", "/scratch")
            return f"{data_dir}/output"

    # Fallback
    return "/tmp/dagster_output"


def get_scratch_path() -> str:
    """Get the scratch/temporary storage path for intermediate files.

    Returns appropriate scratch path based on deployment:
    - Local/development: /tmp/dagster_scratch
    - Docker: /tmp (container temp)
    - Supercomputer sites: $SCRATCH or /tmp

    Returns:
        str: Path for temporary/scratch storage
    """
    deployment = os.getenv("DAGSTER_DEPLOYMENT", "development")

    # Local development
    if deployment == "development":
        return "/tmp/dagster_scratch"

    # Docker deployments - use container /tmp
    if "docker" in deployment:
        return "/tmp"

    # Supercomputer deployments - many HPC sites have $SCRATCH
    if "supercomputer" in deployment:
        # Some sites like leonardo/vsc5 have $SCRATCH env var
        scratch = os.getenv("SCRATCH")
        if scratch:
            return scratch
        # Fallback to /tmp
        return "/tmp"

    # Fallback
    return "/tmp"


# Site-specific configurations
SITE_CONFIGS = {
    "datalab": {
        "description": "TU Wien Datalab cluster",
        "uses_home": True,
        "has_data_env": False,
        "has_scratch_env": False,
    },
    "musica": {
        "description": "Austrian Scientific Computing (ASC) Musica cluster",
        "uses_home": False,
        "has_data_env": True,
        "has_scratch_env": True,
    },
    "vsc5": {
        "description": "Vienna Scientific Cluster 5 (VSC-5)",
        "uses_home": False,
        "has_data_env": True,
        "has_scratch_env": True,
    },
    "leonardo": {
        "description": "Leonardo supercomputer (CINECA)",
        "uses_home": False,
        "has_data_env": True,
        "has_scratch_env": True,
    },
}


def get_site_info(site: str | None = None) -> dict:
    """Get configuration information for a specific HPC site.

    Args:
        site: Site name (e.g., 'datalab', 'musica'). If None, uses SLURM_SUPERCOMPUTER_SITE env var.

    Returns:
        dict: Site configuration with description and capabilities
    """
    if site is None:
        site = os.getenv("SLURM_SUPERCOMPUTER_SITE", "").lower()

    return SITE_CONFIGS.get(
        site,
        {
            "description": "Unknown site",
            "uses_home": False,
            "has_data_env": False,
            "has_scratch_env": False,
        },
    )
