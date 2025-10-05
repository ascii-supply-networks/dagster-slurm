from enum import StrEnum


class Environment(StrEnum):
    DEVELOPMENT = "development"
    STAGING_DOCKER = "staging_docker"
    PRODUCTION_DOCKER = "production_docker"
    PRODUCTION_DOCKER_SESSION = "production_docker_session"
    PRODUCTION_DOCKER_SESSION_CLUSTER_REUSE = "production_docker_session_cluster_reuse"
    PRODUCTION_DOCKER_HETJOB = "production_docker_hetjob"
    PRODUCTION_SUPERCOMPUTER = "production_supercomputer"
    PRODUCTION_SUPERCOMPUTER_SESSION = "production_supercomputer_session"
    PRODUCTION_SUPERCOMPUTER_SESSION_CLUSTER_REUSE = (
        "production_supercomputer_session_cluster_reuse"
    )
    PRODUCTION_SUPERCOMPUTER_HETJOB = "production_supercomputer_hetjob"


class ExecutionMode(StrEnum):
    """Defines the execution modes for the application.
    Members of this enum behave like strings.
    """

    LOCAL = "local"
    SLURM = "slurm"
    SLURM_SESSION = "slurm-session"
    SLURM_HETJOB = "slurm-hetjob"
