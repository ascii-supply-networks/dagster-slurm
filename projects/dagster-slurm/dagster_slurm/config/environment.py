from enum import StrEnum


class Environment(StrEnum):
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


class ExecutionMode(StrEnum):
    """Defines the execution modes for the application.
    Members of this enum behave like strings.
    """

    LOCAL = "local"
    SLURM = "slurm"
    SLURM_SESSION = "slurm-session"
    SLURM_HETJOB = "slurm-hetjob"
