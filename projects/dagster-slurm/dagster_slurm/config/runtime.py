from enum import StrEnum


class RuntimeVariant(StrEnum):
    """Defines the runtime."""

    SHELL = "shell_script"
    RAY = "ray"
    SPARK = "spark"
