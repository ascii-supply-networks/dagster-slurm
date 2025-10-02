from enum import StrEnum


class RuntimeVariant(StrEnum):
    """
    Defines the runtime
    """

    SHELL = "shell"
    RAY = "ray"
    SPARK = "spark"
