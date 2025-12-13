from enum import StrEnum
from typing import Optional

import dagster as dg
from pydantic import Field


class RuntimeVariant(StrEnum):
    """Defines the runtime."""

    SHELL = "shell_script"
    RAY = "ray"
    SPARK = "spark"


class SlurmRunConfig(dg.Config):
    """Per-run configuration for Slurm execution.

    Use this to configure environment caching and payload upload behavior
    at job submission time via the Dagster launchpad.

    Example usage in an asset:

        @dg.asset
        def my_asset(
            context: dg.AssetExecutionContext,
            compute: ComputeResource,
            config: SlurmRunConfig,
        ):
            return compute.run(
                context=context,
                payload_path="script.py",
                config=config,
            ).get_results()

    Then in the Dagster launchpad, you can override:
        - force_env_push: True to force re-upload the environment
        - skip_payload_upload: True to skip uploading the payload script
    """

    force_env_push: bool = Field(
        default=False,
        description=(
            "Force repacking and uploading the environment even when a cached copy "
            "exists. Use this when you've changed base dependencies but the cache "
            "key hasn't updated (e.g., after manual changes to injected packages)."
        ),
    )

    skip_payload_upload: bool = Field(
        default=False,
        description=(
            "Skip uploading the payload script. Use this when the payload already "
            "exists on the remote (e.g., pre-deployed code or testing with a "
            "previously uploaded script)."
        ),
    )

    remote_payload_path: Optional[str] = Field(
        default=None,
        description=(
            "Optional remote path to an existing payload when skip_payload_upload=True. "
            "If not provided, the default remote path will be used."
        ),
    )
