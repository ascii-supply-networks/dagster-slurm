"""Unified compute resource - main facade."""

from typing import Optional, Literal
from dagster import ConfigurableResource, get_dagster_logger
from pydantic import Field
from regex import R
from .slurm import SlurmResource
from .session import SlurmSessionResource
from ..launchers.base import ComputeLauncher
from ..launchers.script import BashLauncher
from ..pipes_clients.local_pipes_client import LocalPipesClient
from ..pipes_clients.slurm_pipes_client import SlurmPipesClient
from ..config.environment import ExecutionMode
from ..config.runtime import RuntimeVariant
import os
from typing import Optional, Literal
from dagster import ConfigurableResource, InitResourceContext, get_dagster_logger
from pydantic import Field
from .slurm import SlurmResource
from ..launchers.base import ComputeLauncher
from ..pipes_clients.local_pipes_client import LocalPipesClient
from ..pipes_clients.slurm_pipes_client import SlurmPipesClient


# dagster_slurm/resources/compute.py (COMPLETE - CORRECTED)

"""Unified compute resource - main facade."""
import os
from typing import Optional, Literal
from dagster import ConfigurableResource, InitResourceContext, get_dagster_logger
from pydantic import Field
from .slurm import SlurmResource
from .session import SlurmSessionResource
from ..launchers.base import ComputeLauncher
from ..pipes_clients.local_pipes_client import LocalPipesClient
from ..pipes_clients.slurm_pipes_client import SlurmPipesClient


class ComputeResource(ConfigurableResource):
    """
    Unified compute resource - adapts to deployment.

    This is the main facade that assets depend on.
    Hides complexity of local vs Slurm vs session execution.

    Usage:
        @asset
        def my_asset(context: AssetExecutionContext, compute: ComputeResource):
            return compute.run(
                context=context,
                payload_path="script.py",
                launcher_type="bash"
            )

    Configuration Examples:

    Local mode (dev):
        compute = ComputeResource(mode="local")

    Slurm per-asset mode (staging):
        slurm = SlurmResource.from_env()
        compute = ComputeResource(mode="slurm", slurm=slurm)

    Slurm session mode (prod):
        slurm = SlurmResource.from_env()
        session = SlurmSessionResource(slurm=slurm, num_nodes=4)
        compute = ComputeResource(mode="slurm-session", slurm=slurm, session=session)
    """

    mode: ExecutionMode = Field(
        description="Execution mode: 'local', 'slurm', or 'slurm-session'"
    )

    # Optional resources (mode-dependent)
    slurm: Optional[SlurmResource] = Field(
        default=None, description="Slurm config (required for slurm modes)"
    )

    session: Optional[SlurmSessionResource] = Field(
        default=None, description="Session resource (required for slurm-session mode)"
    )

    # Launcher configuration
    default_launcher: Optional[ComputeLauncher] = Field(
        default=None, description="Default launcher (auto-created if None)"
    )

    def model_post_init(self, __context):
        """Validate configuration after Pydantic init."""
        # Validate mode-specific requirements
        if (
            self.mode in (ExecutionMode.SLURM, ExecutionMode.SLURM_SESSION)
            and not self.slurm
        ):
            raise ValueError(f"slurm resource required for mode={self.mode}")

        if self.mode == ExecutionMode.SLURM_SESSION and not self.session:
            raise ValueError("session resource required for mode=slurm-session")

        # Create default launcher if not provided
        if not self.default_launcher:
            activate_sh = self.slurm.activate_sh if self.slurm else None
            self.default_launcher = BashLauncher(activate_sh=activate_sh)

    def get_pipes_client(
        self,
        context: InitResourceContext,
        launcher: Optional[ComputeLauncher] = None,
    ):
        """
        Get appropriate Pipes client for this mode.

        Args:
            context: Dagster resource context
            launcher: Override launcher (uses default if None)

        Returns:
            LocalPipesClient or SlurmPipesClient
        """
        launcher = launcher or self.default_launcher

        if self.mode == "local":
            # Local mode: no SSH, no Slurm
            return LocalPipesClient(launcher=launcher)

        elif self.mode == "slurm":
            # Per-asset mode: each asset = separate sbatch job
            return SlurmPipesClient(
                slurm_resource=self.slurm,
                launcher=launcher,
                session_resource=None,  # No session
            )

        else:  # slurm-session
            # Session mode: shared allocation, operator fusion
            # Initialize session if not already done
            if not self.session._initialized:
                self.session.setup_for_execution(context)

            return SlurmPipesClient(
                slurm_resource=self.slurm,
                launcher=launcher,
                session_resource=self.session,
            )

    def run(
        self,
        context,
        payload_path: str,
        launcher: Optional[ComputeLauncher] = None,
        **kwargs,
    ):
        """
        Convenience method - get client and run.

        Args:
            context: Dagster execution context
            payload_path: Path to Python script
            launcher: Override launcher
            **kwargs: Passed to client.run()

        Yields:
            Dagster events
        """
        client = self.get_pipes_client(context, launcher=launcher)

        # Add use_session flag for session mode
        if self.mode == ExecutionMode.SLURM_SESSION:
            kwargs.setdefault("use_session", True)

        yield from client.run(context=context, payload_path=payload_path, **kwargs)

    def teardown(self, context: InitResourceContext):
        """
        Teardown method called by Dagster at end of run.
        Ensures session resources are cleaned up.
        """
        if (
            self.mode == ExecutionMode.SLURM_SESSION
            and self.session
            and self.session._initialized
        ):
            self.session.teardown_after_execution(context)
