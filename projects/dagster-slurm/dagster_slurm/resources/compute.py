"""Unified compute resource - main facade."""

from typing import Any, Dict, List, Optional, Tuple

from dagster import ConfigurableResource, InitResourceContext, get_dagster_logger
from pydantic import Field, PrivateAttr, model_validator

from ..config.environment import ExecutionMode
from ..helpers.ssh_pool import SSHConnectionPool
from ..launchers.base import ComputeLauncher
from ..managers.hetjob import HeterogeneousJobManager, HetJobComponent
from ..pipes_clients.local_pipes_client import LocalPipesClient
from ..pipes_clients.slurm_pipes_client import SlurmPipesClient
from .session import SlurmSessionResource
from .slurm import SlurmResource


class ComputeResource(ConfigurableResource):
    """Unified compute resource - adapts to deployment.

    This is the main facade that assets depend on.
    Hides complexity of local vs Slurm vs session execution.

    Usage:
        @asset
        def my_asset(context: AssetExecutionContext, compute: ComputeResource):
            return compute.run(
                context=context,
                payload_path="script.py",
                launcher=RayLauncher(num_gpus_per_node=2)
            )

    Configuration Examples:

    Local mode (dev):
        compute = ComputeResource(mode="local")

    Slurm per-asset mode (staging):
        slurm = SlurmResource.from_env()
        compute = ComputeResource(mode="slurm", slurm=slurm)

    Slurm session mode with cluster reuse (prod):
        slurm = SlurmResource.from_env()
        session = SlurmSessionResource(slurm=slurm, num_nodes=10)
        compute = ComputeResource(
            mode="slurm-session",
            slurm=slurm,
            session=session,
            enable_cluster_reuse=True,
            cluster_reuse_tolerance=0.2,
        )

    Heterogeneous job mode (optimal resource allocation):
        compute = ComputeResource(
            mode="slurm-hetjob",
            slurm=slurm,
        )
    """

    mode: ExecutionMode = Field(
        description="Execution mode: 'local', 'slurm', 'slurm-session', or 'slurm-hetjob'"
    )

    # Optional resources (mode-dependent)
    slurm: Optional[SlurmResource] = Field(
        default=None, description="Slurm config (required for slurm modes)"
    )

    session: Optional[SlurmSessionResource] = Field(
        default=None, description="Session resource (required for slurm-session mode)"
    )

    # Launcher configuration
    default_launcher: ComputeLauncher = Field(
        description="The default launcher to use if not overridden on per asset basis",
    )

    # Debug and platform settings
    debug_mode: bool = Field(
        default=False, description="If True, never cleanup remote files (for debugging)"
    )

    cleanup_on_failure: bool = Field(
        default=True,
        description="Whether to cleanup remote files on failure (ignored if debug_mode=True)",
    )

    auto_detect_platform: bool = Field(
        default=True,
        description="Auto-detect platform (ARM vs x86) for pixi pack command",
    )

    pack_platform: Optional[str] = Field(
        default=None,
        description="Override platform for pack command: 'linux-64', 'linux-aarch64', 'osx-arm64'",
    )
    pre_deployed_env_path: Optional[str] = Field(
        default=None,
        description="If set, uses a pre-deployed environment at this path instead of live packaging.",
    )

    # Cluster reuse settings (session mode only)
    enable_cluster_reuse: bool = Field(
        default=False,
        description="Enable Ray/Spark cluster reuse across assets in session mode",
    )

    cluster_reuse_tolerance: float = Field(
        default=0.2,
        description="Resource tolerance for cluster reuse (0.2 = 20% difference allowed)",
    )

    # Private state for cluster management
    _active_clusters: Dict[str, Dict[str, Any]] = PrivateAttr(default_factory=dict)

    @model_validator(mode="after")
    def validate_configuration(self) -> "ComputeResource":
        """Validate configuration - runs during Pydantic validation."""
        # Validate mode-specific requirements
        if (
            self.mode
            in (
                ExecutionMode.SLURM,
                ExecutionMode.SLURM_SESSION,
                ExecutionMode.SLURM_HETJOB,
            )
            and not self.slurm
        ):
            raise ValueError(f"slurm resource required for mode={self.mode}")

        if self.mode == ExecutionMode.SLURM_SESSION and not self.session:
            raise ValueError("session resource required for mode=slurm-session")

        # Validate cluster reuse only works in session mode
        if self.enable_cluster_reuse and self.mode != ExecutionMode.SLURM_SESSION:
            raise ValueError("enable_cluster_reuse only works in slurm-session mode")

        return self

    def _log_configuration_once(self):
        """Log configuration info exactly once per instance."""
        logger = get_dagster_logger()

        # Log debug mode warning
        if self.debug_mode:
            logger.warning("🐛 DEBUG MODE ENABLED: Remote files will NOT be cleaned up")

        # Log cluster reuse settings
        if self.enable_cluster_reuse:
            logger.info(
                f"✨ Cluster reuse enabled with {self.cluster_reuse_tolerance * 100:.0f}% tolerance"
            )

        # Log heterogeneous job mode
        if self.mode == ExecutionMode.SLURM_HETJOB:
            logger.info(
                "🎯 Heterogeneous job mode: optimal per-asset resource allocation"
            )

        # Log platform settings
        if self.mode in (
            ExecutionMode.SLURM,
            ExecutionMode.SLURM_SESSION,
            ExecutionMode.SLURM_HETJOB,
        ):
            if self.pack_platform:
                logger.debug(f"Pack platform explicitly set to: {self.pack_platform}")
            elif self.auto_detect_platform:
                logger.debug("Pack platform will be auto-detected")
            else:
                logger.debug("Using default pack command (linux-64)")

    def get_pipes_client(
        self,
        context: InitResourceContext,
        launcher: Optional[ComputeLauncher] = None,
    ):
        """Get appropriate Pipes client for this mode.

        Args:
            context: Dagster resource context
            launcher: Override launcher (uses default if None)

        Returns:
            LocalPipesClient or SlurmPipesClient

        """
        # Resolve launcher with fallback to default
        effective_launcher = launcher or self.default_launcher

        # Ensure we have a launcher
        if effective_launcher is None:
            raise ValueError(
                "No launcher available. Either provide a launcher override or set default_launcher."
            )

        if self.mode == ExecutionMode.LOCAL:
            # Local mode: no SSH, no Slurm
            return LocalPipesClient(launcher=effective_launcher)

        elif self.mode == ExecutionMode.SLURM:
            # Per-asset mode: each asset = separate sbatch job
            return SlurmPipesClient(
                slurm_resource=self.slurm,  # type: ignore
                launcher=effective_launcher,
                session_resource=None,  # No session
                cleanup_on_failure=self.cleanup_on_failure,
                debug_mode=self.debug_mode,
                auto_detect_platform=self.auto_detect_platform,
                pack_platform=self.pack_platform,
                pre_deployed_env_path=self.pre_deployed_env_path,
            )

        elif self.mode == ExecutionMode.SLURM_SESSION:
            # Session mode: shared allocation, operator fusion
            # Initialize session if not already done
            if not self.session._initialized:  # type: ignore
                self.session.setup_for_execution(context)  # type: ignore

            return SlurmPipesClient(
                slurm_resource=self.slurm,  # type: ignore
                launcher=effective_launcher,
                session_resource=self.session,
                cleanup_on_failure=self.cleanup_on_failure,
                debug_mode=self.debug_mode,
                auto_detect_platform=self.auto_detect_platform,
                pack_platform=self.pack_platform,
                pre_deployed_env_path=self.pre_deployed_env_path,
            )

        else:  # ExecutionMode.SLURM_HETJOB
            # Heterogeneous job mode: handled by run_hetjob()
            return SlurmPipesClient(
                slurm_resource=self.slurm,  # type: ignore
                launcher=effective_launcher,
                session_resource=None,
                cleanup_on_failure=self.cleanup_on_failure,
                debug_mode=self.debug_mode,
                auto_detect_platform=self.auto_detect_platform,
                pack_platform=self.pack_platform,
                pre_deployed_env_path=self.pre_deployed_env_path,
            )

    def run(
        self,
        context,
        payload_path: str,
        launcher: Optional[ComputeLauncher] = None,
        extra_slurm_opts: Optional[Dict[str, Any]] = None,
        resource_requirements: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        """Execute asset with optional resource overrides.

        Args:
            context: Dagster execution context
            payload_path: Path to Python script
            launcher: Override launcher for this asset
            extra_slurm_opts: Override Slurm options (non-session mode)
                - nodes: int
                - cpus_per_task: int
                - mem: str (e.g., "32G")
                - gpus_per_node: int
                - time_limit: str (e.g., "02:00:00")
            resource_requirements: Resource requirements for cluster reuse (session mode)
                - cpus: int
                - gpus: int
                - memory_gb: int
                - framework: str ("ray" or "spark")
            **kwargs: Passed to client.run()

        Yields:
            Dagster events

        Examples:
            .. code-block:: python

                # Simple execution with default resources
                yield from compute.run(context, "script.py")

            .. code-block:: python

                # Override launcher for this asset
                ray_launcher = RayLauncher(num_gpus_per_node=4)
                yield from compute.run(context, "script.py", launcher=ray_launcher)

            .. code-block:: python

                # Non-session mode: override Slurm resources
                yield from compute.run(
                    context,
                    "script.py",
                    extra_slurm_opts={"nodes": 1, "cpus_per_task": 16, "mem": "64G", "gpus_per_node": 2}
                )

            .. code-block:: python

                # Session mode: specify resource requirements for cluster reuse
                yield from compute.run(
                    context,
                    "script.py",
                    launcher=RayLauncher(num_gpus_per_node=2),
                    resource_requirements={"cpus": 32, "gpus": 2, "memory_gb": 128, "framework": "ray"}
                )

        """
        self._log_configuration_once()
        logger = get_dagster_logger()

        # Determine effective launcher
        effective_launcher = launcher or self.default_launcher

        # Handle cluster reuse in session mode
        if self.enable_cluster_reuse and resource_requirements:
            cluster_address = self._get_or_create_cluster(
                launcher=effective_launcher,
                requirements=resource_requirements,
                context=context,
            )

            if cluster_address:
                logger.info(f"♻️  Reusing existing cluster: {cluster_address}")
                # Update launcher to connect to existing cluster
                if hasattr(effective_launcher, "ray_address"):
                    # Ray launcher
                    effective_launcher = effective_launcher.model_copy(  # type: ignore
                        update={"ray_address": cluster_address}
                    )
                elif hasattr(effective_launcher, "master_url"):
                    # Spark launcher
                    effective_launcher = effective_launcher.model_copy(  # type: ignore
                        update={"master_url": cluster_address}
                    )

        # Create client with effective launcher (default or override)
        client = self.get_pipes_client(context, launcher=effective_launcher)

        # Pass extra Slurm options to the client
        if extra_slurm_opts:
            kwargs["extra_slurm_opts"] = extra_slurm_opts
            logger.debug(f"Passing extra_slurm_opts to client: {extra_slurm_opts}")

        # Add use_session flag for session mode
        if self.mode == ExecutionMode.SLURM_SESSION:
            kwargs.setdefault("use_session", True)

        # Run with the configured client
        yield from client.run(context=context, payload_path=payload_path, **kwargs)

    def run_hetjob(
        self,
        context,
        assets: List[Tuple[str, str, Dict[str, Any]]],
        launchers: Optional[Dict[str, ComputeLauncher]] = None,
    ):
        """Run multiple assets as a heterogeneous Slurm job.

        Submit all assets together with their specific resource requirements.
        Only waits in queue ONCE, but each asset gets the resources it needs.

        Args:
            context: Dagster execution context
            assets: List of (asset_key, payload_path, resource_requirements)
                resource_requirements:
                    - nodes: int (default: 1)
                    - cpus_per_task: int (default: 2)
                    - mem: str (default: "4G")
                    - gpus_per_node: int (default: 0)
                    - time_limit: str (default: "01:00:00")
            launchers: Optional dict mapping asset_key to ComputeLauncher

        Yields:
            Dagster events

        Example:
            .. code-block:: python

                compute.run_hetjob(
                    context,
                    assets=[
                        ("prep", "prep.py", {"nodes": 1, "cpus_per_task": 8, "mem": "32G"}),
                        ("train", "train.py", {"nodes": 4, "cpus_per_task": 32, "mem": "128G", "gpus_per_node": 2}),
                        ("infer", "infer.py", {"nodes": 8, "cpus_per_task": 16, "mem": "64G", "gpus_per_node": 1}),
                    ],
                    launchers={
                        "train": RayLauncher(num_gpus_per_node=2),
                        "infer": RayLauncher(num_gpus_per_node=1),
                    }
                )

        """
        if self.mode != ExecutionMode.SLURM_HETJOB:
            raise ValueError("run_hetjob only supported in slurm-hetjob mode")
        self._log_configuration_once()

        logger = get_dagster_logger()
        logger.info(f"🎯 Submitting heterogeneous job with {len(assets)} components")

        import uuid

        # Prepare work directory
        run_id = context.run_id or uuid.uuid4().hex
        working_dir = f"{self.slurm.remote_base}/hetjobs/{run_id}"  # type: ignore

        with SSHConnectionPool(self.slurm.ssh) as ssh_pool:  # type: ignore
            # Create working directory
            ssh_pool.run(f"mkdir -p {working_dir}")

            # Pack environment once (shared by all components)
            logger.info("Packing environment with pixi...")
            from ..helpers.env_packaging import pack_environment_with_pixi

            pack_cmd = (
                self._get_pack_command()
                if hasattr(self, "_get_pack_command")
                else ["pixi", "run", "--frozen", "pack"]
            )
            pack_file = pack_environment_with_pixi(pack_cmd=pack_cmd)

            # Upload packed environment
            remote_pack_file = f"{working_dir}/{pack_file.name}"
            ssh_pool.upload_file(str(pack_file.absolute()), remote_pack_file)

            # Extract environment
            env_dir = f"{working_dir}/env"
            ssh_pool.run(f"mkdir -p {env_dir}")
            ssh_pool.run(f"chmod +x {remote_pack_file}")

            # Extract
            logger.info("Extracting environment...")
            extract_cmd = f"cd {working_dir} && {remote_pack_file}"
            ssh_pool.run(extract_cmd, timeout=600)

            # Find activation script
            activation_script = f"{working_dir}/activate.sh"
            python_executable = f"{env_dir}/bin/python"

            # Convert assets to HetJobComponents and prepare scripts
            components = []
            for asset_key, payload_path, resources in assets:
                # Get launcher for this asset
                launcher = (launchers or {}).get(asset_key, self.default_launcher)

                # Upload payload
                remote_payload = f"{working_dir}/{asset_key}_payload.py"
                ssh_pool.upload_file(payload_path, remote_payload)

                # Generate execution script for this component

                # Create fake pipes context for this component
                pipes_context = {
                    "DAGSTER_PIPES_CONTEXT": "{}",  # Minimal context
                    "DAGSTER_PIPES_MESSAGES": f"{working_dir}/{asset_key}_messages.jsonl",
                }

                execution_plan = launcher.prepare_execution(  # type: ignore
                    payload_path=remote_payload,
                    python_executable=python_executable,
                    working_dir=working_dir,
                    pipes_context=pipes_context,
                    activation_script=activation_script,
                )

                # Write component script
                script_path = f"{working_dir}/{asset_key}_script.sh"
                script_content = "\n".join(execution_plan.payload)
                ssh_pool.write_file(script_content, script_path)
                ssh_pool.run(f"chmod +x {script_path}")

                component = HetJobComponent(
                    asset_key=asset_key,
                    nodes=resources.get("nodes", 1),
                    cpus_per_task=resources.get("cpus_per_task", 2),
                    mem=resources.get("mem", "4G"),
                    gpus_per_node=resources.get("gpus_per_node", 0),
                    time_limit=resources.get("time_limit", "01:00:00"),
                    script_path=script_path,
                    partition=resources.get("partition"),
                )
                components.append(component)

                logger.info(
                    f"  📦 Component {asset_key}: "
                    f"{component.nodes} nodes, {component.cpus_per_task} CPUs, "
                    f"{component.mem} mem"
                    + (
                        f", {component.gpus_per_node} GPUs"
                        if component.gpus_per_node > 0
                        else ""
                    )
                )

            # Create heterogeneous job manager
            manager = HeterogeneousJobManager(
                slurm_resource=self.slurm,  # type: ignore
                ssh_pool=ssh_pool,
                working_dir=working_dir,
            )

            # Submit heterogeneous job
            job_name = f"dagster_hetjob_{run_id[:8]}"
            job_id = manager.submit_heterogeneous_job(
                components=components,
                job_name=job_name,
            )

            # Wait for completion
            try:
                manager.wait_for_completion(
                    job_id=job_id,
                    poll_interval=5,
                    timeout=None,  # No timeout
                )

                logger.info(f"✅ Heterogeneous job {job_id} completed successfully")

                # Retrieve component logs
                component_logs = manager.get_component_logs(job_id)
                for comp_id, log_content in component_logs.items():
                    if comp_id < len(components):
                        asset_key = components[comp_id].asset_key
                        logger.debug(
                            f"Component {comp_id} ({asset_key}) log:\n{log_content}"
                        )

            except Exception as e:
                logger.error(f"❌ Heterogeneous job {job_id} failed: {e}")

                if not self.debug_mode and self.cleanup_on_failure:
                    try:
                        ssh_pool.run(f"rm -rf {working_dir}")
                        logger.info(f"Cleaned up working directory: {working_dir}")
                    except Exception as cleanup_error:
                        logger.warning(f"Cleanup failed: {cleanup_error}")
                else:
                    logger.info(
                        f"Working directory preserved for debugging: {working_dir}"
                    )

                raise

            # Cleanup on success (unless debug mode)
            if not self.debug_mode:
                try:
                    ssh_pool.run(f"rm -rf {working_dir}")
                    logger.info(f"Cleaned up working directory: {working_dir}")
                except Exception as cleanup_error:
                    logger.warning(f"Cleanup failed: {cleanup_error}")

    def _get_or_create_cluster(
        self,
        launcher: Optional[ComputeLauncher],
        requirements: Dict[str, Any],
        context,
    ) -> Optional[str]:
        """Get existing cluster if resources match, otherwise return None to create new.

        Args:
            launcher: Launcher for this asset
            requirements: Resource requirements dict
            context: Dagster context

        Returns:
            Cluster address if reusable cluster found, None otherwise

        """
        if not self.enable_cluster_reuse:
            return None

        framework = requirements.get("framework", "unknown")
        required_cpus = requirements.get("cpus", 0)
        required_gpus = requirements.get("gpus", 0)
        required_memory_gb = requirements.get("memory_gb", 0)

        logger = get_dagster_logger()

        # Check active clusters for compatible match
        for cluster_id, info in self._active_clusters.items():
            if info["framework"] != framework:
                continue

            # Check resource compatibility
            cpu_diff = abs(info["cpus"] - required_cpus) / max(required_cpus, 1)
            mem_diff = abs(info["memory_gb"] - required_memory_gb) / max(
                required_memory_gb, 1
            )
            gpu_match = info["gpus"] == required_gpus  # GPUs must match exactly

            if (
                cpu_diff <= self.cluster_reuse_tolerance
                and mem_diff <= self.cluster_reuse_tolerance
                and gpu_match
            ):
                logger.info(
                    f"♻️  Found compatible {framework} cluster: "
                    f"CPUs {info['cpus']} (±{cpu_diff * 100:.1f}%), "
                    f"GPUs {info['gpus']}, "
                    f"Memory {info['memory_gb']}GB (±{mem_diff * 100:.1f}%)"
                )
                return info["address"]

        # No compatible cluster found
        logger.info(f"🆕 No compatible {framework} cluster found, will create new")
        return None

    def register_cluster(
        self,
        cluster_address: str,
        framework: str,
        cpus: int,
        gpus: int,
        memory_gb: int,
    ):
        """Register a newly created cluster for future reuse.

        Args:
            cluster_address: Address of the cluster (e.g., "10.0.0.1:6379")
            framework: "ray" or "spark"
            cpus: Total CPUs in cluster
            gpus: Total GPUs in cluster
            memory_gb: Total memory in GB

        """
        if not self.enable_cluster_reuse:
            return

        import uuid

        cluster_id = f"{framework}_{uuid.uuid4().hex[:8]}"

        self._active_clusters[cluster_id] = {
            "framework": framework,
            "address": cluster_address,
            "cpus": cpus,
            "gpus": gpus,
            "memory_gb": memory_gb,
        }

        logger = get_dagster_logger()
        logger.info(
            f"📝 Registered {framework} cluster {cluster_id}: "
            f"{cpus} CPUs, {gpus} GPUs, {memory_gb}GB"
        )

    def teardown(self, context: InitResourceContext):
        """Teardown method called by Dagster at end of run.
        Ensures session resources and clusters are cleaned up.
        """
        logger = get_dagster_logger()

        # Cleanup registered clusters
        if self._active_clusters:
            logger.info(f"🧹 Cleaning up {len(self._active_clusters)} clusters...")
            for cluster_id, info in self._active_clusters.items():
                logger.info(f"  - Stopping {info['framework']} cluster {cluster_id}")
                # Clusters are cleaned up automatically by session shutdown

        # Teardown session
        if (
            self.mode == ExecutionMode.SLURM_SESSION
            and self.session
            and self.session._initialized
        ):
            self.session.teardown_after_execution(context)
