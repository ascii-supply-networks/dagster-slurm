import os
from functools import cached_property
from typing import Literal, Optional

import dagster as dg
from dagster_ray import PipesRayJobClient, RayResource
from ray.job_submission import JobSubmissionClient


class PipesRayJobClientLazyLocalResource(dg.ConfigurableResource):
    """A resource that provides a PipesRayJobClient it connects to the Ray Job Submission server.
    It prefers the launcher-exported env var RAY_DASHBOARD_ADDRESS.
    If missing, it derives the URL from RAY_ADDRESS plus dashboard_port/port_strategy.
    """

    ray_cluster: RayResource
    dashboard_port: int = 8265
    port_strategy: Literal["fixed", "hash_jobid"] = "fixed"

    @cached_property
    def pipes_client(self) -> PipesRayJobClient:
        """Lazily initializes and returns a PipesRayJobClient connected to the
        provided Ray cluster.
        """
        addr = self._discover_job_server()
        dg.get_dagster_logger().info(f"Connecting PipesRayJobClient to Ray at: {addr}")
        return PipesRayJobClient(client=JobSubmissionClient(address=addr))

    def _discover_job_server(self) -> str:
        # 1. Launcher exported the exact URL
        dash = os.getenv("RAY_DASHBOARD_ADDRESS")
        if dash:
            return (
                dash if dash.startswith(("http://", "https://")) else f"http://{dash}"
            )
        # 2. derive host from RAY_ADDRESS
        host = self._host_from_ray_address(os.getenv("RAY_ADDRESS"))
        # 3. Decide port
        port = self._dashboard_port()
        return f"http://{host}:{port}"

    def _host_from_ray_address(
        self, ray_addr: Optional[str], default: str = "127.0.0.1"
    ) -> str:
        if not ray_addr:
            return default
        no_scheme = ray_addr.split("://", 1)[-1]
        host_part = no_scheme.rsplit(":", 1)[0]
        host = host_part.strip("[]")
        return host or default

    def _dashboard_port(self) -> int:
        port = int(self.dashboard_port)
        if getattr(self, "port_strategy", "fixed") == "hash_jobid":
            jobid = os.getenv("SLURM_JOB_ID")
            if jobid and jobid.isdigit():
                port += int(jobid) % 1000
        return port

    def run(self, *args, **kwargs):
        return self.pipes_client.run(*args, **kwargs)
