from functools import cached_property

import dagster as dg
from dagster_ray import PipesRayJobClient, RayResource
from ray.job_submission import JobSubmissionClient
from typing import Literal, Optional
import os
import re

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
            if not re.match(r"^https?://", dash):
                dash = f"http://{dash}"
            return dash

        # 22 derive host from RAY_ADDRESS
        ray_addr = os.getenv("RAY_ADDRESS", "")
        host = "127.0.0.1"
        if ray_addr:
            if "://" in ray_addr:
                ray_addr = ray_addr.split("://", 1)[1]
            host = ray_addr.rsplit(":", 1)[0].strip("[]") or "127.0.0.1"

        # 3) Decide port
        port = int(self.dashboard_port)
        if self.port_strategy == "hash_jobid":
            jobid = os.getenv("SLURM_JOB_ID", "")
            if jobid.isdigit():
                port += (int(jobid) % 1000)

        return f"http://{host}:{port}"


    def run(self, *args, **kwargs):
        return self.pipes_client.run(*args, **kwargs)
