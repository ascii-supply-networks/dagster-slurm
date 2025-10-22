from functools import cached_property

import dagster as dg
from dagster_ray import PipesRayJobClient, RayResource
from ray.job_submission import JobSubmissionClient
import os

class PipesRayJobClientLazyLocalResource(dg.ConfigurableResource):
    """A resource that provides a PipesRayJobClient.
    It depends on a Ray cluster resource being available.
    """

    ray_cluster: RayResource
    jobs_api_url: str | None = None 
    
    @cached_property
    def pipes_client(self) -> PipesRayJobClient:
        """Lazily initializes and returns a PipesRayJobClient connected to the
        provided Ray cluster.
        """
        addr = (
            self.jobs_api_url
            or os.environ.get("RAY_JOB_SUBMISSION_ADDRESS")
            or os.environ.get("RAY_DASHBOARD_ADDRESS")
        )
        if not addr:
            # Try to derive from RAY_ADDRESS if it looks like host:port or http://...
            env_addr = os.environ.get("RAY_ADDRESS")
            if env_addr:
                if env_addr.startswith("http"):
                    addr = env_addr
                elif ":" in env_addr:
                    host = env_addr.split(":", 1)[0]
                    addr = f"http://{host}:8265"
            try:
                import ray
                if ray.is_initialized():
                    gcs = ray.get_runtime_context().gcs_address  # type: ignore[attr-defined]
                    host = gcs.split(":", 1)[0] if gcs else "127.0.0.1"
                    addr = f"http://{host}:8265"
            except Exception as e:
                dg.get_dagster_logger().warning(f"Could not infer Ray Jobs address: {e}")
                addr = "http://127.0.0.1:8265"

        dg.get_dagster_logger().info(f"Using Ray Jobs API at: {addr}")
        return PipesRayJobClient(client=JobSubmissionClient(address=addr))

    def run(self, *args, **kwargs):
        return self.pipes_client.run(*args, **kwargs)
