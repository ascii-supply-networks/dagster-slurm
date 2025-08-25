from functools import cached_property

import dagster as dg
from dagster_ray import PipesRayJobClient, RayResource
from ray.job_submission import JobSubmissionClient


class PipesRayJobClientLazyLocalResource(dg.ConfigurableResource):
    """
    A resource that provides a PipesRayJobClient.
    It depends on a Ray cluster resource being available.
    """

    ray_cluster: RayResource

    @cached_property
    def pipes_client(self) -> PipesRayJobClient:
        """
        Lazily initializes and returns a PipesRayJobClient connected to the
        provided Ray cluster.
        """
        try:
            import ray

            if ray.is_initialized():  # type: ignore[missing-argument]
                ray_address = ray.get_runtime_context().gcs_address  # type: ignore[missing-argument]
                if ray_address:
                    host = ray_address.split(":")[0]
                    dashboard_address = f"http://{host}:8265"
                else:
                    dashboard_address = "http://127.0.0.1:8265"
            else:
                dashboard_address = "http://127.0.0.1:8265"
        except Exception as e:
            dg.get_dagster_logger().warning(f"Could not get Ray address: {e}")
            dashboard_address = "http://127.0.0.1:8265"

        dg.get_dagster_logger().info(
            f"Connecting PipesRayJobClient to Ray at: {dashboard_address}"
        )
        return PipesRayJobClient(client=JobSubmissionClient(address=dashboard_address))

    def run(self, *args, **kwargs):
        return self.pipes_client.run(*args, **kwargs)
