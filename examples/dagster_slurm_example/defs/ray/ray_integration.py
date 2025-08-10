import dagster as dg
from dagster_ray import PipesRayJobClient, RayResource


@dg.asset
def ray_minithing(  # noqa:C901
    context: dg.AssetExecutionContext,
    ray_cluster: RayResource,
):
    context.log.info("Hello from Ray minithing")

    import ray

    @ray.remote
    def divide(x: int) -> float:
        if x == 0:
            raise ValueError("Division by zero")
        return 10 / x

    futures = [divide.remote(i) for i in [1, 2, 0, 3]]
    successful_results = []
    failed_tasks = []

    while futures:
        ready, futures = ray.wait(futures, num_returns=1)  # type: ignore[unexpected-keyword]

        for fut in ready:
            try:
                result = ray.get(fut)
                successful_results.append(result)
                context.log.info(f"Task succeeded with result: {result}")
            except Exception as e:
                failed_tasks.append(fut)
                context.log.error(f"A Ray task failed: {e}")

    context.add_output_metadata(
        {
            "num_successful": len(successful_results),
            "num_failed": len(failed_tasks),
        }
    )


@dg.asset
def my_external_asset(
    context: dg.AssetExecutionContext, pipes_ray_job_client: PipesRayJobClient
):
    fp = dg.file_relative_path(__file__, "ray_external.py")
    return pipes_ray_job_client.run(
        context=context,
        submit_job_params={
            "entrypoint": f"python {fp}",
        },
        extras={"param": "value"},
    ).get_results()
