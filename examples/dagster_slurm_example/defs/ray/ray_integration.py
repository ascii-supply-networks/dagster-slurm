import dagster as dg
from dagster_ray import RayResource


@dg.asset
def ray_minithing(  # noqa:C901
    context: dg.AssetExecutionContext,
    ray_cluster: RayResource,
):
    print("********!!!!!!")
    # TODO: Why is this never logged?
    # TODO: Why is ray never starting?
    context.log.info("Hello from Ray minithing")

    import ray

    with ray_cluster.get_context():

        @ray.remote
        def divide(x: int) -> float:
            if x == 0:
                raise ValueError("Division by zero")
            return 10 / x

        futures = [divide.remote(i) for i in [1, 2, 0, 3]]
        successful_results = []
        failed_tasks = []

        while futures:
            ready, futures = ray.wait(futures, num_returns=1)

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
