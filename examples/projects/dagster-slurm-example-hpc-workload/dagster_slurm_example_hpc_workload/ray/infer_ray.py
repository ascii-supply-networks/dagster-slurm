"""Example Ray inference script."""

import os
import time

import ray
from dagster_pipes import PipesContext, open_dagster_pipes


@ray.remote
def run_inference_batch(batch_id: int):
    """Simulate distributed inference."""
    return f"Processed batch {batch_id}"


def main():  # noqa: C901
    context = PipesContext.get()
    context.log.info("Starting Ray inference...")
    print("hello from inference")
    model_path = os.environ.get("MODEL_PATH")
    context.log.info(f"Loading model from: {model_path}")

    if not ray.is_initialized():  # type: ignore
        context.log.info("Connecting to Ray cluster...")

        # Get connection parameters from environment
        ray_address = os.environ.get("RAY_ADDRESS", "auto")
        node_ip = os.environ.get("RAY_NODE_IP_ADDRESS")

        # Build init kwargs
        init_kwargs = {
            "address": ray_address,
            "logging_level": "INFO",
        }

        if node_ip:
            init_kwargs["_node_ip_address"] = node_ip
            context.log.debug(f"Using node IP: {node_ip}")

        context.log.info(f"Try Connecting to Ray at: {ray_address}")

        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                ray.init(**init_kwargs)
                context.log.info("Connected to Ray cluster.")
                break
            except Exception as e:
                if attempt < max_attempts:
                    context.log.warning(
                        f"Connection attempt {attempt} failed: {e}. "
                        f"Retrying in 10 seconds..."
                    )
                    time.sleep(10)
                else:
                    context.log.error(
                        f"Failed to connect after {max_attempts} attempts"
                    )
                    raise

    # Distributed inference
    num_batches = 20
    futures = [run_inference_batch.remote(i) for i in range(num_batches)]
    results = ray.get(futures)
    context.log.info(f"Processed {len(results)} batches")
    context.report_asset_materialization(
        metadata={
            "num_batches": num_batches,
            "framework": "ray",
            "predictions_path": "/path/to/predictions",
        }
    )
    context.log.info("Inference complete!")


if __name__ == "__main__":
    with open_dagster_pipes() as context:
        main()
