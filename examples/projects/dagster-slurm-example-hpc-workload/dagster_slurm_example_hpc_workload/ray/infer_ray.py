"""Example Ray inference script."""

import os
import sys
import time

import ray
from dagster_pipes import PipesContext, open_dagster_pipes


@ray.remote
def run_inference_batch(batch_id: int):
    """Simulate distributed inference."""
    return f"Processed batch {batch_id}"


def main():
    context = PipesContext.get()
    context.log.info("Starting Ray inference...")
    model_path = os.environ.get("MODEL_PATH")
    context.log.info(f"Loading model from: {model_path}")

    if not ray.is_initialized():  # type: ignore
        context.log.info("Connecting to Ray cluster...")

        # Initialize with normal logging (you'll see the logs)
        ray.init(  # type: ignore
            address=os.environ.get("RAY_ADDRESS", "auto"),  # type: ignore
            logging_level="INFO",  # Keep normal logging  # type: ignore
        )

        # CRITICAL: Wait for Ray's async connection logs to finish
        time.sleep(1.0)  # Give Ray time to print connection messages
        sys.stdout.flush()
        sys.stderr.flush()

        context.log.info(f"Connected to Ray at {ray.get_runtime_context().gcs_address}")  # type: ignore
    # Distributed inference
    num_batches = 20
    futures = [run_inference_batch.remote(i) for i in range(num_batches)]
    results = ray.get(futures)
    context.log.info(f"Processed {len(results)} batches")
    context.report_asset_materialization(
        metadata={
            "num_batches": num_batches,
            "framework": "ray",
        }
    )
    context.log.info("Inference complete!")


if __name__ == "__main__":
    with open_dagster_pipes() as context:
        main()
