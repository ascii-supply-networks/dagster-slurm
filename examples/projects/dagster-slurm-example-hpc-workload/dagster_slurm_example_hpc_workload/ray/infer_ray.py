"""Example Ray inference script."""

import os

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
