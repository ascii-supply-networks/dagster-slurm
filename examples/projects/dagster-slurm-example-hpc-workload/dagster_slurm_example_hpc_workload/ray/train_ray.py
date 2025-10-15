"""Example Ray training script."""

import ray
from dagster_pipes import PipesContext, open_dagster_pipes


@ray.remote
def train_model_chunk(chunk_id: int):
    """Simulate distributed training."""
    return f"Trained chunk {chunk_id}"


def main():
    context = PipesContext.get()
    context.log.info("Starting Ray training...")
    # Ray is already initialized by launcher
    context.log.info(f"Ray address: {ray.get_runtime_context().gcs_address}")  # type: ignore
    # Distributed training
    num_chunks = 10
    futures = [train_model_chunk.remote(i) for i in range(num_chunks)]
    results = ray.get(futures)
    context.log.info(f"Trained {len(results)} chunks")
    context.report_asset_materialization(
        metadata={
            "num_chunks": num_chunks,
            "framework": "ray",
            "model_path": "/path/to/model",
        }
    )
    context.log.info("Training complete!")


if __name__ == "__main__":
    with open_dagster_pipes() as context:
        main()
