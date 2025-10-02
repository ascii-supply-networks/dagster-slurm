"""Example payload script - aggregation."""

import os

from dagster_pipes import PipesContext, open_dagster_pipes


def main():
    context = PipesContext.get()
    context.log.info("Starting aggregation...")
    processed_path = os.environ.get("PROCESSED_DATA")
    context.log.info(f"Aggregating from: {processed_path}")
    # Your aggregation logic
    context.report_asset_materialization(
        metadata={
            "total_records": 1000,
            "aggregation_time": "5s",
        }
    )
    context.log.info("Aggregation complete!")


if __name__ == "__main__":
    with open_dagster_pipes() as context:
        main()
