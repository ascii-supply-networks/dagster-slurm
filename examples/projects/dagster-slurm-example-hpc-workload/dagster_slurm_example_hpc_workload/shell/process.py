"""Example payload script - data processing."""

import os

from dagster_pipes import PipesContext, open_dagster_pipes


def main():
    context = PipesContext.get()
    context.log.info("Starting data processing...")
    input_path = os.environ.get("INPUT_DATA")
    output_path = os.environ.get("OUTPUT_DATA")
    context.log.info(f"Input: {input_path}")
    context.log.info(f"Output: {output_path}")
    # Your processing logic here
    result = {"rows_processed": 1000}
    context.report_asset_materialization(
        metadata={
            "rows": result["rows_processed"],
            "processing_time": "10s",
        }
    )
    context.log.info("Processing complete!")


if __name__ == "__main__":
    with open_dagster_pipes() as context:
        main()
