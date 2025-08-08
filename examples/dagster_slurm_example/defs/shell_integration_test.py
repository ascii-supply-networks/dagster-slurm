import pandas as pd
from dagster_pipes import PipesContext, open_dagster_pipes

from dagster_slurm_example.defs.shared import example_defs_prefix


def main():
    context = PipesContext.get()
    context.log.info("Hello from pipes during testing")
    context.log.info(f"Running subprocess with extras: {context.extras}")

    orders_df = pd.DataFrame({"order_id": [1, 2], "item_id": [432, 878]})
    context.report_asset_check(
        asset_key=f"{example_defs_prefix}/orders",
        passed=orders_df[["item_id"]].notnull().all().item(),
        check_name="no_empty_order_check",
        severity="WARN",
    )


if __name__ == "__main__":
    with open_dagster_pipes() as context:
        main()
