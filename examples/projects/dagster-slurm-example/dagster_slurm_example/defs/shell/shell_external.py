import os

import pandas as pd
from dagster_pipes import PipesContext, open_dagster_pipes
from dagster_slurm_example_shared.shared import foo

from dagster_slurm_example.defs.shared import example_defs_prefix


def main():
    context = PipesContext.get()
    context.log.info("Hello from pipes")
    print(context.extras)
    print(context.get_extra("foo"))
    print(os.environ["MY_ENV_VAR_IN_SUBPROCESS"])
    print("*" * 10)
    print(foo)
    print("*" * 10)

    orders_df = pd.DataFrame({"order_id": [1, 2], "item_id": [432, 878]})
    total_orders = len(orders_df)
    total_items = int(orders_df["item_id"].nunique())

    context.log.info(f"Total orders: {total_orders}, Total items: {total_items}")

    context.report_asset_materialization(
        asset_key=f"{example_defs_prefix}/orders",
        metadata={"total_orders": total_orders},
    )
    context.report_asset_materialization(
        asset_key="users", metadata={"total_users": total_items}
    )


if __name__ == "__main__":
    with open_dagster_pipes() as context:
        main()
