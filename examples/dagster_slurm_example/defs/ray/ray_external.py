from dagster_pipes import PipesContext, open_dagster_pipes


def main():
    context = PipesContext.get()
    context.log.info("Hello from pipes")
    print(context.extras)
    assert context.get_extra("param") == "value"
    value = context.get_extra("param")
    context.report_asset_materialization(
        metadata={
            "some_metric": {"raw_value": 57, "type": "int"},
            "other_metric": value,
        },
        data_version="alpha",
    )


if __name__ == "__main__":
    with open_dagster_pipes() as context:
        main()
