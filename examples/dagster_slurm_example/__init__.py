import dagster as dg


@dg.definitions
def defs():
    return dg.Definitions(
        assets=[],
        resources={},
    )
