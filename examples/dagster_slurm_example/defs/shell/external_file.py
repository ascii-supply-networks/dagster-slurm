from dagster_pipes import open_dagster_pipes, PipesEnvVarParamsLoader
from dagster_pipes import PipesDefaultContextLoader, PipesDefaultMessageWriter

with open_dagster_pipes(
    params_loader=PipesEnvVarParamsLoader(),
    context_loader=PipesDefaultContextLoader(),      # reads path from env
    message_writer=PipesDefaultMessageWriter(),      # writes to path from env
) as pipes:
    pipes.log.info("running in Slurm")
    pipes.report_asset_materialization(metadata={"rows": {"raw_value": 123, "type": "int"}})