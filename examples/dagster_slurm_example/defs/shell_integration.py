import shutil

import dagster as dg

from dagster_slurm_example.defs.shared import example_defs_prefix


@dg.multi_asset(
    specs=[dg.AssetSpec(key=[example_defs_prefix, "orders"]), dg.AssetSpec("users")]
)
def subprocess_asset(
    context: dg.AssetExecutionContext,
    pipes_subprocess_client: dg.PipesSubprocessClient,
):
    cmd = [shutil.which("python"), dg.file_relative_path(__file__, "shell_external.py")]
    return pipes_subprocess_client.run(
        command=cmd,
        context=context,
        extras={"foo": "bar"},
        env={
            "MY_ENV_VAR_IN_SUBPROCESS": "my_value",
        },
    ).get_results()


@dg.asset_check(
    asset=dg.AssetKey([example_defs_prefix, "orders"]),
    blocking=True,
)
def no_empty_order_check(
    context: dg.AssetCheckExecutionContext,
    pipes_subprocess_client: dg.PipesSubprocessClient,
) -> dg.AssetCheckResult:
    cmd = [
        shutil.which("python"),
        dg.file_relative_path(__file__, "shell_integration_test.py"),
    ]

    results = pipes_subprocess_client.run(
        command=cmd, context=context.op_execution_context
    ).get_asset_check_result()
    return results
