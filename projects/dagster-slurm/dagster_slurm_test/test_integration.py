"""Integration tests."""

import dagster as dg
from dagster import AssetExecutionContext, asset, materialize
from dagster_slurm import ComputeResource


def test_local_asset_execution(temp_dir, local_compute_resource):
    """Test complete local asset execution."""
    # Create test payload
    payload = temp_dir / "test_payload.py"
    payload.write_text("""
from dagster_pipes import open_dagster_pipes

with open_dagster_pipes() as context:
    context.log.info("Test execution")
    context.report_asset_materialization(metadata={"test": "value"})
""")

    @asset
    def test_asset(context: AssetExecutionContext, compute: ComputeResource):
        yield from compute.run(
            context=context,
            payload_path=str(payload),
        ).get_results()

    result = materialize(
        [test_asset],
        resources={"compute": local_compute_resource},
    )

    assert result.success


def test_bash_launcher_integration(temp_dir, local_compute_resource):
    """Test bash launcher integration."""
    payload = temp_dir / "bash_payload.py"
    payload.write_text("""
import os
from dagster_pipes import open_dagster_pipes

with open_dagster_pipes() as context:
    test_var = os.environ.get("TEST_VAR", "not_set")
    context.log.info(f"TEST_VAR={test_var}")
    context.report_asset_materialization(metadata={"test_var": test_var})
""")

    @asset
    def bash_asset(context: AssetExecutionContext, compute: ComputeResource):
        yield from compute.run(
            context=context,
            payload_path=str(payload),
            extra_env={"TEST_VAR": "test_value"},
        ).get_results()

    result = materialize(
        [bash_asset],
        resources={"compute": local_compute_resource},
    )

    assert result.success


def test_local_asset_check_execution(temp_dir, local_compute_resource):
    """Test local asset checks reported via Dagster Pipes."""
    payload = temp_dir / "asset_check_payload.py"
    payload.write_text("""
from dagster_pipes import open_dagster_pipes

with open_dagster_pipes() as context:
    context.report_asset_check(
        asset_key="orders",
        check_name="orders_are_non_empty",
        passed=True,
    )
""")

    @dg.asset
    def orders():
        return 1

    @dg.asset_check(asset=orders)
    def orders_are_non_empty(
        context: dg.AssetCheckExecutionContext,
        compute: ComputeResource,
    ):
        return compute.run(
            context=context,
            payload_path=str(payload),
        ).get_asset_check_result()

    job = dg.define_asset_job("asset_check_job")
    defs = dg.Definitions(
        assets=[orders],
        asset_checks=[orders_are_non_empty],
        jobs=[job],
        resources={"compute": local_compute_resource},
    )

    result = defs.resolve_job_def("asset_check_job").execute_in_process()

    assert result.success
    evaluations = result.get_asset_check_evaluations()
    assert len(evaluations) == 1
    assert evaluations[0].check_name == "orders_are_non_empty"
    assert evaluations[0].passed is True
