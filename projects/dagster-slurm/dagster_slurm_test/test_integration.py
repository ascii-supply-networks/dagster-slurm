"""Integration tests."""

from dagster import AssetExecutionContext, asset, materialize
from dagster_slurm import ComputeResource
from dagster_slurm.config.environment import ExecutionMode


def test_local_asset_execution(temp_dir):
    """Test complete local asset execution."""
    # Create test payload
    payload = temp_dir / "test_payload.py"
    payload.write_text("""
from dagster_pipes import open_dagster_pipes

with open_dagster_pipes() as context:
    context.log.info("Test execution")
    context.report_asset_materialization(metadata={"test": "value"})
""")

    # Create asset
    @asset
    def test_asset(context: AssetExecutionContext, compute: ComputeResource):
        return list(
            compute.run(
                context=context,
                payload_path=str(payload),
            )
        )

    # Materialize
    result = materialize(
        [test_asset],
        resources={"compute": ComputeResource(mode=ExecutionMode.LOCAL)},
    )

    assert result.success


def test_bash_launcher_integration(temp_dir):
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
        return list(
            compute.run(
                context=context,
                payload_path=str(payload),
                extra_env={"TEST_VAR": "test_value"},
            )
        )

    result = materialize(
        [bash_asset],
        resources={"compute": ComputeResource(mode=ExecutionMode.LOCAL)},
    )

    assert result.success
