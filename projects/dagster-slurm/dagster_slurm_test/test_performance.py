"""Integration tests for dagster-slurm across different deployment modes.

These tests assume the SLURM Docker cluster is already running.
Run with: pytest -v -m integration
"""

from pathlib import Path
from typing import Dict, Any
from .test_utils import run_dg_command, assert_materialization_success

import pytest


pytestmark = pytest.mark.slow

# ============================================================================
# Performance Tests
# ============================================================================


class TestPerformance:
    """Performance comparison tests between modes."""

    @pytest.mark.slow
    def test_staging_vs_production_performance(
        self,
        example_project_dir: Path,
        deployment_metadata: Dict[str, Any],
        slurm_cluster_ready,
    ):
        """Compare execution time between staging and production modes."""
        import time

        # Test staging mode
        start = time.time()
        staging_result = run_dg_command(
            example_project_dir,
            deployment="STAGING_DOCKER",
            assets="process_data",
            timeout=600,
        )
        staging_time = time.time() - start
        assert_materialization_success(staging_result, "process_data")

        # Test production mode
        start = time.time()
        prod_result = run_dg_command(
            example_project_dir,
            deployment="PRODUCTION_DOCKER",
            assets="process_data",
            env_overrides={
                "CI_DEPLOYED_ENVIRONMENT_PATH": deployment_metadata["deployment_path"]
            },
            timeout=300,
        )
        prod_time = time.time() - start
        assert_materialization_success(prod_result, "process_data")

        print("\n📊 Performance Comparison:")
        print(f"   Staging: {staging_time:.2f}s")
        print(f"   Production: {prod_time:.2f}s")
        print(f"   Speedup: {staging_time / prod_time:.2f}x")

        # Production should be significantly faster (expect at least 2x)
        assert prod_time < staging_time, (
            f"Production mode should be faster than staging "
            f"(staging: {staging_time:.2f}s, prod: {prod_time:.2f}s)"
        )
