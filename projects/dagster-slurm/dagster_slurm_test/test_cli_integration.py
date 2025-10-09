"""Integration tests for dagster-slurm across different deployment modes.

These tests assume the SLURM Docker cluster is already running.
Run with: pytest -v -m integration
"""

import json
import os
import subprocess
import time
from pathlib import Path
from typing import Dict, Any

import pytest


pytestmark = pytest.mark.needs_slurm_docker


def run_dg_command(
    example_project_dir: Path,
    deployment: str,
    assets: str,
    env_overrides: Dict[str, str] = {},
    timeout: int = 300,
) -> subprocess.CompletedProcess:
    """Run a `dg launch` command with specified deployment mode.

    Args:
        example_project_dir: Path to examples directory
        deployment: DAGSTER_DEPLOYMENT value (development, STAGING_DOCKER, etc.)
        assets: Comma-separated asset names
        env_overrides: Additional environment variables
        timeout: Command timeout in seconds

    Returns:
        CompletedProcess with stdout/stderr
    """
    env = os.environ.copy()
    env["DAGSTER_DEPLOYMENT"] = deployment

    if env_overrides:
        env.update(env_overrides)

    cmd = [
        "pixi",
        "run",
        "-e",
        "dev",
        "dg",
        "--target-path",
        "examples",
        "launch",
        "--assets",
        assets,
    ]

    print(f"\n🚀 Running: {' '.join(cmd)}")
    print(f"   Deployment: {deployment}")
    print(f"   Assets: {assets}")

    result = subprocess.run(
        cmd,
        cwd=example_project_dir,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )

    print("\n--- STDOUT ---")
    print(result.stdout)
    if result.stderr:
        print("\n--- STDERR ---")
        print(result.stderr)

    return result


def assert_materialization_success(result: subprocess.CompletedProcess, assets: str):
    """Assert that materialization completed successfully."""
    assert result.returncode == 0, (
        f"Command failed with exit code {result.returncode}\n"
        f"STDERR: {result.stderr}\n"
        f"STDOUT: {result.stdout}"
    )

    # Check for success indicators
    for asset in assets.split(","):
        assert asset in result.stdout, f"Asset {asset} not found in output"

    # Check for common failure patterns
    assert "STEP_FAILURE" not in result.stdout, "Step failure detected"
    assert "CheckError" not in result.stdout, "Check error detected"
    assert "ConnectionError" not in result.stderr, "Connection error detected"


# ============================================================================
# Development Mode Tests (Local)
# ============================================================================


class TestDevelopmentMode:
    """Tests for local development mode (no SLURM)."""

    def test_list_defs_development(self, example_project_dir: Path):
        """Test listing definitions in development mode."""
        result = subprocess.run(
            [
                "pixi",
                "run",
                "-e",
                "dev",
                "dg",
                "--target-path",
                "examples",
                "list",
                "defs",
            ],
            cwd=example_project_dir,
            env={**os.environ, "DAGSTER_DEPLOYMENT": "development"},
            capture_output=True,
            text=True,
            timeout=30,
        )

        assert result.returncode == 0
        assert "process_data" in result.stdout
        assert "aggregate_results" in result.stdout
        assert "distributed_training" in result.stdout
        assert "distributed_inference" in result.stdout

    def test_bash_assets_development(self, example_project_dir: Path):
        """Test shell/bash assets in local mode."""
        result = run_dg_command(
            example_project_dir,
            deployment="development",
            assets="process_data,aggregate_results",
            timeout=120,
        )

        assert_materialization_success(result, "process_data,aggregate_results")

        # Check for local execution indicators
        assert "LocalPipesClient" in result.stdout or "local" in result.stdout.lower()

    def test_ray_assets_development(self, example_project_dir: Path):
        """Test Ray assets in local mode (single-node)."""
        result = run_dg_command(
            example_project_dir,
            deployment="development",
            assets="distributed_training,distributed_inference",
            timeout=180,
        )

        assert_materialization_success(
            result, "distributed_training,distributed_inference"
        )

        # Check for Ray local mode indicators
        assert (
            "Single-node mode" in result.stdout or "local Ray cluster" in result.stdout
        )


# ============================================================================
# Staging Docker Mode Tests (Interactive Build)
# ============================================================================


class TestStagingDockerMode:
    """Tests for STAGING_DOCKER mode (on-demand environment packaging)."""

    @pytest.fixture(autouse=True)
    def require_slurm(self, slurm_cluster_ready):
        """Ensure SLURM cluster is ready for all staging tests."""
        pass

    def test_list_defs_staging(self, example_project_dir: Path):
        """Test listing definitions in staging mode."""
        result = subprocess.run(
            [
                "pixi",
                "run",
                "-e",
                "dev",
                "dg",
                "--target-path",
                "examples",
                "list",
                "defs",
            ],
            cwd=example_project_dir,
            env={**os.environ, "DAGSTER_DEPLOYMENT": "STAGING_DOCKER"},
            capture_output=True,
            text=True,
            timeout=30,
        )

        assert result.returncode == 0

    def test_bash_assets_staging(self, example_project_dir: Path):
        """Test shell/bash assets on SLURM with interactive build."""
        result = run_dg_command(
            example_project_dir,
            deployment="STAGING_DOCKER",
            assets="process_data,aggregate_results",
            timeout=600,  # Longer timeout for environment packing
        )

        assert_materialization_success(result, "process_data,aggregate_results")

        # Check for SLURM execution indicators
        assert "Submitted job" in result.stdout
        assert "pixi pack" in result.stdout or "Packing environment" in result.stdout

    def test_ray_single_node_staging(self, example_project_dir: Path):
        """Test Ray single-node execution on SLURM with interactive build."""
        result = run_dg_command(
            example_project_dir,
            deployment="STAGING_DOCKER",
            assets="distributed_training",
            timeout=600,
        )

        assert_materialization_success(result, "distributed_training")

        # Check for Ray cluster indicators
        assert "Ray" in result.stdout
        assert "Submitted job" in result.stdout

    def test_ray_multi_node_staging(self, example_project_dir: Path):
        """Test Ray multi-node cluster on SLURM with interactive build."""
        result = run_dg_command(
            example_project_dir,
            deployment="STAGING_DOCKER",
            assets="distributed_training,distributed_inference",
            timeout=600,
        )

        assert_materialization_success(
            result, "distributed_training,distributed_inference"
        )

        # Check for multi-node Ray indicators
        assert "nodes" in result.stdout.lower()


# ============================================================================
# Production Docker Mode Tests (Pre-packaged Environment)
# ============================================================================


class TestProductionDockerMode:
    """Tests for PRODUCTION_DOCKER mode (pre-deployed environment)."""

    @pytest.fixture(autouse=True)
    def require_slurm(self, slurm_cluster_ready):
        """Ensure SLURM cluster is ready for all production tests."""
        pass

    def test_bash_assets_production(
        self, example_project_dir: Path, deployment_metadata: Dict[str, Any]
    ):
        """Test shell/bash assets with pre-deployed environment."""
        result = run_dg_command(
            example_project_dir,
            deployment="PRODUCTION_DOCKER",
            assets="process_data,aggregate_results",
            env_overrides={
                "CI_DEPLOYED_ENVIRONMENT_PATH": deployment_metadata["deployment_path"]
            },
            timeout=300,
        )

        assert_materialization_success(result, "process_data,aggregate_results")

        # Check that we're using pre-deployed environment (no packing)
        assert "pixi pack" not in result.stdout
        assert "Submitted job" in result.stdout

    def test_ray_assets_production(
        self, example_project_dir: Path, deployment_metadata: Dict[str, Any]
    ):
        """Test Ray assets with pre-deployed environment."""
        result = run_dg_command(
            example_project_dir,
            deployment="PRODUCTION_DOCKER",
            assets="distributed_training,distributed_inference",
            env_overrides={
                "CI_DEPLOYED_ENVIRONMENT_PATH": deployment_metadata["deployment_path"]
            },
            timeout=300,
        )

        assert_materialization_success(
            result, "distributed_training,distributed_inference"
        )

        # Verify we're using pre-deployed environment
        assert "pixi pack" not in result.stdout
        assert deployment_metadata["deployment_path"] in result.stdout


# ============================================================================
# Session Mode Tests
# ============================================================================

# class TestSessionModes:
#     """Tests for session-based execution modes."""

#     @pytest.fixture(autouse=True)
#     def require_slurm(self, slurm_cluster_ready):
#         """Ensure SLURM cluster is ready for all session tests."""
#         pass

#     @pytest.mark.skip(reason="Session mode requires additional setup")
#     def test_staging_session_mode(self, example_project_dir: Path):
#         """Test STAGING_DOCKER_SESSION mode."""
#         result = run_dg_command(
#             example_project_dir,
#             deployment="STAGING_DOCKER_SESSION",
#             assets="process_data,aggregate_results",
#             timeout=600,
#         )

#         assert_materialization_success(result, "process_data,aggregate_results")

#     @pytest.mark.skip(reason="Cluster reuse requires additional setup")
#     def test_staging_session_cluster_reuse(self, example_project_dir: Path):
#         """Test STAGING_DOCKER_SESSION_CLUSTER_REUSE mode."""
#         result = run_dg_command(
#             example_project_dir,
#             deployment="STAGING_DOCKER_SESSION_CLUSTER_REUSE",
#             assets="distributed_training,distributed_inference",
#             timeout=600,
#         )

#         assert_materialization_success(result, "distributed_training,distributed_inference")
#         assert "Reusing existing cluster" in result.stdout

#     @pytest.mark.skip(reason="HetJob mode requires additional setup")
#     def test_staging_hetjob_mode(self, example_project_dir: Path):
#         """Test STAGING_DOCKER_HETJOB mode."""
#         result = run_dg_command(
#             example_project_dir,
#             deployment="STAGING_DOCKER_HETJOB",
#             assets="process_data,aggregate_results,distributed_training",
#             timeout=600,
#         )

#         assert_materialization_success(
#             result,
#             "process_data,aggregate_results,distributed_training"
#         )
#         assert "heterogeneous job" in result.stdout.lower()


# ============================================================================
# Edge Cases and Error Handling
# ============================================================================


class TestEdgeCases:
    """Tests for edge cases and error scenarios."""

    def test_invalid_deployment_mode(self, example_project_dir: Path):
        """Test handling of invalid deployment mode."""
        with pytest.raises(subprocess.CalledProcessError):
            subprocess.run(
                [
                    "pixi",
                    "run",
                    "-e",
                    "dev",
                    "dg",
                    "--target-path",
                    "examples",
                    "launch",
                    "--assets",
                    "process_data",
                ],
                cwd=example_project_dir,
                env={**os.environ, "DAGSTER_DEPLOYMENT": "INVALID_MODE"},
                check=True,
                capture_output=True,
                timeout=30,
            )

    def test_nonexistent_asset(self, example_project_dir: Path):
        """Test handling of non-existent asset."""
        with pytest.raises(subprocess.CalledProcessError):
            subprocess.run(
                [
                    "pixi",
                    "run",
                    "-e",
                    "dev",
                    "dg",
                    "--target-path",
                    "examples",
                    "launch",
                    "--assets",
                    "nonexistent_asset",
                ],
                cwd=example_project_dir,
                env={**os.environ, "DAGSTER_DEPLOYMENT": "development"},
                check=True,
                capture_output=True,
                timeout=30,
            )


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

        print(f"\n📊 Performance Comparison:")
        print(f"   Staging: {staging_time:.2f}s")
        print(f"   Production: {prod_time:.2f}s")
        print(f"   Speedup: {staging_time / prod_time:.2f}x")

        # Production should be significantly faster (expect at least 2x)
        assert prod_time < staging_time, (
            f"Production mode should be faster than staging "
            f"(staging: {staging_time:.2f}s, prod: {prod_time:.2f}s)"
        )
