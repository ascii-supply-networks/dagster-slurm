"""Integration tests for dagster-slurm across different deployment modes.

These tests assume the SLURM Docker cluster is already running.
Run with: pytest -v -m integration
"""

import os
import subprocess
from pathlib import Path
from typing import Dict, Any

import pytest
from .test_utils import run_dg_command, assert_materialization_success


@pytest.fixture(scope="module")
def docker_slurm_env(docker_ssh_key: Path) -> Dict[str, str]:
    return {
        "SLURM_EDGE_NODE_HOST": "127.0.0.1",
        "SLURM_EDGE_NODE_PORT": "2223",
        "SLURM_EDGE_NODE_USER": "submitter",
        "SLURM_EDGE_NODE_KEY_PATH": str(docker_ssh_key),
        "SLURM_EDGE_NODE_PASSWORD": "",
        "SLURM_EDGE_NODE_JUMP_HOST": "",
        "SLURM_EDGE_NODE_JUMP_USER": "",
        "SLURM_EDGE_NODE_JUMP_PASSWORD": "",
    }


pytestmark = pytest.mark.needs_slurm_docker


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
        assert "LocalPipesClient" in result.stderr or "local" in result.stderr.lower()

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
            "Single-node mode" in result.stderr
            or "local Ray cluster" in result.stderr.lower()
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

    def test_list_defs_staging(
        self, example_project_dir: Path, docker_slurm_env: Dict[str, str]
    ):
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
            env={
                **os.environ,
                **docker_slurm_env,
                "DAGSTER_DEPLOYMENT": "STAGING_DOCKER",
            },
            capture_output=True,
            text=True,
            timeout=30,
        )

        assert result.returncode == 0

    def test_bash_assets_staging(
        self, example_project_dir: Path, docker_slurm_env: Dict[str, str]
    ):
        """Test shell/bash assets on SLURM with interactive build."""
        result = run_dg_command(
            example_project_dir,
            deployment="STAGING_DOCKER",
            assets="process_data,aggregate_results",
            env_overrides=docker_slurm_env,
            timeout=600,  # Longer timeout for environment packing
        )

        assert_materialization_success(result, "process_data,aggregate_results")

        # Check for SLURM execution indicators
        assert "Submitted job" in result.stderr
        assert "pixi pack" in result.stderr or "Packing environment" in result.stderr

    def test_ray_staging(
        self, example_project_dir: Path, docker_slurm_env: Dict[str, str]
    ):
        """Test Ray multi-node cluster on SLURM with interactive build."""
        result = run_dg_command(
            example_project_dir,
            deployment="STAGING_DOCKER",
            assets="distributed_training,distributed_inference",
            env_overrides=docker_slurm_env,
            timeout=600,
        )

        assert_materialization_success(
            result, "distributed_training,distributed_inference"
        )

        # Check for multi-node Ray indicators
        assert "nodes" in result.stderr.lower()


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
        self,
        example_project_dir: Path,
        deployment_metadata: Dict[str, Any],
        docker_slurm_env: Dict[str, str],
    ):
        """Test shell/bash assets with pre-deployed environment."""
        result = run_dg_command(
            example_project_dir,
            deployment="PRODUCTION_DOCKER",
            assets="process_data,aggregate_results",
            env_overrides={
                **docker_slurm_env,
                "CI_DEPLOYED_ENVIRONMENT_PATH": deployment_metadata["deployment_path"],
            },
            timeout=300,
        )

        assert_materialization_success(result, "process_data,aggregate_results")

        # Check that we're using pre-deployed environment (no packing)
        assert "pixi pack" not in result.stderr
        assert "Submitted job" in result.stderr

    def test_ray_assets_production(
        self,
        example_project_dir: Path,
        deployment_metadata: Dict[str, Any],
        docker_slurm_env: Dict[str, str],
    ):
        """Test Ray assets with pre-deployed environment."""
        result = run_dg_command(
            example_project_dir,
            deployment="PRODUCTION_DOCKER",
            assets="distributed_training,distributed_inference",
            env_overrides={
                **docker_slurm_env,
                "CI_DEPLOYED_ENVIRONMENT_PATH": deployment_metadata["deployment_path"],
            },
            timeout=300,
        )

        assert_materialization_success(
            result, "distributed_training,distributed_inference"
        )

        # Verify we're using pre-deployed environment
        assert "pixi pack" not in result.stderr
        assert deployment_metadata["deployment_path"] in result.stderr


# ============================================================================
# Session Mode Tests
# ============================================================================


# class TestSessionModes:
#     """Tests for session-based execution modes."""

#     @pytest.fixture(autouse=True)
#     def require_slurm(self, slurm_cluster_ready):
#         """Ensure SLURM cluster is ready for all session tests."""
#         pass

#     def test_staging_session_mode(self, example_project_dir: Path):
#         """Test STAGING_DOCKER_SESSION mode."""
#         result = run_dg_command(
#             example_project_dir,
#             deployment="STAGING_DOCKER_SESSION",
#             assets="process_data,aggregate_results",
#             timeout=600,
#         )

#         assert_materialization_success(result, "process_data,aggregate_results")
#         assert "Session resource initialized" in result.stderr

#     def test_staging_session_cluster_reuse(self, example_project_dir: Path):
#         """Test STAGING_DOCKER_SESSION_CLUSTER_REUSE mode."""
#         result = run_dg_command(
#             example_project_dir,
#             deployment="STAGING_DOCKER_SESSION_CLUSTER_REUSE",
#             assets="distributed_training,distributed_inference",
#             timeout=600,
#         )

#         assert_materialization_success(
#             result, "distributed_training,distributed_inference"
#         )
#         assert "Cluster reuse enabled" in result.stderr

#     def test_staging_hetjob_mode(self, example_project_dir: Path):
#         """Test STAGING_DOCKER_HETJOB mode."""
#         result = run_dg_command(
#             example_project_dir,
#             deployment="STAGING_DOCKER_HETJOB",
#             assets="heterogeneous_pipeline",
#             timeout=600,
#         )

#         assert_materialization_success(result, "heterogeneous_pipeline")
#         assert "heterogeneous job" in result.stderr.lower()


# # ============================================================================
# # Edge Cases and Error Handling
# # ============================================================================


# class TestEdgeCases:
#     """Tests for edge cases and error scenarios."""

#     def test_invalid_deployment_mode(self, example_project_dir: Path):
#         """Test handling of invalid deployment mode."""
#         with pytest.raises(subprocess.CalledProcessError):
#             subprocess.run(
#                 [
#                     "pixi",
#                     "run",
#                     "-e",
#                     "dev",
#                     "dg",
#                     "--target-path",
#                     "examples",
#                     "launch",
#                     "--assets",
#                     "process_data",
#                 ],
#                 cwd=example_project_dir,
#                 env={**os.environ, "DAGSTER_DEPLOYMENT": "INVALID_MODE"},
#                 check=True,
#                 capture_output=True,
#                 timeout=30,
#             )

#     def test_nonexistent_asset(self, example_project_dir: Path):
#         """Test handling of non-existent asset."""
#         with pytest.raises(subprocess.CalledProcessError):
#             subprocess.run(
#                 [
#                     "pixi",
#                     "run",
#                     "-e",
#                     "dev",
#                     "dg",
#                     "--target-path",
#                     "examples",
#                     "launch",
#                     "--assets",
#                     "nonexistent_asset",
#                 ],
#                 cwd=example_project_dir,
#                 env={**os.environ, "DAGSTER_DEPLOYMENT": "development"},
#                 check=True,
#                 capture_output=True,
#                 timeout=30,
#             )
