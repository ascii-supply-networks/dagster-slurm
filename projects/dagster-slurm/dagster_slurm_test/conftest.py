"""Pytest configuration and fixtures."""

import tempfile
from pathlib import Path
from typing import Type


import pytest
from dagster_slurm import (
    ComputeResource,
    SlurmQueueConfig,
    SlurmResource,
    SSHConnectionResource,
    BashLauncher,
    ComputeLauncher,
    RayLauncher,
    SparkLauncher,
)
from dagster_slurm.config.environment import ExecutionMode
import subprocess
import time
import json
from typing import Any, Dict
import platform
from loguru import logger


@pytest.fixture
def temp_dir():
    """Temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def mock_ssh_resource():
    """Mock SSH connection resource."""
    return SSHConnectionResource(
        host="localhost",
        port=2223,
        user="testuser",
        key_path="/tmp/test_key",
    )


@pytest.fixture
def mock_slurm_resource(mock_ssh_resource):
    """Mock Slurm resource."""
    return SlurmResource(
        ssh=mock_ssh_resource,
        queue=SlurmQueueConfig(
            partition="test",
            time_limit="00:10:00",
            cpus=2,
            mem="1G",
        ),
        remote_base="/tmp/dagster_test",
    )


@pytest.fixture(scope="module")
def local_compute_resource() -> ComputeResource:
    """
    Provides a correctly configured ComputeResource for local execution tests.
    This fixture encapsulates the required explicit constructor arguments.
    """
    return ComputeResource(
        mode=ExecutionMode.LOCAL,
        default_launcher=BashLauncher(),
    )


@pytest.fixture(scope="module")
def slurm_compute_resource() -> ComputeResource:
    """
    Provides a correctly configured ComputeResource for slurm docker execution tests.
    This fixture encapsulates the required explicit constructor arguments.
    """
    ssh = SSHConnectionResource(
        host="localhost",
        port=2223,
        user="submitter",
        password="submitter",
    )

    slurm = SlurmResource(
        ssh=ssh,
        queue=SlurmQueueConfig(),
        remote_base="/home/testuser/dagster",
    )
    return ComputeResource(
        mode=ExecutionMode.SLURM,
        default_launcher=BashLauncher(),
        slurm=slurm,
    )


# This fixture provides the base SlurmResource. It's marked as "session" scope
# so it only runs once for the entire test session, which is efficient.
@pytest.fixture(scope="session")
def slurm_resource_for_testing() -> SlurmResource:
    """
    Provides a base SlurmResource configured for the Docker test cluster.
    Skips tests if connection details are not available in environment variables.
    """
    ssh = SSHConnectionResource(
        host="localhost",
        port=2223,
        user="submitter",
        password="submitter",
    )

    return SlurmResource(
        ssh=ssh,
        queue=SlurmQueueConfig(partition="batch"),
        remote_base="/home/submitter/dagster_ci_runs",
    )


def _compute_resource_factory(
    slurm_resource: SlurmResource, launcher_class: Type[ComputeLauncher]
) -> ComputeResource:
    """A factory to create a ComputeResource with a specific launcher."""
    return ComputeResource(
        mode=ExecutionMode.SLURM,
        slurm=slurm_resource,
        default_launcher=launcher_class(),
        # For CI, it's useful to see logs even on failure; enable if desired
        cleanup_on_failure=True,
    )


@pytest.fixture(scope="module")
def slurm_bash_compute_resource(
    slurm_resource_for_testing: SlurmResource,
) -> ComputeResource:
    """Provides a ComputeResource for SLURM mode with a BashLauncher."""
    return _compute_resource_factory(slurm_resource_for_testing, BashLauncher)


@pytest.fixture(scope="module")
def slurm_ray_compute_resource(
    slurm_resource_for_testing: SlurmResource,
) -> ComputeResource:
    """Provides a ComputeResource for SLURM mode with a RayLauncher."""
    return _compute_resource_factory(slurm_resource_for_testing, RayLauncher)


@pytest.fixture(scope="module")
def slurm_spark_compute_resource(
    slurm_resource_for_testing: SlurmResource,
) -> ComputeResource:
    """Provides a ComputeResource for SLURM mode with a SparkLauncher."""
    return _compute_resource_factory(slurm_resource_for_testing, SparkLauncher)


@pytest.fixture(scope="session")
def example_project_dir() -> Path:
    """Returns the path to the example project directory."""
    path = Path(__file__).parent.parent.parent.parent / "examples"
    if not path.is_dir():
        pytest.fail(f"Example project directory not found at: {path}")
    return path


@pytest.fixture(scope="session")
def slurm_cluster_ready():
    """Verify SLURM cluster is ready before running tests."""
    print("\n🔍 Checking SLURM cluster status...")

    for attempt in range(30):
        try:
            result = subprocess.run(
                ["docker", "exec", "slurmctld", "sinfo"],
                capture_output=True,
                text=True,
                timeout=10,
            )

            if result.returncode == 0 and "idle" in result.stdout:
                print("✅ SLURM cluster is ready!")
                print(result.stdout)
                return True

        except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
            print(f"⏳ Waiting for SLURM... ({attempt + 1}/12)")
            time.sleep(5)

    pytest.fail("❌ SLURM cluster is not ready")


@pytest.fixture(scope="session")
def deployment_metadata_file(example_project_dir: Path) -> Path:
    """Get path to deployment metadata file."""
    return example_project_dir / "deployment_metadata.json"


@pytest.fixture
def deployment_metadata(example_project_dir: Path) -> Dict[str, Any]:
    """Read deployment metadata for PRODUCTION_DOCKER mode."""
    metadata_file = example_project_dir / "deployment_metadata.json"

    if not metadata_file.exists():
        system = platform.system().lower()
        machine = platform.machine().lower()

        logger.info(f"Auto-detected platform: {system}/{machine}")

        if system == "darwin" and "arm" in machine:
            prep_cmd = ["pixi", "run", "deploy-prod-docker-aarch"]
        elif system == "linux" and ("aarch64" in machine or "arm" in machine):
            prep_cmd = ["pixi", "run", "deploy-prod-docker"]
        else:
            raise ValueError(f"Unsupported platform: {system}/{machine}")
        subprocess.run(
            prep_cmd,
            cwd=example_project_dir,
            check=True,
            capture_output=True,
        )

    with open(metadata_file) as f:
        return json.load(f)


# @pytest.fixture(scope="session")
# def create_production_deployment(
#     example_project_dir: Path,
#     deployment_metadata_file: Path
# ) -> Generator[dict, None, None]:
#     """Create a production deployment if it doesn't exist.

#     Yields:
#         Deployment metadata dictionary
#     """
#     # Check if deployment already exists
#     if deployment_metadata_file.exists():
#         with open(deployment_metadata_file) as f:
#             metadata = json.load(f)

#         # Verify the deployment path exists on remote
#         try:
#             result = subprocess.run(
#                 [
#                     "docker", "exec", "slurmctld",
#                     "test", "-d", metadata["deployment_path"]
#                 ],
#                 capture_output=True,
#                 timeout=10,
#             )

#             if result.returncode == 0:
#                 print(f"✅ Using existing deployment: {metadata['deployment_path']}")
#                 yield metadata
#                 return

#         except subprocess.TimeoutExpired:
#             pass

#     # Create new deployment
#     print("\n🏗️  Creating production deployment...")

#     # Detect platform
#     import platform
#     machine = platform.machine().lower()

#     if machine in ("arm64", "aarch64"):
#         deploy_command = "deploy-prod-docker-aarch"
#         print("📦 Detected ARM64 architecture")
#     else:
#         deploy_command = "deploy-prod-docker"
#         print("📦 Detected x86_64 architecture")

#     try:
#         result = subprocess.run(
#             ["pixi", "run", deploy_command],
#             cwd=example_project_dir,
#             capture_output=True,
#             text=True,
#             timeout=600,  # 10 minutes for deployment
#         )

#         if result.returncode != 0:
#             print(f"❌ Deployment failed:\n{result.stderr}")
#             pytest.skip("Could not create production deployment")

#         print("✅ Production deployment created")

#         # Read the metadata
#         with open(deployment_metadata_file) as f:
#             metadata = json.load(f)

#         print(f"📍 Deployment path: {metadata['deployment_path']}")
#         print(f"🔖 Git commit: {metadata['git_commit_short']}")
#         print(f"🖥️  Platform: {metadata['platform']}")

#         yield metadata

#         # Cleanup is handled by the deployment itself

#     except subprocess.TimeoutExpired:
#         pytest.skip("Deployment timed out")
