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
