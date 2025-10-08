"""Pytest configuration and fixtures."""

import tempfile
from pathlib import Path

import pytest
from dagster_slurm import (
    ComputeResource,
    SlurmQueueConfig,
    SlurmResource,
    SSHConnectionResource,
)
from dagster_slurm.config.environment import ExecutionMode
from dagster_slurm import ComputeResource, BashLauncher


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
