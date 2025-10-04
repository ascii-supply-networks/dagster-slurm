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
            cpus="2",
            mem="1G",
        ),
        remote_base="/tmp/dagster_test",
        remote_python="python3",
    )


@pytest.fixture
def local_compute_resource():
    """Local compute resource for testing."""
    return ComputeResource(mode=ExecutionMode.LOCAL)
