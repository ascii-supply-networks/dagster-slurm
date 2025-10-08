"""Tests for resources."""

import pytest
from dagster_slurm import (
    SlurmQueueConfig,
    SlurmResource,
    SSHConnectionResource,
    BashLauncher,
)
from pathlib import Path
from dagster_slurm.config.environment import ExecutionMode


@pytest.fixture
def mock_ssh_key_path(tmp_path: Path) -> Path:
    key_path = tmp_path / "id_rsa"
    key_path.touch()
    return key_path


def test_ssh_resource_creation(mock_ssh_key_path: Path):
    """Test SSH resource creation."""
    ssh = SSHConnectionResource(
        host="example.com",
        port=22,
        user="testuser",
        key_path=str(mock_ssh_key_path),
    )

    assert ssh.host == "example.com"
    assert ssh.port == 22
    assert ssh.user == "testuser"
    assert ssh.key_path == str(mock_ssh_key_path)


def test_ssh_resource_creation_password():
    """Test SSH resource creation."""
    ssh = SSHConnectionResource(
        host="example.com",
        port=22,
        user="testuser",
        password="testpassword",
    )

    assert ssh.host == "example.com"
    assert ssh.port == 22
    assert ssh.user == "testuser"
    assert ssh.password == "testpassword"


def test_slurm_resource_creation(mock_ssh_key_path: Path):
    """Test Slurm resource creation."""
    ssh = SSHConnectionResource(
        host="localhost",
        port=2223,
        user="submitter",
        password="submitter",
    )

    queue = SlurmQueueConfig(
        partition="batch",
        time_limit="01:00:00",
        cpus=4,
        mem="8G",
    )

    slurm = SlurmResource(
        ssh=ssh,
        queue=queue,
        remote_base="/home/submitter/submitter",
    )

    assert slurm.ssh.host == "localhost"
    assert slurm.queue.partition == "batch"
    assert slurm.remote_base == "/home/submitter/submitter"


def test_compute_resource_local_mode(local_compute_resource):
    """Test compute resource in local mode."""
    compute = local_compute_resource

    assert compute.mode == "local"
    assert compute.slurm is None
    assert isinstance(compute.default_launcher, BashLauncher)


def test_compute_resource_slurm_mode(slurm_compute_resource):
    """Test compute resource in Slurm mode."""

    compute = slurm_compute_resource
    assert compute.mode == ExecutionMode.SLURM
    assert compute.slurm is not None
