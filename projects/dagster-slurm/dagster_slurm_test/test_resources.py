"""Tests for resources."""

import pytest
from dagster_slurm import (
    ComputeResource,
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


def test_ssh_resource_from_env(monkeypatch, mock_ssh_key_path: Path):
    """Test SSH resource creation from environment."""
    monkeypatch.setenv("SLURM_SSH_HOST", "cluster.example.com")
    monkeypatch.setenv("SLURM_SSH_PORT", "2222")
    monkeypatch.setenv("SLURM_SSH_USER", "admin")
    # FIX: Point to an actual file
    monkeypatch.setenv("SLURM_SSH_KEY", str(mock_ssh_key_path))

    ssh = SSHConnectionResource.from_env()

    assert ssh.host == "cluster.example.com"
    assert ssh.port == 2222
    assert ssh.user == "admin"
    assert ssh.key_path == str(mock_ssh_key_path)


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
        remote_base="/home/testuser/dagster",
    )

    assert slurm.ssh.host == "cluster.example.com"
    assert slurm.queue.partition == "batch"
    assert slurm.remote_base == "/home/testuser/dagster"


def test_compute_resource_local_mode():
    """Test compute resource in local mode."""
    compute = ComputeResource(mode=ExecutionMode.LOCAL)

    assert compute.mode == "local"
    assert compute.slurm is None
    assert isinstance(compute.default_launcher, BashLauncher)


def test_compute_resource_slurm_mode():
    """Test compute resource in Slurm mode."""
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

    compute = ComputeResource(mode=ExecutionMode.SLURM, slurm=slurm)

    assert compute.mode == ExecutionMode.SLURM
    assert compute.slurm is not None


def test_compute_resource_validation():
    """Test compute resource validation."""
    # Should raise error: slurm mode requires slurm resource
    with pytest.raises(ValueError, match="slurm resource required"):
        ComputeResource(mode=ExecutionMode.SLURM)
