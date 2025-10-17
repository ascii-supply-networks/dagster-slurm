import os
import subprocess
from pathlib import Path

import pytest

from dagster_slurm.helpers.ssh_pool import SSHConnectionPool
from dagster_slurm.resources.ssh import SSHConnectionResource


class DummyResult:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def test_control_master_fallback_key_auth(monkeypatch, tmp_path):
    """Ensure we gracefully fall back to direct SSH when ControlMaster fails."""

    key_path = tmp_path / "id_test"
    key_path.write_text("dummy-key")
    os.chmod(key_path, 0o600)

    ssh_resource = SSHConnectionResource(
        host="example.com",
        port=22,
        user="testuser",
        key_path=str(key_path),
    )

    commands = []

    def fake_run(cmd, *args, **kwargs):
        commands.append(cmd)
        if "-M" in cmd:
            # Simulate ControlMaster failure
            return DummyResult(returncode=255, stderr="ControlMaster not permitted")
        if cmd[0] == "scp":
            return DummyResult(returncode=0)
        if cmd[0] == "ssh":
            return DummyResult(returncode=0, stdout="ok\n")
        return DummyResult(returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    pool = SSHConnectionPool(ssh_resource)

    with pool:
        assert pool._fallback_mode is True
        assert pool.control_path is None

        output = pool.run("echo fallback")
        assert output == "ok\n"

        local_file = tmp_path / "payload.txt"
        local_file.write_text("data")
        pool.upload_file(str(local_file), "/tmp/remote.txt")

    # Ensure fallback commands do not rely on ControlPath
    fallback_calls = [
        cmd for cmd in commands if cmd and cmd[0] == "ssh" and "-M" not in cmd
    ]
    assert any("-o" in cmd for cmd in fallback_calls)
    assert not any("ControlPath" in " ".join(cmd) for cmd in fallback_calls)


def test_control_master_failure_password_raises(monkeypatch):
    """Password authentication must still raise if ControlMaster is unavailable."""

    ssh_resource = SSHConnectionResource(
        host="example.com",
        port=22,
        user="testuser",
        password="secret",
    )

    def fake_run_with_password(*args, **kwargs):
        return DummyResult(returncode=1, stderr="password auth disabled")

    monkeypatch.setattr(
        SSHConnectionPool, "_run_with_password", staticmethod(fake_run_with_password)
    )

    pool = SSHConnectionPool(ssh_resource)

    with pytest.raises(RuntimeError):
        with pool:
            pass
