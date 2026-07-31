import os
import re
import shlex
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError
from dagster_slurm.helpers import ssh_pool as ssh_pool_module
from dagster_slurm.helpers.ssh_pool import SSHConnectionPool
from dagster_slurm.resources.ssh import SSHConnectionResource


class DummyResult:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def _resolved_ssh_config(command: list[str]) -> dict[str, str]:
    result = subprocess.run(
        ["ssh", "-G", "-F", "/dev/null", *command[1:]],
        check=True,
        capture_output=True,
        text=True,
    )
    return {
        key: value
        for line in result.stdout.splitlines()
        for key, _, value in [line.partition(" ")]
    }


@pytest.mark.parametrize(
    ("mode", "expected_checking"),
    [
        ("off", "false"),
        ("accept-new", "accept-new"),
        ("strict", "true"),
    ],
)
def test_host_key_modes_are_applied_by_openssh(
    tmp_path,
    mode,
    expected_checking,
):
    key_path = tmp_path / "id_test"
    key_path.write_text("dummy-key")
    known_hosts_path = tmp_path / "known_hosts"
    ssh_resource = SSHConnectionResource(
        host="example.com",
        user="testuser",
        key_path=str(key_path),
        host_key_checking=mode,
        known_hosts_file=(str(known_hosts_path) if mode != "off" else None),
    )

    resolved = _resolved_ssh_config(ssh_resource.get_ssh_base_command())

    assert resolved["stricthostkeychecking"] == expected_checking
    assert resolved["userknownhostsfile"] == (
        str(known_hosts_path) if mode != "off" else "/dev/null"
    )


def test_host_key_checking_defaults_to_strict_in_openssh(tmp_path):
    key_path = tmp_path / "id_test"
    key_path.write_text("dummy-key")
    ssh_resource = SSHConnectionResource(
        host="example.com",
        user="testuser",
        key_path=str(key_path),
    )

    resolved = _resolved_ssh_config(ssh_resource.get_ssh_base_command())

    assert resolved["stricthostkeychecking"] == "true"
    assert resolved["userknownhostsfile"] == str(Path.home() / ".ssh" / "known_hosts")


def test_extra_opts_override_host_key_defaults_in_openssh(tmp_path):
    key_path = tmp_path / "id_test"
    key_path.write_text("dummy-key")
    known_hosts_path = tmp_path / "caller_known_hosts"
    ssh_resource = SSHConnectionResource(
        host="example.com",
        user="testuser",
        key_path=str(key_path),
        host_key_checking="off",
        extra_opts=[
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            f"UserKnownHostsFile={known_hosts_path}",
        ],
    )

    resolved = _resolved_ssh_config(ssh_resource.get_ssh_base_command())

    assert resolved["stricthostkeychecking"] == "true"
    assert resolved["userknownhostsfile"] == str(known_hosts_path)


def test_jump_host_policy_is_applied_by_openssh(tmp_path):
    target_key_path = tmp_path / "target_key"
    target_key_path.write_text("dummy-key")
    jump_key_path = tmp_path / "jump_key"
    jump_key_path.write_text("dummy-key")
    jump_known_hosts_path = tmp_path / "jump_known_hosts"
    jump = SSHConnectionResource(
        host="jump.example.com",
        user="jumpuser",
        key_path=str(jump_key_path),
        host_key_checking="accept-new",
        known_hosts_file=str(jump_known_hosts_path),
    )
    target = SSHConnectionResource(
        host="target.example.com",
        user="targetuser",
        key_path=str(target_key_path),
        jump_host=jump,
    )

    target_config = _resolved_ssh_config(target.get_ssh_base_command())
    jump_command = shlex.split(target_config["proxycommand"])
    jump_command[jump_command.index("%h:%p")] = "target.example.com:22"
    jump_config = _resolved_ssh_config(jump_command)

    assert jump_config["hostname"] == "jump.example.com"
    assert jump_config["stricthostkeychecking"] == "accept-new"
    assert jump_config["userknownhostsfile"] == str(jump_known_hosts_path)
    assert jump_config["identityfile"] == str(jump_key_path)


def test_host_key_policy_loads_from_environment(monkeypatch, tmp_path):
    known_hosts_path = tmp_path / "known_hosts"
    monkeypatch.setenv("TEST_SSH_HOST", "example.com")
    monkeypatch.setenv("TEST_SSH_USER", "testuser")
    monkeypatch.setenv("TEST_SSH_PASSWORD", "secret")
    monkeypatch.setenv("TEST_SSH_HOST_KEY_CHECKING", "strict")
    monkeypatch.setenv("TEST_SSH_KNOWN_HOSTS_FILE", str(known_hosts_path))

    ssh_resource = SSHConnectionResource.from_env(prefix="TEST_SSH")

    assert ssh_resource.host_key_checking == "strict"
    assert ssh_resource.known_hosts_file == str(known_hosts_path)


def test_empty_known_hosts_file_is_rejected():
    with pytest.raises(ValidationError, match="known_hosts_file cannot be empty"):
        SSHConnectionResource(
            host="example.com",
            user="testuser",
            password="secret",
            known_hosts_file=" ",
        )


def test_host_key_failures_include_actionable_guidance(monkeypatch):
    ssh_resource = SSHConnectionResource(
        host="example.com",
        user="testuser",
        password="secret",
        host_key_checking="strict",
    )

    def fail_with_host_key_error(*_args, **_kwargs):
        return DummyResult(returncode=255, stderr="Host key verification failed.")

    monkeypatch.setattr(
        SSHConnectionPool,
        "_run_with_password",
        staticmethod(fail_with_host_key_error),
    )

    with SSHConnectionPool(ssh_resource) as pool:
        with pytest.raises(RuntimeError, match="Add the verified key"):
            pool.run("true")


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


def test_control_master_failure_password_fallback(monkeypatch, tmp_path):
    """Password auth now falls back gracefully when ControlMaster is skipped."""

    ssh_resource = SSHConnectionResource(
        host="example.com",
        port=22,
        user="testuser",
        password="secret",
    )

    commands = []

    def fake_run_with_password(cmd, *_, **__):
        commands.append(cmd)
        # Return stdout for ssh commands, empty for scp
        if cmd[0] == "ssh":
            return DummyResult(returncode=0, stdout="ok\n")
        return DummyResult(returncode=0)

    monkeypatch.setattr(
        SSHConnectionPool,
        "_run_with_password",
        staticmethod(fake_run_with_password),
    )

    pool = SSHConnectionPool(ssh_resource)

    with pool:
        assert pool._fallback_mode is True
        output = pool.run("echo hi")
        assert output == "ok\n"

        local_file = tmp_path / "data.txt"
        local_file.write_text("payload")
        pool.upload_file(str(local_file), "/tmp/remote.txt")

    # Ensure all commands were executed without ControlPath
    assert commands, "No password-based commands executed"
    assert all("ControlPath" not in " ".join(cmd) for cmd in commands)


def test_control_master_key_auth_commands_keep_noninteractive_opts(
    monkeypatch, tmp_path
):
    """Pooled SSH/SCP commands must stay key-based if the control socket vanishes."""

    key_path = tmp_path / "id_test"
    key_path.write_text("dummy-key")
    os.chmod(key_path, 0o600)

    ssh_resource = SSHConnectionResource(
        host="example.com",
        port=2222,
        user="testuser",
        key_path=str(key_path),
        extra_opts=["-o", "Compression=yes"],
    )

    commands = []

    def fake_run(cmd, *args, **kwargs):
        commands.append(cmd)
        if cmd[0] == "ssh":
            return DummyResult(returncode=0, stdout="ok\n")
        if cmd[0] == "scp":
            return DummyResult(returncode=0)
        return DummyResult(returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    pool = SSHConnectionPool(ssh_resource)

    with pool:
        output = pool.run("echo pooled")
        assert output == "ok\n"

        local_file = tmp_path / "payload.txt"
        local_file.write_text("data")
        pool.upload_file(str(local_file), "/tmp/remote.txt")

    pooled_ssh_calls = [
        cmd
        for cmd in commands
        if cmd and cmd[0] == "ssh" and "-M" not in cmd and "-O" not in cmd
    ]
    assert pooled_ssh_calls
    for cmd in pooled_ssh_calls:
        assert "-p" in cmd
        assert cmd[cmd.index("-p") + 1] == "2222"
        assert "-i" in cmd
        assert cmd[cmd.index("-i") + 1] == str(key_path)
        assert "IdentitiesOnly=yes" in cmd
        assert "BatchMode=yes" in cmd
        assert "Compression=yes" in cmd

    scp_calls = [cmd for cmd in commands if cmd and cmd[0] == "scp"]
    assert scp_calls
    for cmd in scp_calls:
        assert "-P" in cmd
        assert cmd[cmd.index("-P") + 1] == "2222"
        assert "-i" in cmd
        assert cmd[cmd.index("-i") + 1] == str(key_path)
        assert "IdentitiesOnly=yes" in cmd
        assert "BatchMode=yes" in cmd
        assert "Compression=yes" in cmd


def test_write_file_preserves_single_quotes(monkeypatch):
    ssh_resource = SSHConnectionResource(
        host="example.com",
        port=22,
        user="testuser",
        password="secret",
    )
    pool = SSHConnectionPool(ssh_resource)
    commands: list[str] = []

    def fake_run(cmd, *_, **__):
        commands.append(cmd)
        return ""

    monkeypatch.setattr(pool, "run", fake_run)

    pool.write_file("awk '{print $1}'\necho 'done'", "/tmp/script.sh")

    assert len(commands) == 1
    assert "awk '{print $1}'" in commands[0]
    assert "echo 'done'" in commands[0]
    assert "'\\''" not in commands[0]


def test_write_file_uses_unique_heredoc_delimiter(monkeypatch):
    ssh_resource = SSHConnectionResource(
        host="example.com",
        port=22,
        user="testuser",
        password="secret",
    )
    pool = SSHConnectionPool(ssh_resource)
    commands: list[str] = []
    uuid_hexes = iter(["collision", "safe"])

    def fake_run(cmd, *_, **__):
        commands.append(cmd)
        return ""

    monkeypatch.setattr(pool, "run", fake_run)
    monkeypatch.setattr(
        ssh_pool_module.uuid,
        "uuid4",
        lambda: SimpleNamespace(hex=next(uuid_hexes)),
    )

    pool.write_file("DAGSTER_EOF_collision\npayload", "/tmp/script.sh")

    assert len(commands) == 1
    assert "<<'DAGSTER_EOF_safe'" in commands[0]
    assert re.search(r"^DAGSTER_EOF_safe$", commands[0], re.MULTILINE)
