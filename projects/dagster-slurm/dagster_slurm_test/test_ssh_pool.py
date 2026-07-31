import logging
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
        if "-O" in cmd:
            # No live master behind the control socket
            return DummyResult(returncode=255, stderr="No such file or directory")
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
        assert pool.multiplexing_active is False
        assert "ControlMaster not permitted" in (pool.fallback_reason or "")
        assert pool.control_path is None

        output = pool.run("echo fallback")
        assert output == "ok\n"

        local_file = tmp_path / "payload.txt"
        local_file.write_text("data")
        pool.upload_file(str(local_file), "/tmp/remote.txt")

    # Ensure fallback commands do not rely on ControlPath
    fallback_calls = [
        cmd
        for cmd in commands
        if cmd and cmd[0] == "ssh" and "-M" not in cmd and "-O" not in cmd
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
        assert pool.multiplexing_active is False
        assert "password-based authentication" in (pool.fallback_reason or "")
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


def _user_ssh_config(tmp_path, host: str = "example.com") -> str:
    config = tmp_path / "user_ssh_config"
    config.write_text(
        f"Host {host}\n"
        "  StrictHostKeyChecking no\n"
        "  UserKnownHostsFile /my/hosts\n"
        "  BatchMode no\n"
    )
    return str(config)


def _resolved_with_config(command: list[str], config: str) -> dict[str, str]:
    result = subprocess.run(
        ["ssh", "-G", "-F", config, *command[1:]],
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
    "variable",
    ["TEST_SSH_KNOWN_HOSTS_FILE", "TEST_SSH_HOST_KEY_CHECKING", "TEST_SSH_KEY"],
)
def test_blank_environment_values_mean_unset(monkeypatch, variable):
    """`FOO=` in a dotenv file must not fail validation."""
    monkeypatch.setenv("TEST_SSH_HOST", "example.com")
    monkeypatch.setenv("TEST_SSH_USER", "testuser")
    monkeypatch.setenv("TEST_SSH_PASSWORD", "secret")
    monkeypatch.setenv(variable, "   ")

    ssh_resource = SSHConnectionResource.from_env(prefix="TEST_SSH")

    assert ssh_resource.known_hosts_file is None
    assert ssh_resource.host_key_checking == "strict"
    assert ssh_resource.key_path is None


def test_inherit_host_key_checking_defers_to_user_ssh_config(tmp_path):
    """'inherit' must emit no host-key options so ~/.ssh/config wins."""
    key_path = tmp_path / "id_test"
    key_path.write_text("dummy-key")
    ssh_resource = SSHConnectionResource(
        host="example.com",
        user="testuser",
        key_path=str(key_path),
        host_key_checking="inherit",
    )

    assert ssh_resource.get_host_key_opts() == []

    resolved = _resolved_with_config(
        ssh_resource.get_ssh_base_command(), _user_ssh_config(tmp_path)
    )
    assert resolved["stricthostkeychecking"] == "false"
    assert resolved["userknownhostsfile"] == "/my/hosts"


def test_strict_host_key_checking_still_overrides_user_ssh_config(tmp_path):
    """The default stays explicit, so a run is reproducible across machines."""
    key_path = tmp_path / "id_test"
    key_path.write_text("dummy-key")
    ssh_resource = SSHConnectionResource(
        host="example.com", user="testuser", key_path=str(key_path)
    )

    resolved = _resolved_with_config(
        ssh_resource.get_ssh_base_command(), _user_ssh_config(tmp_path)
    )
    assert resolved["stricthostkeychecking"] == "true"


def test_inherit_still_honours_an_explicit_known_hosts_file(tmp_path):
    known_hosts = tmp_path / "known_hosts"
    ssh_resource = SSHConnectionResource(
        host="example.com",
        user="testuser",
        password="secret",
        host_key_checking="inherit",
        known_hosts_file=str(known_hosts),
    )

    assert ssh_resource.get_host_key_opts() == [
        "-o",
        f"UserKnownHostsFile={known_hosts}",
    ]


def test_batch_mode_can_be_inherited(tmp_path):
    """A passphrase-protected key needs OpenSSH to be allowed to prompt."""
    key_path = tmp_path / "id_test"
    key_path.write_text("dummy-key")
    strict = SSHConnectionResource(
        host="example.com", user="testuser", key_path=str(key_path)
    )
    inheriting = SSHConnectionResource(
        host="example.com",
        user="testuser",
        key_path=str(key_path),
        batch_mode=None,
    )

    assert "BatchMode=yes" in strict.get_key_auth_opts()
    assert not any("BatchMode" in opt for opt in inheriting.get_key_auth_opts())

    resolved = _resolved_with_config(
        inheriting.get_ssh_base_command(), _user_ssh_config(tmp_path)
    )
    assert resolved["batchmode"] == "no"


def test_connection_without_key_or_password_defers_authentication():
    """No explicit credential means ssh-agent and ~/.ssh/config decide."""
    ssh_resource = SSHConnectionResource(host="example.com", user="testuser")

    assert ssh_resource.uses_inherited_auth is True
    command = " ".join(ssh_resource.get_ssh_base_command())
    assert "-i " not in command
    assert "PreferredAuthentications" not in command
    assert "PubkeyAuthentication" not in command


def test_both_credentials_are_still_rejected(tmp_path):
    key_path = tmp_path / "id_test"
    key_path.write_text("dummy-key")
    with pytest.raises(ValidationError, match="Cannot specify both"):
        SSHConnectionResource(
            host="example.com",
            user="testuser",
            key_path=str(key_path),
            password="secret",
        )


def test_unconfigured_jump_host_is_handed_back_to_openssh():
    """-J keeps the operator's own bastion settings from ~/.ssh/config."""
    jump = SSHConnectionResource(
        host="jump.example.com",
        user="jumpuser",
        host_key_checking="inherit",
        batch_mode=None,
    )
    target = SSHConnectionResource(
        host="target.example.com",
        user="testuser",
        password="secret",
        jump_host=jump,
    )

    assert jump.defers_to_ssh_config is True
    assert target.get_proxy_command_opts() == ["-J", "jumpuser@jump.example.com"]


def test_configured_jump_host_still_uses_an_explicit_proxy_command(tmp_path):
    jump_key = tmp_path / "jump_key"
    jump_key.write_text("dummy-key")
    jump = SSHConnectionResource(
        host="jump.example.com", user="jumpuser", key_path=str(jump_key)
    )
    target = SSHConnectionResource(
        host="target.example.com",
        user="testuser",
        password="secret",
        jump_host=jump,
    )

    opts = target.get_proxy_command_opts()
    assert opts[0] == "-o"
    assert opts[1].startswith("ProxyCommand=")
    assert str(jump_key) in opts[1]


def test_batch_mode_failures_get_an_actionable_hint(tmp_path):
    key_path = tmp_path / "id_test"
    key_path.write_text("dummy-key")
    strict = SSHConnectionResource(
        host="example.com", user="testuser", key_path=str(key_path)
    )
    inheriting = strict.model_copy(update={"batch_mode": None})

    hint = strict.auth_error_hint(
        "testuser@example.com: Permission denied (publickey)."
    )
    assert "ssh-agent" in hint
    assert inheriting.auth_error_hint("Permission denied (publickey).") == ""
    assert strict.auth_error_hint("some unrelated failure") == ""


def _key_auth_resource(tmp_path, *, port: int = 22, host: str = "example.com"):
    key_path = tmp_path / "id_test"
    if not key_path.exists():
        key_path.write_text("dummy-key")
        os.chmod(key_path, 0o600)
    return SSHConnectionResource(
        host=host,
        port=port,
        user="testuser",
        key_path=str(key_path),
    )


def _master_lifecycle_stub(commands: list[list[str]], state: dict):
    """Fake ``subprocess.run`` that models an OpenSSH ControlMaster."""

    def fake_run(cmd, *args, **kwargs):
        commands.append(cmd)
        if "-O" in cmd:
            if "exit" in cmd:
                state["alive"] = False
                return DummyResult(returncode=0)
            return DummyResult(returncode=0 if state["alive"] else 255)
        if "-M" in cmd:
            if not state.get("spawn_ok", True):
                return DummyResult(returncode=255, stderr="connection refused")
            state["alive"] = True
            return DummyResult(returncode=0)
        return DummyResult(returncode=0, stdout="ok\n")

    return fake_run


def test_control_socket_is_shared_per_target(monkeypatch, tmp_path):
    """The socket name must not be random, or every pool re-handshakes."""
    monkeypatch.setenv("DAGSTER_SLURM_SSH_CONTROL_DIR", str(tmp_path / "control"))

    resource = _key_auth_resource(tmp_path)
    first = SSHConnectionPool(resource).control_path
    second = SSHConnectionPool(resource).control_path

    assert first is not None
    assert first == second
    assert first.endswith("cm-testuser@example.com:22")

    other_port = SSHConnectionPool(_key_auth_resource(tmp_path, port=2222)).control_path
    assert other_port != first


def test_password_auth_has_no_control_socket(tmp_path, monkeypatch):
    monkeypatch.setenv("DAGSTER_SLURM_SSH_CONTROL_DIR", str(tmp_path / "control"))
    resource = SSHConnectionResource(host="example.com", user="testuser", password="s")

    assert resource.supports_multiplexing is False
    assert resource.control_socket_path() is None
    assert SSHConnectionPool(resource).control_path is None


def test_live_master_is_reused_instead_of_respawned(monkeypatch, tmp_path):
    monkeypatch.setenv("DAGSTER_SLURM_SSH_CONTROL_DIR", str(tmp_path / "control"))
    commands: list[list[str]] = []
    state = {"alive": True}
    monkeypatch.setattr(subprocess, "run", _master_lifecycle_stub(commands, state))

    with SSHConnectionPool(_key_auth_resource(tmp_path)) as pool:
        assert pool.multiplexing_active is True

    assert not [cmd for cmd in commands if "-M" in cmd]


def test_exit_leaves_master_alive_for_the_next_caller(monkeypatch, tmp_path):
    monkeypatch.setenv("DAGSTER_SLURM_SSH_CONTROL_DIR", str(tmp_path / "control"))
    commands: list[list[str]] = []
    state = {"alive": False}
    monkeypatch.setattr(subprocess, "run", _master_lifecycle_stub(commands, state))

    with SSHConnectionPool(_key_auth_resource(tmp_path)):
        pass

    assert len([cmd for cmd in commands if "-M" in cmd]) == 1
    assert not [cmd for cmd in commands if "-O" in cmd and "exit" in cmd]
    assert state["alive"] is True


def test_master_shutdown_can_be_forced_via_env(monkeypatch, tmp_path):
    monkeypatch.setenv("DAGSTER_SLURM_SSH_CONTROL_DIR", str(tmp_path / "control"))
    monkeypatch.setenv("DAGSTER_SLURM_SSH_CLOSE_MASTER_ON_EXIT", "1")
    commands: list[list[str]] = []
    state = {"alive": False}
    monkeypatch.setattr(subprocess, "run", _master_lifecycle_stub(commands, state))

    with SSHConnectionPool(_key_auth_resource(tmp_path)):
        pass

    assert [cmd for cmd in commands if "-O" in cmd and "exit" in cmd]
    assert state["alive"] is False


def test_nested_context_does_not_start_a_second_master(monkeypatch, tmp_path):
    monkeypatch.setenv("DAGSTER_SLURM_SSH_CONTROL_DIR", str(tmp_path / "control"))
    commands: list[list[str]] = []
    state = {"alive": False}
    monkeypatch.setattr(subprocess, "run", _master_lifecycle_stub(commands, state))

    pool = SSHConnectionPool(_key_auth_resource(tmp_path))
    with pool:
        with pool:
            assert pool.multiplexing_active is True
        assert pool.multiplexing_active is True

    assert len([cmd for cmd in commands if "-M" in cmd]) == 1


def test_dead_master_is_restarted_before_the_next_command(monkeypatch, tmp_path):
    """A stale socket makes OpenSSH reconnect silently - detect and heal it."""
    monkeypatch.setenv("DAGSTER_SLURM_SSH_CONTROL_DIR", str(tmp_path / "control"))
    commands: list[list[str]] = []
    state = {"alive": False}
    monkeypatch.setattr(subprocess, "run", _master_lifecycle_stub(commands, state))

    pool = SSHConnectionPool(_key_auth_resource(tmp_path))
    pool.MASTER_CHECK_INTERVAL_SECONDS = 0.0

    with pool:
        assert len([cmd for cmd in commands if "-M" in cmd]) == 1
        state["alive"] = False  # master died mid-run
        assert pool.run("true") == "ok\n"

        assert len([cmd for cmd in commands if "-M" in cmd]) == 2
        assert pool.multiplexing_active is True


def test_unrecoverable_master_loss_is_reported_loudly(monkeypatch, tmp_path):
    monkeypatch.setenv("DAGSTER_SLURM_SSH_CONTROL_DIR", str(tmp_path / "control"))
    commands: list[list[str]] = []
    state = {"alive": False, "spawn_ok": True}
    monkeypatch.setattr(subprocess, "run", _master_lifecycle_stub(commands, state))

    reported: list[tuple[str, str]] = []
    pool = SSHConnectionPool(_key_auth_resource(tmp_path))
    pool.MASTER_CHECK_INTERVAL_SECONDS = 0.0

    with pool:
        pool.reporter = lambda level, message: reported.append((level, message))
        state["alive"] = False
        state["spawn_ok"] = False

        assert pool.run("true") == "ok\n"

    assert pool.multiplexing_active is False
    assert pool.multiplexing_unsupported is False
    assert len(reported) == 1
    level, message = reported[0]
    assert level == "error"
    assert "SSH MULTIPLEXING FAILED" in message
    assert "could cause the account to be blocked" in message


def test_one_off_commands_attach_to_the_shared_socket(monkeypatch, tmp_path):
    """Helper commands outside the pool must multiplex too."""
    monkeypatch.setenv("DAGSTER_SLURM_SSH_CONTROL_DIR", str(tmp_path / "control"))
    resource = _key_auth_resource(tmp_path)

    resolved = _resolved_ssh_config(resource.get_ssh_base_command())

    assert resolved["controlmaster"] == "auto"
    assert resolved["controlpath"].endswith("cm-testuser@example.com:22")

    scp_command = resource.get_scp_base_command()
    assert "ControlMaster=auto" in " ".join(scp_command)


def test_pooled_commands_never_promote_themselves_to_master(monkeypatch, tmp_path):
    monkeypatch.setenv("DAGSTER_SLURM_SSH_CONTROL_DIR", str(tmp_path / "control"))
    commands: list[list[str]] = []
    state = {"alive": True}
    monkeypatch.setattr(subprocess, "run", _master_lifecycle_stub(commands, state))

    with SSHConnectionPool(_key_auth_resource(tmp_path)) as pool:
        pool.run("true")

    pooled = [
        cmd
        for cmd in commands
        if cmd and cmd[0] == "ssh" and "-M" not in cmd and "-O" not in cmd
    ]
    assert pooled
    for cmd in pooled:
        assert "ControlMaster=no" in cmd


def test_password_auth_is_reported_as_unsupported_not_failed():
    """Password auth cannot be multiplexed - that is not a ControlMaster failure.

    The early return happens before ControlMaster is ever attempted, so calling
    it a failure was both untrue and unactionable: there is no ControlMaster
    configuration to fix, and it fired on every run of such a deployment.
    """
    ssh_resource = SSHConnectionResource(
        host="example.com", user="testuser", password="secret"
    )
    reports: list[tuple[str, str]] = []

    pool = SSHConnectionPool(ssh_resource)
    pool.reporter = lambda level, message: reports.append((level, message))
    with pool:
        pass

    assert pool.multiplexing_unsupported is True
    assert pool.multiplexing_active is False
    assert len(reports) == 1
    level, message = reports[0]
    assert level == "warning"
    assert "SSH MULTIPLEXING FAILED" not in message
    assert "Stop the run" not in message
    assert "password-based authentication" in message


def test_pool_reports_its_own_state_without_a_reporter(caplog):
    """Sensors, session setup and hetjob submission report without extra wiring.

    They create a pool and never call back into the Pipes client, so the report
    has to come from the pool itself or it would never be emitted at all.
    """
    ssh_resource = SSHConnectionResource(
        host="example.com", user="testuser", password="secret"
    )

    with caplog.at_level(logging.WARNING):
        with SSHConnectionPool(ssh_resource):
            pass

    assert any(
        "SSH multiplexing is unavailable" in record.message for record in caplog.records
    )


def test_healthy_pool_describes_no_degradation(monkeypatch, tmp_path):
    commands: list[list[str]] = []
    state = {"alive": True}
    monkeypatch.setattr(subprocess, "run", _master_lifecycle_stub(commands, state))

    reports: list[tuple[str, str]] = []
    pool = SSHConnectionPool(_key_auth_resource(tmp_path))
    pool.reporter = lambda level, message: reports.append((level, message))
    with pool:
        assert pool.multiplexing_active is True

    assert pool.describe_multiplexing() is None
    assert reports == []
