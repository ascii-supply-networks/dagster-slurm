"""Prove multiplexing and refusal of direct fallback with a real local sshd."""

from __future__ import annotations

import concurrent.futures
import getpass
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from dagster_slurm import (
    BashLauncher,
    SlurmPipesClient,
    SlurmQueueConfig,
    SlurmResource,
)
from dagster_slurm.helpers import SSHConnectionPool
from dagster_slurm.helpers.message_readers import SSHMessageReader
from dagster_slurm.helpers.ssh_helpers import ssh_run
from dagster_slurm.resources.ssh import SSHConnectionResource


@pytest.fixture
def local_ssh(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> Iterator[tuple[SSHConnectionResource, Path]]:
    sshd = shutil.which("sshd") or "/usr/sbin/sshd"
    if not Path(sshd).is_file():
        if os.environ.get("CI"):
            pytest.fail("local OpenSSH server is required in CI; use the devenv shell")
        pytest.skip("local OpenSSH server is unavailable")
    # StrictModes checks every ancestor of AuthorizedKeysFile; /tmp is shared.
    with tempfile.TemporaryDirectory(
        prefix=".jubust-ssh-", dir=Path.home()
    ) as directory:
        root = Path(directory)
        for name in ("host", "identity"):
            subprocess.run(
                ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(root / name)],
                check=True,
            )
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        known_hosts = root / "known_hosts"
        known_hosts.write_text(f"[127.0.0.1]:{port} {(root / 'host.pub').read_text()}")
        config = root / "sshd_config"
        config.write_text(
            f"Port {port}\nListenAddress 127.0.0.1\nHostKey {root / 'host'}\n"
            f"PidFile {root / 'sshd.pid'}\nAuthorizedKeysFile {root / 'identity.pub'}\n"
            "PasswordAuthentication no\nKbdInteractiveAuthentication no\nUsePAM no\n"
            f"StrictModes yes\nLogLevel VERBOSE\nMaxSessions {getattr(request, 'param', 8)}\nSubsystem sftp internal-sftp\n"
        )
        log = root / "server.log"
        monkeypatch.setenv("DAGSTER_SLURM_SSH_CONTROL_DIR", str(root / "control"))
        monkeypatch.delenv("DAGSTER_SLURM_SSH_CLOSE_MASTER_ON_EXIT", raising=False)
        ssh = SSHConnectionResource(
            host="127.0.0.1",
            port=port,
            user=getpass.getuser(),
            key_path=str(root / "identity"),
            host_key_checking="strict",
            known_hosts_file=str(known_hosts),
            extra_opts=[
                "-F",
                "/dev/null",
                "-o",
                "IdentitiesOnly=yes",
                "-o",
                "IdentityAgent=none",
            ],
        )
        with log.open("w") as output:
            server = subprocess.Popen(
                [sshd, "-D", "-e", "-f", str(config)], stderr=output
            )
            try:
                deadline = time.monotonic() + 5
                while "Server listening" not in log.read_text():
                    if server.poll() is not None or time.monotonic() > deadline:
                        pytest.fail(log.read_text())
                    time.sleep(0.02)
                yield ssh, log
            finally:
                # This pool addresses only the task-owned local server/socket.
                subprocess.run(
                    [
                        "ssh",
                        "-S",
                        str(SSHConnectionPool(ssh).control_path),
                        "-O",
                        "exit",
                        ssh.host,
                    ],
                    capture_output=True,
                    timeout=5,
                )
                server.terminate()
                server.wait(timeout=5)
                print(log.read_text())


def _commands(ssh: SSHConnectionResource, root: Path) -> list[list[str]]:
    local_file = root / "upload"
    local_file.write_text("payload")
    client = SlurmPipesClient(
        slurm_resource=SlurmResource(
            ssh=ssh, queue=SlurmQueueConfig(), remote_base=str(root / "remote")
        ),
        launcher=BashLauncher(),
    )
    tails = [
        SSHMessageReader(
            remote_path="/unused", ssh_config=ssh
        )._build_ssh_tail_command(),
        client._build_tail_command("/unused"),
    ]
    commands = [
        [*ssh.get_ssh_base_command(), "true"],
        [
            *ssh.get_scp_base_command(),
            str(local_file),
            f"{ssh.user}@{ssh.host}:{root / 'received'}",
        ],
    ]
    for tail in tails:
        assert tail is not None
        tail[-1] = "true"
        commands.append(tail)
    return commands


def test_commands_transfers_streams_and_legacy_helper_share_one_connection(local_ssh):
    ssh, log = local_ssh
    root = log.parent
    with SSHConnectionPool(ssh) as pool:
        assert pool.run("printf ready", timeout=5) == "ready"
        for command in _commands(ssh, root):
            subprocess.run(command, check=True, timeout=5, capture_output=True)
        pool.upload_file(str(root / "upload"), str(root / "pooled-upload"))
        assert ssh_run("printf legacy", ssh) == ("legacy", "", 0)
    assert (root / "received").read_text() == "payload"
    assert (root / "pooled-upload").read_text() == "payload"
    assert log.read_text().count("Accepted publickey") == 1


def test_missing_socket_blocks_network_fallback_for_every_command_builder(local_ssh):
    ssh, log = local_ssh
    with SSHConnectionPool(ssh) as pool:
        commands = _commands(ssh, log.parent)
        # Remove the master after command construction: this exercises the
        # health-check/exec race, not only a Python startup check.
        pool.close_master()
        before = log.read_text().count("Connection from")
        for command in commands:
            result = subprocess.run(command, capture_output=True, timeout=5)
            assert result.returncode != 0
        assert log.read_text().count("Connection from") == before
    assert log.read_text().count("Accepted publickey") == 1


@pytest.mark.parametrize("operation", ["command", "upload"])
def test_cached_health_check_does_not_allow_direct_fallback(local_ssh, operation):
    ssh, log = local_ssh
    with SSHConnectionPool(ssh) as pool:
        source = log.parent / "upload"
        source.write_text("payload")
        # Simulate loss after a successful health check, within its cache TTL.
        subprocess.run(
            ["ssh", "-S", str(pool.control_path), "-O", "exit", ssh.host],
            check=True,
            capture_output=True,
            timeout=5,
        )
        before = log.read_text().count("Connection from")
        with pytest.raises(RuntimeError, match="SSH command failed"):
            if operation == "command":
                pool.run("true", timeout=5)
            else:
                pool.upload_file(str(source), str(log.parent / "unexpected"))
        assert log.read_text().count("Connection from") == before


def test_cold_start_is_shared_across_independent_processes(local_ssh):
    ssh, log = local_ssh
    worker = (
        "import sys; "
        "from dagster_slurm.helpers import SSHConnectionPool; "
        "from dagster_slurm.resources.ssh import SSHConnectionResource; "
        "pool=SSHConnectionPool(SSHConnectionResource.model_validate_json(sys.argv[1])); "
        "pool.__enter__(); assert pool.run('printf pooled', timeout=5)=='pooled'; "
        "pool.__exit__(None,None,None)"
    )

    def command(_):
        return subprocess.run(
            [sys.executable, "-c", worker, ssh.model_dump_json()],
            check=True,
            capture_output=True,
            timeout=30,
            env=os.environ.copy(),
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(command, range(4)))
    assert log.read_text().count("Accepted publickey") == 1


@pytest.mark.parametrize("local_ssh", [2], indirect=True)
def test_session_exhaustion_does_not_open_another_connection(local_ssh):
    ssh, log = local_ssh
    with SSHConnectionPool(ssh):
        command = [*ssh.get_ssh_base_command(), "sleep 1"]
        holders = [
            subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            for _ in range(2)
        ]
        try:
            deadline = time.monotonic() + 5
            while log.read_text().count("Starting session") < 2:
                if time.monotonic() >= deadline:
                    pytest.fail(log.read_text())
                time.sleep(0.01)
            result = subprocess.run(
                [*ssh.get_ssh_base_command(), "true"],
                capture_output=True,
                timeout=5,
                check=False,
            )
            assert result.returncode != 0
            assert log.read_text().count("Accepted publickey") == 1
            assert log.read_text().count("Connection from") == 1
        finally:
            for holder in holders:
                holder.communicate(timeout=5)


def test_explicit_jump_and_target_share_their_masters(local_ssh):
    jump, log = local_ssh
    assert jump.known_hosts_file is not None
    known_hosts = Path(jump.known_hosts_file)
    known_hosts.write_text(
        known_hosts.read_text()
        + known_hosts.read_text().replace("127.0.0.1", "localhost")
    )
    target = SSHConnectionResource(
        **{**jump.model_dump(), "host": "localhost", "jump_host": jump}
    )
    with SSHConnectionPool(target) as pool:
        assert pool.run("printf through-jump", timeout=5) == "through-jump"
        for command in _commands(target, log.parent):
            subprocess.run(command, check=True, timeout=5, capture_output=True)
    with SSHConnectionPool(target) as pool:
        assert pool.run("true", timeout=5) == ""
    assert log.read_text().count("Accepted publickey") == 2
    # The fixture owns both masters; only these local sockets are closed.
    subprocess.run(
        ["ssh", "-S", str(target.control_socket_path()), "-O", "exit", target.host],
        capture_output=True,
        timeout=5,
    )
