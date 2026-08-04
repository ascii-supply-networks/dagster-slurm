import io
import json
from types import SimpleNamespace
from typing import Any, cast

from dagster_slurm.helpers.message_readers import LocalMessageReader, SSHMessageReader
from dagster_slurm.resources.ssh import SSHConnectionResource


class _CollectingHandler:
    def __init__(self) -> None:
        self.messages: list[dict] = []

    def handle_message(self, message) -> None:
        self.messages.append(message)


class _FakeProcess:
    def __init__(self, lines: list[str]) -> None:
        self.stdout = iter(lines)
        self.stderr = io.StringIO("")

    def wait(self) -> int:
        return 0


def test_ssh_message_reader_resumes_after_reconnect(monkeypatch, tmp_path):
    key_path = tmp_path / "id_test"
    key_path.write_text("dummy-key")

    ssh_resource = SSHConnectionResource(
        host="example.com",
        port=22,
        user="testuser",
        key_path=str(key_path),
    )

    commands: list[list[str]] = []
    payload_lines = [
        json.dumps({"method": "opened", "params": {}}) + "\n",
        json.dumps(
            {
                "method": "log_external_stream",
                "params": {"stream": "stdout", "text": "hello\n"},
            }
        )
        + "\n",
        json.dumps(
            {
                "method": "log_external_stream",
                "params": {"stream": "stderr", "text": "å"},
            }
        )
        + "\n",
        json.dumps(
            {
                "method": "report_asset_materialization",
                "params": {"asset_key": "myprefix/orders"},
            }
        )
        + "\n",
    ]

    def fake_popen(cmd, stdout=None, stderr=None, text=None, bufsize=None):
        del stdout, stderr, text, bufsize
        commands.append(cmd)
        tail_cmd = cmd[-1]
        if "-n +1 " in tail_cmd:
            return _FakeProcess(payload_lines)
        if "-n +5 " in tail_cmd:
            return _FakeProcess([])
        raise AssertionError(f"Unexpected tail command: {tail_cmd}")

    monkeypatch.setattr(
        "dagster_slurm.helpers.message_readers.subprocess.Popen", fake_popen
    )

    reader = SSHMessageReader(
        remote_path="/tmp/messages.jsonl",
        ssh_config=ssh_resource,
        max_reconnect_attempts=1,
        # The fake tail exits instantly; treat that as a healthy session so
        # these tests exercise resume/close handling rather than the new
        # flapping-connection backoff.
        healthy_session_seconds=0.0,
    )
    handler = _CollectingHandler()

    reader._read_loop_with_reconnect(handler)

    assert [message["method"] for message in handler.messages] == [
        "opened",
        "log_external_stream",
        "log_external_stream",
        "report_asset_materialization",
    ]
    assert reader._stdio_messages == {"stdout": 1, "stderr": 1}
    assert reader._stdio_bytes == {"stdout": 6, "stderr": 2}
    assert len(commands) == 2
    assert "-n +1 " in commands[0][-1]
    assert "-n +5 " in commands[1][-1]


def test_ssh_message_reader_tracks_closed_exception(monkeypatch, tmp_path):
    key_path = tmp_path / "id_test"
    key_path.write_text("dummy-key")

    ssh_resource = SSHConnectionResource(
        host="example.com",
        port=22,
        user="testuser",
        key_path=str(key_path),
    )

    payload_lines = [
        json.dumps({"method": "opened", "params": {}}) + "\n",
        json.dumps(
            {
                "method": "closed",
                "params": {
                    "exception": {
                        "name": "RuntimeError",
                        "message": "boom",
                        "stack": ["line 1"],
                        "cause": None,
                        "context": None,
                    }
                },
            }
        )
        + "\n",
    ]

    popen_calls = 0

    def fake_popen(cmd, stdout=None, stderr=None, text=None, bufsize=None):
        nonlocal popen_calls
        del cmd, stdout, stderr, text, bufsize
        popen_calls += 1
        if popen_calls == 1:
            return _FakeProcess(payload_lines)
        return _FakeProcess([])

    monkeypatch.setattr(
        "dagster_slurm.helpers.message_readers.subprocess.Popen", fake_popen
    )

    reader = SSHMessageReader(
        remote_path="/tmp/messages.jsonl",
        ssh_config=ssh_resource,
        max_reconnect_attempts=1,
        # The fake tail exits instantly; treat that as a healthy session so
        # these tests exercise resume/close handling rather than the new
        # flapping-connection backoff.
        healthy_session_seconds=0.0,
    )
    handler = _CollectingHandler()

    reader._read_loop_with_reconnect(handler)

    assert reader.closed_message is not None
    assert reader.closed_exception == {
        "name": "RuntimeError",
        "message": "boom",
        "stack": ["line 1"],
        "cause": None,
        "context": None,
    }


def test_ssh_message_reader_emits_one_bounded_stdio_summary(monkeypatch, tmp_path):
    key_path = tmp_path / "id_test"
    key_path.write_text("dummy-key")
    reader = SSHMessageReader(
        remote_path="/tmp/messages.jsonl",
        ssh_config=SSHConnectionResource(
            host="example.com", user="testuser", key_path=str(key_path)
        ),
    )
    debug_calls: list[tuple[Any, ...]] = []
    reader.logger = cast(
        Any,
        SimpleNamespace(debug=lambda *args: debug_calls.append(args)),
    )
    monkeypatch.setattr(reader, "_read_loop_with_reconnect", lambda _handler: None)
    monkeypatch.setattr(reader, "_await_closed", lambda **_kwargs: True)
    reader.total_messages = 5
    reader._stdio_messages = {"stdout": 2, "stderr": 1}
    reader._stdio_bytes = {"stdout": 21, "stderr": 8}

    with reader.read_messages(_CollectingHandler()):
        pass

    summary_calls = [
        call for call in debug_calls if call[0].startswith("SSHMessageReader summary:")
    ]
    assert summary_calls == [
        (
            "SSHMessageReader summary: total_messages=%d; "
            "stdout_messages=%d, stdout_bytes=%d; "
            "stderr_messages=%d, stderr_bytes=%d",
            5,
            2,
            21,
            1,
            8,
        )
    ]


def test_ssh_message_reader_control_path_keeps_key_auth_opts(tmp_path):
    key_path = tmp_path / "id_test"
    key_path.write_text("dummy-key")

    ssh_resource = SSHConnectionResource(
        host="example.com",
        port=2222,
        user="testuser",
        key_path=str(key_path),
    )

    reader = SSHMessageReader(
        remote_path="/tmp/messages.jsonl",
        ssh_config=ssh_resource,
        control_path="/tmp/dagster-slurm-control",
    )

    cmd = reader._build_ssh_tail_command()

    assert cmd is not None
    assert "ControlPath=/tmp/dagster-slurm-control" in cmd
    assert "-i" in cmd
    assert cmd[cmd.index("-i") + 1] == str(key_path)
    assert "IdentitiesOnly=yes" in cmd
    assert "BatchMode=yes" in cmd


def test_local_message_reader_uses_configurable_closed_drain_timeout(tmp_path):
    messages_path = tmp_path / "messages.jsonl"
    messages_path.write_text(
        "\n".join(
            [
                json.dumps({"method": "opened", "params": {}}),
                json.dumps({"method": "closed", "params": {}}),
            ]
        )
        + "\n"
    )
    reader = LocalMessageReader(
        messages_path=str(messages_path),
        poll_interval=0.01,
        creation_timeout=0.1,
        closed_message_drain_timeout=0.01,
    )
    handler = _CollectingHandler()

    reader._tail_file(handler)

    assert [message["method"] for message in handler.messages] == [
        "opened",
        "closed",
    ]


def test_ssh_message_reader_tail_multiplexes_and_keeps_proxy_jump(
    isolated_ssh_control_dir, tmp_path
):
    """A tail without ControlMaster (or ProxyJump) is a direct login-node hit."""

    jump_key = tmp_path / "jump_key"
    jump_key.write_text("dummy-key")
    target_key = tmp_path / "target_key"
    target_key.write_text("dummy-key")

    jump = SSHConnectionResource(
        host="jump.example.com", user="jumpuser", key_path=str(jump_key)
    )
    target = SSHConnectionResource(
        host="target.example.com",
        user="testuser",
        key_path=str(target_key),
        jump_host=jump,
    )

    cmd = SSHMessageReader(
        remote_path="/tmp/messages.jsonl", ssh_config=target
    )._build_ssh_tail_command()

    assert cmd is not None
    joined = " ".join(cmd)
    assert "ProxyCommand=" in joined
    assert "ControlMaster=auto" in joined
    assert f"ControlPath={isolated_ssh_control_dir}/cm-" in joined


def test_ssh_message_reader_tracks_the_pool_control_path(tmp_path):
    key_path = tmp_path / "id_test"
    key_path.write_text("dummy-key")
    ssh_resource = SSHConnectionResource(
        host="example.com", user="testuser", key_path=str(key_path)
    )

    pool = SimpleNamespace(control_path="/tmp/pool-socket")
    reader = SSHMessageReader(
        remote_path="/tmp/messages.jsonl",
        ssh_config=ssh_resource,
        control_path="/tmp/stale-socket",
        ssh_pool=cast(Any, pool),
    )

    assert reader.control_path == "/tmp/pool-socket"

    # Once the pool abandons its socket the reader must stop using it too.
    pool.control_path = None
    assert reader.control_path is None


def test_reconnect_delay_backs_off_and_is_capped(tmp_path):
    key_path = tmp_path / "id_test"
    key_path.write_text("dummy-key")
    reader = SSHMessageReader(
        remote_path="/tmp/messages.jsonl",
        ssh_config=SSHConnectionResource(
            host="example.com", user="testuser", key_path=str(key_path)
        ),
        reconnect_interval=2.0,
        max_reconnect_interval=10.0,
    )

    assert reader._reconnect_delay(0) == 2.0
    assert reader._reconnect_delay(1) == 2.0
    assert reader._reconnect_delay(2) == 4.0
    assert reader._reconnect_delay(3) == 8.0
    assert reader._reconnect_delay(9) == 10.0


def test_flapping_tail_stops_reconnecting(monkeypatch, tmp_path):
    """A tail that dies right after each message must not reconnect forever."""
    key_path = tmp_path / "id_test"
    key_path.write_text("dummy-key")
    ssh_resource = SSHConnectionResource(
        host="example.com", user="testuser", key_path=str(key_path)
    )

    attempts = {"count": 0}

    def fake_popen(cmd, stdout=None, stderr=None, text=None, bufsize=None):
        del cmd, stdout, stderr, text, bufsize
        attempts["count"] += 1
        return _FakeProcess([json.dumps({"method": "opened", "params": {}}) + "\n"])

    monkeypatch.setattr(
        "dagster_slurm.helpers.message_readers.subprocess.Popen", fake_popen
    )

    reader = SSHMessageReader(
        remote_path="/tmp/messages.jsonl",
        ssh_config=ssh_resource,
        reconnect_interval=0.0,
        max_reconnect_attempts=3,
    )
    reader._read_loop_with_reconnect(_CollectingHandler())

    assert attempts["count"] == 3
