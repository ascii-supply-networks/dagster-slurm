import io
import json

from dagster_slurm.helpers.message_readers import SSHMessageReader
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
        if "-n +3 " in tail_cmd:
            return _FakeProcess([])
        raise AssertionError(f"Unexpected tail command: {tail_cmd}")

    monkeypatch.setattr(
        "dagster_slurm.helpers.message_readers.subprocess.Popen", fake_popen
    )

    reader = SSHMessageReader(
        remote_path="/tmp/messages.jsonl",
        ssh_config=ssh_resource,
        max_reconnect_attempts=1,
    )
    handler = _CollectingHandler()

    reader._read_loop_with_reconnect(handler)

    assert [message["method"] for message in handler.messages] == [
        "opened",
        "report_asset_materialization",
    ]
    assert len(commands) == 2
    assert "-n +1 " in commands[0][-1]
    assert "-n +3 " in commands[1][-1]


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
