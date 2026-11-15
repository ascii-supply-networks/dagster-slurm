import json
import subprocess
import threading
import time
from types import SimpleNamespace
from typing import Any, cast

import pytest
from dagster_slurm.helpers import message_readers as message_readers_module
from dagster_slurm.helpers.message_readers import LocalMessageReader, SSHMessageReader
from dagster_slurm.resources.ssh import SSHConnectionResource


class _CollectingHandler:
    def __init__(self) -> None:
        self.messages: list[dict] = []

    def handle_message(self, message) -> None:
        self.messages.append(message)


class _LocalShellPool:
    """Pool double that runs each read command in a local shell.

    It exercises the real remote command, and like the real pool it holds no
    session between calls: every read is one short-lived process.
    """

    control_path = "/tmp/pool-socket"

    def __init__(self, *, prefix: bytes = b"", crlf: bool = False) -> None:
        self.commands: list[str] = []
        self.prefix = prefix
        self.crlf = crlf
        self.failures: list[Exception] = []

    def run_bytes(self, cmd: str, timeout: float | None = None) -> bytes:
        self.commands.append(cmd)
        if self.failures:
            raise self.failures.pop(0)
        output = subprocess.run(
            ["bash", "--noprofile", "--norc", "-c", cmd],
            capture_output=True,
            check=True,
            timeout=timeout,
        ).stdout
        output = self.prefix + output
        if self.crlf:
            output = output.replace(b"\n", b"\r\n")
        return output


def _resource(tmp_path, **kwargs) -> SSHConnectionResource:
    key_path = tmp_path / "id_test"
    key_path.write_text("dummy-key")
    return SSHConnectionResource(
        host="example.com", user="testuser", key_path=str(key_path), **kwargs
    )


def _line(message: dict) -> bytes:
    return (json.dumps(message) + "\n").encode()


def _reader(tmp_path, pool, **kwargs) -> SSHMessageReader:
    resource = kwargs.pop("ssh_config", None) or _resource(tmp_path)
    return SSHMessageReader(
        remote_path=str(tmp_path / "run dir" / "messages.jsonl"),
        ssh_config=resource,
        ssh_pool=cast(Any, pool),
        **kwargs,
    )


def _remote_file(tmp_path):
    path = tmp_path / "run dir" / "messages.jsonl"
    path.parent.mkdir(exist_ok=True)
    return path


def test_ssh_message_reader_reads_from_byte_offsets_and_carries_partial_lines(
    tmp_path,
):
    path = _remote_file(tmp_path)
    pool = _LocalShellPool()
    reader = _reader(tmp_path, pool)
    handler = _CollectingHandler()
    tracker = message_readers_module._ClosedMessageTracker()

    # No file yet: nothing read, offset unchanged.
    assert reader._read_once(handler, tracker) == (0, False)
    assert reader._offset == 0

    stdout = _line(
        {
            "method": "log_external_stream",
            "params": {"stream": "stdout", "text": "hi\n"},
        }
    )
    stderr = _line(
        {"method": "log_external_stream", "params": {"stream": "stderr", "text": "å"}}
    )
    path.write_bytes(_line({"method": "opened", "params": {}}) + stdout + stderr[:20])
    reader._read_once(handler, tracker)
    assert [m["method"] for m in handler.messages] == ["opened", "log_external_stream"]

    # The rest of the half-written line arrives with the next read.
    with path.open("ab") as file:
        file.write(stderr[20:])
    reader._read_once(handler, tracker)

    assert [m["params"]["text"] for m in handler.messages[1:]] == ["hi\n", "å"]
    assert reader._offset == path.stat().st_size
    assert reader._partial == b""
    assert reader._stdio_messages == {"stdout": 1, "stderr": 1}
    assert reader._stdio_bytes == {"stdout": 3, "stderr": 2}
    assert reader.total_messages == 3
    # Each read starts at the byte after the previous one and quotes the path.
    assert "tail -c +1 " in pool.commands[1]
    assert (
        f"tail -c +{len(path.read_bytes()) - len(stderr) + 20 + 1} "
        in (pool.commands[2])
    )
    assert "'" + str(path) + "'" in pool.commands[2]


@pytest.mark.parametrize(
    ("prefix", "crlf", "force_tty"),
    [(b"Welcome to the login node\n", False, False), (b"", True, True)],
)
def test_ssh_message_reader_ignores_banners_and_tty_line_endings(
    tmp_path, prefix, crlf, force_tty
):
    path = _remote_file(tmp_path)
    path.write_bytes(
        _line({"method": "opened", "params": {}})
        + _line({"method": "closed", "params": {}})
    )
    pool = _LocalShellPool(prefix=prefix, crlf=crlf)
    reader = _reader(
        tmp_path, pool, ssh_config=_resource(tmp_path, force_tty=force_tty)
    )
    handler = _CollectingHandler()
    tracker = message_readers_module._ClosedMessageTracker()

    reader._read_once(handler, tracker)

    assert reader._offset == path.stat().st_size
    assert [m["method"] for m in handler.messages] == ["opened"]
    assert reader.closed_message == {"method": "closed", "params": {}}


def test_ssh_message_reader_reads_a_large_backlog_in_chunks(tmp_path, monkeypatch):
    monkeypatch.setattr(message_readers_module, "READ_CHUNK_BYTES", 64)
    path = _remote_file(tmp_path)
    messages = [
        {"method": "report_custom_message", "params": {"payload": i}} for i in range(20)
    ]
    path.write_bytes(b"".join(_line(m) for m in messages))
    pool = _LocalShellPool()
    reader = _reader(tmp_path, pool, final_read_timeout=0.0)
    handler = _CollectingHandler()

    reader._closed_tracker = message_readers_module._ClosedMessageTracker()
    reader._final_read(handler)

    assert handler.messages == messages
    assert len(pool.commands) > 2


def test_ssh_message_reader_streams_while_the_step_runs_and_reads_the_rest_at_exit(
    tmp_path,
):
    path = _remote_file(tmp_path)
    pool = _LocalShellPool()
    reader = _reader(tmp_path, pool, poll_interval=0.01, max_poll_interval=0.05)
    handler = _CollectingHandler()

    with reader.read_messages(handler) as params:
        assert params["path"] == str(path)
        path.write_bytes(_line({"method": "opened", "params": {}}))
        deadline = time.monotonic() + 5
        while not handler.messages and time.monotonic() < deadline:
            time.sleep(0.01)
        # Streamed before the step ended.
        assert [m["method"] for m in handler.messages] == ["opened"]
        with path.open("ab") as file:
            file.write(
                _line(
                    {
                        "method": "closed",
                        "params": {
                            "exception": {"name": "RuntimeError", "message": "boom"}
                        },
                    }
                )
            )

    assert [m["method"] for m in handler.messages] == ["opened", "closed"]
    assert reader.closed_exception == {"name": "RuntimeError", "message": "boom"}
    assert not reader._reader_thread or not reader._reader_thread.is_alive()


def test_ssh_message_reader_final_read_waits_for_late_closed_message(tmp_path):
    """A shared file system may publish the last bytes on the login node late."""
    path = _remote_file(tmp_path)
    path.write_bytes(_line({"method": "opened", "params": {}}))
    pool = _LocalShellPool()
    reader = _reader(tmp_path, pool, final_read_timeout=5.0)
    reader._closed_tracker = message_readers_module._ClosedMessageTracker()
    handler = _CollectingHandler()

    def publish_late():
        time.sleep(0.3)
        with path.open("ab") as file:
            file.write(_line({"method": "closed", "params": {}}))

    writer = threading.Thread(target=publish_late)
    writer.start()
    reader._final_read(handler)
    writer.join()
    reader._closed_tracker.maybe_flush(handler, force=True)

    assert [m["method"] for m in handler.messages] == ["opened", "closed"]


def test_ssh_message_reader_forwards_unterminated_last_message_at_exit(tmp_path):
    path = _remote_file(tmp_path)
    path.write_bytes(json.dumps({"method": "opened", "params": {}}).encode())
    reader = _reader(tmp_path, _LocalShellPool(), final_read_timeout=0.0)
    reader._closed_tracker = message_readers_module._ClosedMessageTracker()
    handler = _CollectingHandler()

    reader._final_read(handler)

    assert [m["method"] for m in handler.messages] == ["opened"]


def test_failed_reads_back_off_and_keep_polling(tmp_path):
    """A broken link must neither storm SSH nor stop streaming for good."""
    path = _remote_file(tmp_path)
    path.write_bytes(_line({"method": "opened", "params": {}}))
    pool = _LocalShellPool()
    pool.failures = [RuntimeError("SSH command failed (exit 255)")] * 4
    reader = _reader(
        tmp_path,
        pool,
        reconnect_interval=0.01,
        max_reconnect_interval=0.02,
        max_reconnect_attempts=2,
        poll_interval=0.01,
    )
    reader._closed_tracker = message_readers_module._ClosedMessageTracker()
    handler = _CollectingHandler()

    thread = threading.Thread(target=reader._read_loop, args=(handler,))
    thread.start()
    deadline = time.monotonic() + 5
    while not handler.messages and time.monotonic() < deadline:
        time.sleep(0.01)
    reader._stop_flag.set()
    reader._wake.set()
    thread.join(timeout=5)

    assert [m["method"] for m in handler.messages] == ["opened"]
    assert len(pool.commands) >= 5


def test_ssh_message_reader_opens_its_own_pool_without_one(tmp_path, monkeypatch):
    pools: list[Any] = []

    class FakePool(_LocalShellPool):
        def __init__(self, config):
            super().__init__()
            self.config = config
            self.entered = 0
            self.exited = 0
            pools.append(self)

        def __enter__(self):
            self.entered += 1
            return self

        def __exit__(self, *args):
            self.exited += 1

    monkeypatch.setattr(message_readers_module, "SSHConnectionPool", FakePool)
    path = _remote_file(tmp_path)
    path.write_bytes(_line({"method": "opened", "params": {}}))
    reader = SSHMessageReader(
        remote_path=str(path),
        ssh_config=_resource(tmp_path),
        final_read_timeout=0.0,
    )
    handler = _CollectingHandler()

    with reader.read_messages(handler):
        pass

    assert [m["method"] for m in handler.messages] == ["opened"]
    assert [(p.entered, p.exited) for p in pools] == [(1, 1)]
    assert reader._ssh_pool is None


def test_ssh_message_reader_emits_one_bounded_stdio_summary(monkeypatch, tmp_path):
    reader = _reader(tmp_path, _LocalShellPool())
    debug_calls: list[tuple[Any, ...]] = []
    reader.logger = cast(
        Any,
        SimpleNamespace(debug=lambda *args: debug_calls.append(args)),
    )
    monkeypatch.setattr(reader, "_read_loop", lambda _handler: None)
    monkeypatch.setattr(reader, "_final_read", lambda _handler: None)
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
            (
                "SSHMessageReader summary: total_messages=%d; "
                "stdout_messages=%d, stdout_bytes=%d; "
                "stderr_messages=%d, stderr_bytes=%d"
            ),
            5,
            2,
            21,
            1,
            8,
        )
    ]


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


def test_poll_delay_backs_off_while_idle_and_is_capped(tmp_path):
    reader = _reader(
        tmp_path, _LocalShellPool(), poll_interval=1.0, max_poll_interval=5.0
    )

    assert [reader._poll_delay(n) for n in range(4)] == [1.0] * 4
    assert reader._poll_delay(5) == 2.0
    assert reader._poll_delay(6) == 4.0
    assert reader._poll_delay(7) == 5.0
    assert reader._poll_delay(10_000) == 5.0
