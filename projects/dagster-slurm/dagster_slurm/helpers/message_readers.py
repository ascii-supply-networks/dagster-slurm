"""Dagster Pipes message readers for local and SSH execution."""

import json
import os
import shlex
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, Optional

from dagster import PipesMessageReader, get_dagster_logger
from dagster_pipes import PipesDefaultMessageWriter

from .ssh_pool import SSHConnectionPool

#: Bytes of ``messages.jsonl`` fetched per read. A larger backlog is read in
#: several back-to-back rounds, so one read never holds a session for long.
READ_CHUNK_BYTES = 4 * 1024 * 1024

#: Printed before the file content, so output a ``post_login_command`` or a
#: login banner writes to stdout is not taken for messages.
_READ_MARKER = "__DAGSTER_SLURM_PIPES_MESSAGES__"


class _ClosedMessageTracker:
    """Track 'closed' messages to allow draining trailing stdio."""

    def __init__(self, drain_timeout: float = 20.0):
        self._drain_timeout = drain_timeout
        self._pending: dict[str, Any] | None = None
        self._deadline: float | None = None

    def observe(self, message: Any) -> bool:
        if isinstance(message, dict) and message.get("method") == "closed":
            self._pending = message
            self._deadline = time.time() + self._drain_timeout
            return True
        return False

    def maybe_flush(self, handler, *, force: bool = False) -> bool:
        if not self._pending:
            return False
        if not force and self._deadline and time.time() < self._deadline:
            return False

        handler.handle_message(self._pending)
        self._pending = None
        self._deadline = None
        return True


class LocalMessageReader(PipesMessageReader):
    """Tails a local messages file.
    Used for local dev mode.
    """

    def __init__(
        self,
        messages_path: str,
        include_stdio: bool = True,
        poll_interval: float = 0.2,
        creation_timeout: float = 30.0,
        closed_message_drain_timeout: float = 2.0,
    ):
        self.messages_path = messages_path
        self.include_stdio = include_stdio
        self.poll_interval = poll_interval
        self.creation_timeout = creation_timeout
        self.closed_message_drain_timeout = closed_message_drain_timeout
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # This method is moved out of read_messages and becomes a proper class method
    def _tail_file(self, handler):
        """Background thread that tails file."""
        logger = get_dagster_logger()
        pos = 0
        deadline = time.time() + self.creation_timeout

        # Wait for file creation
        while not os.path.exists(self.messages_path):
            if time.time() > deadline:
                logger.warning(f"Messages file not created: {self.messages_path}")
                return
            if self._stop.is_set():
                return
            time.sleep(0.5)

        # Wait for file to be readable
        while True:
            try:
                with open(self.messages_path, "r") as f:
                    break
            except OSError:
                if time.time() > deadline:
                    logger.warning(f"Messages file not readable: {self.messages_path}")
                    return
                time.sleep(0.5)

        # Tail file
        tracker = _ClosedMessageTracker(drain_timeout=self.closed_message_drain_timeout)

        while not self._stop.is_set():
            try:
                with open(self.messages_path, "rb") as f:
                    # Handle file truncation
                    try:
                        size = os.path.getsize(self.messages_path)
                        if pos > size:
                            pos = 0  # File was truncated
                    except OSError:
                        pass

                    if pos > 0:
                        f.seek(pos)

                    while not self._stop.is_set():
                        line = f.readline()
                        if not line:
                            if tracker.maybe_flush(handler):
                                self._stop.set()
                                break
                            break

                        pos = f.tell()
                        decoded_line = line.decode("utf-8", errors="replace").strip()
                        if not decoded_line:
                            continue

                        if self._stop.is_set():
                            break
                        try:
                            msg = json.loads(decoded_line)
                            if tracker.observe(msg):
                                continue
                            handler.handle_message(msg)
                        except json.JSONDecodeError:
                            # Ignore non-JSON lines
                            pass
                        except Exception as e:
                            # Keep reading: one bad message must not end the stream.
                            logger.warning(
                                f"Error handling message: {e}", exc_info=True
                            )

                    pos = f.tell()

            except Exception as e:
                # The reader thread must survive anything and keep polling.
                logger.warning(f"Error reading messages: {e}", exc_info=True)

            if tracker.maybe_flush(handler, force=self._stop.is_set()):
                self._stop.set()
                break

            self._stop.wait(self.poll_interval)

        tracker.maybe_flush(handler, force=True)

    @contextmanager
    def read_messages(self, handler) -> Generator[dict[str, Any], None, None]:
        """Context manager that tails messages file."""
        params = {
            PipesDefaultMessageWriter.FILE_PATH_KEY: self.messages_path,
            PipesDefaultMessageWriter.INCLUDE_STDIO_IN_MESSAGES_KEY: self.include_stdio,
        }

        # Start background thread
        self._stop.clear()
        self._thread = threading.Thread(
            # Target the new method and pass handler as an argument
            target=self._tail_file,
            args=(handler,),
            daemon=True,
            name="local-pipes-reader",
        )
        self._thread.start()

        try:
            yield params
        finally:
            self._stop.set()
            if self._thread:
                self._thread.join(timeout=5)

    def no_messages_debug_text(self) -> str:
        return f"LocalMessageReader: {self.messages_path}"


class SSHMessageReader(PipesMessageReader):
    """Read Pipes messages from a remote file by polling it over SSH.

    Each poll reads ``messages.jsonl`` from the last byte offset in one short
    command on the shared ControlMaster. The command holds one of the master's
    session slots only while it runs (see
    :meth:`SSHConnectionPool.session_slot`), so the reader never keeps an SSH
    session open for the lifetime of a step. Many concurrent steps therefore
    cannot exhaust sshd's ``MaxSessions`` on the shared master.

    The reader polls every ``poll_interval`` seconds and backs off to
    ``max_poll_interval`` while the file does not grow. When the step ends it
    reads once more, so no message written before the end is lost. A partial
    last line is kept until the rest of it arrives.
    """

    def __init__(
        self,
        remote_path: str,
        ssh_config,
        control_path: str | None = None,
        reconnect_interval: float = 2.0,
        max_reconnect_attempts: int = 10,
        ssh_pool: Optional["SSHConnectionPool"] = None,
        max_reconnect_interval: float = 60.0,
        healthy_session_seconds: float = 30.0,
        poll_interval: float = 1.0,
        max_poll_interval: float = 5.0,
        read_timeout: float = 60.0,
        final_read_timeout: float = 2.0,
    ):
        """Args:
        remote_path: Path to messages.jsonl on remote host
        ssh_config: SSHConnectionResource instance
        control_path: ControlMaster socket, shown in diagnostics. The attached
            pool's socket takes precedence.
        reconnect_interval: Seconds to wait after the first failed read.
        max_reconnect_attempts: Consecutive failed reads after which the
            reader logs an error. It keeps polling at the capped interval.
        ssh_pool: Pool whose master the reads use. Without one the reader
            opens its own pool for the duration of ``read_messages``.
        max_reconnect_interval: Upper bound for the backoff after failed reads.
        healthy_session_seconds: Ignored. Kept for backwards compatibility with
            the former ``tail -F`` reader.
        poll_interval: Seconds between reads while messages arrive.
        max_poll_interval: Upper bound for the wait between reads while the
            file does not grow.
        read_timeout: Seconds one read may take before it counts as failed.
        final_read_timeout: Seconds the final read keeps retrying until the
            payload's 'closed' message is visible, for shared file systems
            that publish the last bytes late.

        """
        self.remote_path = remote_path
        self.ssh_config = ssh_config
        self._control_path = control_path
        self.reconnect_interval = reconnect_interval
        self.max_reconnect_attempts = max_reconnect_attempts
        self.max_reconnect_interval = max_reconnect_interval
        self.healthy_session_seconds = healthy_session_seconds
        self.poll_interval = poll_interval
        self.max_poll_interval = max(max_poll_interval, poll_interval)
        self.read_timeout = read_timeout
        self.final_read_timeout = final_read_timeout
        self.logger = get_dagster_logger()
        self._stop_flag = threading.Event()
        self._wake = threading.Event()
        self._read_lock = threading.Lock()
        self._reader_thread: threading.Thread | None = None
        self.total_messages = 0
        self._ssh_pool = ssh_pool
        #: Bytes of the remote file already read, including ``_partial``.
        self._offset = 0
        #: Start of a line whose newline has not been read yet.
        self._partial = b""
        self._forwarded_lines: dict[str, int] = {"stdout": 0, "stderr": 0}
        self._stdio_messages: dict[str, int] = {"stdout": 0, "stderr": 0}
        self._stdio_bytes: dict[str, int] = {"stdout": 0, "stderr": 0}
        self._closed_message: dict[str, Any] | None = None
        self._closed_exception: dict[str, Any] | None = None

    @property
    def control_path(self) -> str | None:
        """Current ControlMaster socket, tracking the pool when one is attached."""
        pool = self._ssh_pool
        if pool is not None:
            return getattr(pool, "control_path", self._control_path)
        return self._control_path

    @contextmanager
    def read_messages(self, handler) -> Generator[dict, None, None]:
        """Poll the remote messages file in the background while the step runs.

        Yields:
            Params dict with message file path for remote process

        """
        self.logger.debug(f"Starting SSH message reader for {self.remote_path}")

        owned_pool: SSHConnectionPool | None = None
        if self._ssh_pool is None:
            owned_pool = SSHConnectionPool(self.ssh_config)
            owned_pool.__enter__()
            self._ssh_pool = owned_pool

        self._closed_tracker = _ClosedMessageTracker()
        self._stop_flag.clear()
        self._wake.clear()
        self._reader_thread = threading.Thread(
            target=self._read_loop,
            args=(handler,),
            daemon=True,
            name="ssh-pipes-reader",
        )
        self._reader_thread.start()

        try:
            # Yield the params that the remote process needs
            yield {
                "path": self.remote_path,
                PipesDefaultMessageWriter.INCLUDE_STDIO_IN_MESSAGES_KEY: True,
            }
        finally:
            self._stop_flag.set()
            self._wake.set()
            if self._reader_thread.is_alive():
                self._reader_thread.join(timeout=5)

            # The step is over: read everything it wrote since the last poll.
            self._final_read(handler)
            self._closed_tracker.maybe_flush(handler, force=True)

            if owned_pool is not None:
                owned_pool.__exit__(None, None, None)
                self._ssh_pool = None

            self.logger.debug(
                "SSHMessageReader summary: total_messages=%d; "
                "stdout_messages=%d, stdout_bytes=%d; "
                "stderr_messages=%d, stderr_bytes=%d",
                self.total_messages,
                self._stdio_messages["stdout"],
                self._stdio_bytes["stdout"],
                self._stdio_messages["stderr"],
                self._stdio_bytes["stderr"],
            )
            self.logger.debug("Message reader stopped")

    @property
    def closed_message(self) -> dict[str, Any] | None:
        return self._closed_message

    @property
    def closed_exception(self) -> dict[str, Any] | None:
        return self._closed_exception

    def _record_protocol_message(self, message: Any) -> None:
        if not isinstance(message, dict) or message.get("method") != "closed":
            return

        self._closed_message = message
        params = message.get("params")
        if isinstance(params, dict):
            exception = params.get("exception")
            if isinstance(exception, dict):
                self._closed_exception = exception

    def _forward_message(
        self,
        message: Any,
        handler: Any,
        tracker: _ClosedMessageTracker,
    ) -> bool:
        """Record and forward one parsed Pipes message.

        Returns whether the message was forwarded immediately. Closed messages
        are held by the tracker so trailing stdio can be drained first.
        """
        self._record_protocol_message(message)
        if tracker.observe(message):
            return False

        if isinstance(message, dict) and message.get("method") == "log_external_stream":
            params = message.get("params", {}) or {}
            stream = params.get("stream")
            text = params.get("text", "")
            if isinstance(stream, str) and stream in self._forwarded_lines:
                text_value = str(text)
                line_count = len(text_value.splitlines())
                if text_value and not text_value.endswith("\n"):
                    line_count = max(line_count, 1)
                if line_count:
                    self._forwarded_lines[stream] += line_count
                self._stdio_messages[stream] += 1
                self._stdio_bytes[stream] += len(text_value.encode("utf-8"))

        handler.handle_message(message)
        return True

    def _reconnect_delay(self, failures: int) -> float:
        """Exponentially back off after failed reads so a broken link is not stormed."""
        if failures <= 0:
            return self.reconnect_interval
        delay = self.reconnect_interval * (2 ** min(failures - 1, 6))
        return min(delay, self.max_reconnect_interval)

    def _poll_delay(self, idle_rounds: int) -> float:
        """Wait before the next read: short while messages arrive, longer when idle."""
        delay = self.poll_interval * (2 ** min(max(0, idle_rounds - 4), 16))
        return min(delay, self.max_poll_interval)

    def _read_command(self) -> str:
        """Shell command printing the file from the current offset, if it exists."""
        quoted = shlex.quote(self.remote_path)
        return (
            f"[ -f {quoted} ] || exit 0; printf '%s\\n' {_READ_MARKER}; "
            f"tail -c +{self._offset + 1} {quoted} | head -c {READ_CHUNK_BYTES}"
        )

    def _payload(self, output: bytes) -> bytes | None:
        """Return the file bytes from a read's output, or None without a file."""
        if self.ssh_config.requires_tty:
            # A pseudo-terminal turns every newline into CRLF. JSON lines never
            # contain a raw carriage return, so this restores the exact bytes.
            output = output.replace(b"\r\n", b"\n")
        marker = _READ_MARKER.encode() + b"\n"
        start = output.find(marker)
        if start < 0:
            return None
        return output[start + len(marker) :]

    def _read_once(self, handler, tracker: _ClosedMessageTracker) -> tuple[int, bool]:
        """Read new bytes once and forward every complete line.

        Returns the number of new bytes and whether more may be waiting.
        """
        with self._read_lock:
            pool = self._ssh_pool
            if pool is None:
                raise RuntimeError("SSHMessageReader has no SSH connection pool")
            data = self._payload(
                pool.run_bytes(self._read_command(), timeout=self.read_timeout)
            )
            if not data:
                return 0, False
            self._offset += len(data)
            lines = (self._partial + data).split(b"\n")
            self._partial = lines.pop()
            for line in lines:
                self._handle_line(line, handler, tracker)
            return len(data), len(data) >= READ_CHUNK_BYTES

    def _handle_line(self, raw: bytes, handler, tracker: _ClosedMessageTracker) -> None:
        line = raw.decode("utf-8", errors="replace").strip()
        if not line:
            return
        try:
            message = json.loads(line)
        except json.JSONDecodeError as je:
            self.logger.warning(f"Malformed JSON message: {line[:100]}... Error: {je}")
            return
        try:
            if self._forward_message(message, handler, tracker):
                self.total_messages += 1
        except Exception as e:
            self.logger.warning(f"Error handling message: {e}", exc_info=True)

    def _read_loop(self, handler) -> None:
        """Poll the messages file until the step ends."""
        tracker = self._closed_tracker
        failures = 0
        idle_rounds = 0
        self.logger.debug(
            "Polling %s for Pipes messages over the shared SSH master",
            self.remote_path,
        )

        while not self._stop_flag.is_set():
            try:
                new_bytes, more = self._read_once(handler, tracker)
            except Exception as exc:  # noqa: BLE001 - logged below
                # The reader thread must survive anything and keep polling.
                if self._stop_flag.is_set():
                    break
                failures += 1
                if failures == self.max_reconnect_attempts:
                    self.logger.error(
                        f"Reading Pipes messages from {self.remote_path} failed "
                        f"{failures} times in a row; retrying every "
                        f"{self._reconnect_delay(failures):.0f}s. Last error: {exc}"
                    )
                elif failures == 1:
                    self.logger.warning(
                        f"Reading Pipes messages from {self.remote_path} failed; "
                        f"retrying. Error: {exc}"
                    )
                else:
                    self.logger.debug(f"Reading Pipes messages failed again: {exc}")
                delay = self._reconnect_delay(failures)
            else:
                if failures:
                    self.logger.info(
                        f"Reading Pipes messages from {self.remote_path} recovered."
                    )
                failures = 0
                idle_rounds = 0 if new_bytes else idle_rounds + 1
                delay = 0.0 if more else self._poll_delay(idle_rounds)

            with self._read_lock:
                if tracker.maybe_flush(handler):
                    self._stop_flag.set()
                    break

            if delay > 0 and self._wake.wait(delay):
                self._wake.clear()

    def _final_read(self, handler) -> None:
        """Read what the step wrote since the last poll.

        Retries briefly until the 'closed' message is visible, because a
        shared file system may show the last bytes on the login node late.
        """
        tracker = self._closed_tracker
        deadline = time.monotonic() + self.final_read_timeout
        while True:
            try:
                while self._read_once(handler, tracker)[1]:
                    pass
            except Exception as exc:  # noqa: BLE001 - logged, must not mask the step
                self.logger.warning(
                    f"Final read of Pipes messages from {self.remote_path} "
                    f"failed: {exc}"
                )
                break
            if self._closed_message is not None or time.monotonic() >= deadline:
                break
            time.sleep(min(0.25, max(0.0, deadline - time.monotonic())))

        with self._read_lock:
            partial, self._partial = self._partial, b""
            if partial.strip():
                # The writer ends every message with a newline; a complete
                # message without one is still worth forwarding.
                self._handle_line(partial, handler, tracker)

    def no_messages_debug_text(self) -> str:
        """Return debug text shown when no messages received."""
        return (
            f"SSHMessageReader: {self.ssh_config.user}@{self.ssh_config.host}:"
            f"{self.remote_path}\n"
            f"ControlPath: {self.control_path or 'not set'}\n"
            f"Auth method: {self._auth_method_label()}\n\n"
            f"Check if the remote process is writing messages to the file:\n"
            f"  ssh {self.ssh_config.user}@{self.ssh_config.host} -p {self.ssh_config.port} "
            f"'cat {self.remote_path}'"
        )

    def _auth_method_label(self) -> str:
        if self.ssh_config.uses_key_auth:
            return "key"
        if self.ssh_config.uses_password_auth:
            return "password"
        return "inherited from ~/.ssh/config"
