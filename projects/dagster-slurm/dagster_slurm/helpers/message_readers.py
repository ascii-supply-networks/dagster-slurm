"""Dagster Pipes message readers for local and SSH execution."""

import json
import os
import subprocess
import threading
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, Iterator, Optional, cast

import shlex

from dagster import PipesMessageReader, get_dagster_logger
from dagster_pipes import PipesDefaultMessageWriter

if TYPE_CHECKING:  # pragma: no cover
    from .ssh_pool import SSHConnectionPool


class _ClosedMessageTracker:
    """Track 'closed' messages to allow draining trailing stdio."""

    def __init__(self, drain_timeout: float = 20.0):
        self._drain_timeout = drain_timeout
        self._pending: Optional[Dict[str, Any]] = None
        self._deadline: Optional[float] = None

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
    ):
        self.messages_path = messages_path
        self.include_stdio = include_stdio
        self.poll_interval = poll_interval
        self.creation_timeout = creation_timeout
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

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
            except IOError:
                if time.time() > deadline:
                    logger.warning(f"Messages file not readable: {self.messages_path}")
                    return
                time.sleep(0.5)

        # Tail file
        tracker = _ClosedMessageTracker()

        while not self._stop.is_set():
            try:
                with open(self.messages_path, "rb") as f:
                    # Handle file truncation
                    try:
                        size = os.path.getsize(self.messages_path)
                        if pos > size:
                            pos = 0  # File was truncated
                    except Exception:
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
                            logger.warning(f"Error handling message: {e}")

                    pos = f.tell()

            except Exception as e:
                logger.warning(f"Error reading messages: {e}")

            if tracker.maybe_flush(handler, force=self._stop.is_set()):
                self._stop.set()
                break

            self._stop.wait(self.poll_interval)

        tracker.maybe_flush(handler, force=True)

    @contextmanager
    def read_messages(self, handler) -> Iterator[Dict[str, Any]]:
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
    """Read Pipes messages from remote file via SSH tail with auto-reconnect.

    Uses SSH ControlMaster connection for efficient tailing.
    Automatically reconnects if the tail process dies.
    """

    def __init__(
        self,
        remote_path: str,
        ssh_config,
        control_path: Optional[str] = None,
        reconnect_interval: float = 2.0,
        max_reconnect_attempts: int = 10,
        ssh_pool: Optional["SSHConnectionPool"] = None,
    ):
        """Args:
        remote_path: Path to messages.jsonl on remote host
        ssh_config: SSHConnectionResource instance
        control_path: Path to ControlMaster socket (required for password auth)
        reconnect_interval: Seconds to wait before reconnecting
        max_reconnect_attempts: Maximum reconnection attempts.

        """
        self.remote_path = remote_path
        self.ssh_config = ssh_config
        self.control_path = control_path
        self.reconnect_interval = reconnect_interval
        self.max_reconnect_attempts = max_reconnect_attempts
        self.logger = get_dagster_logger()
        self._proc: Optional[subprocess.Popen[str]] = None
        self._pexpect_child: Optional[Any] = None
        self._stop_flag = threading.Event()
        self._reader_thread: Optional[threading.Thread] = None
        self.total_messages = 0
        self._ssh_pool = ssh_pool
        self._fallback_next_line = 1
        self._forwarded_lines: Dict[str, int] = {"stdout": 0, "stderr": 0}

    @contextmanager
    def read_messages(self, handler) -> Iterator[dict]:
        """Context manager that tails remote messages file with auto-reconnect.

        Yields:
            Params dict with message file path for remote process

        """
        self.logger.debug(f"Starting SSH message reader for {self.remote_path}")

        # Start reader thread with auto-reconnect
        self._closed_tracker = _ClosedMessageTracker()
        self._reader_thread = threading.Thread(
            target=self._read_loop_with_reconnect,
            args=(handler,),
            daemon=True,
        )
        self._reader_thread.start()

        try:
            # Yield the params that the remote process needs
            yield {
                "path": self.remote_path,
                PipesDefaultMessageWriter.INCLUDE_STDIO_IN_MESSAGES_KEY: True,
            }

            # Wait for messages - keep reader alive while job runs
            # The reader will stop when we set the stop flag
            time.sleep(1.0)

        finally:
            # Signal stop
            # self.logger.debug("Stopping message reader...")
            self._stop_flag.set()

            # Give time for final messages to be flushed
            time.sleep(1.0)

            # Terminate tail process
            if self._proc:
                try:
                    self._proc.terminate()
                    self._proc.wait(timeout=5)
                except Exception as e:
                    self.logger.debug(f"Error terminating tail process: {e}")
                    try:
                        self._proc.kill()
                    except:  # noqa: E722
                        pass
            if self._pexpect_child is not None:
                try:
                    self._pexpect_child.close(force=True)
                except Exception as e:  # pragma: no cover - best effort cleanup
                    self.logger.debug(f"Error closing tail session: {e}")
                finally:
                    self._pexpect_child = None

            # Wait for reader thread to finish
            if self._reader_thread and self._reader_thread.is_alive():
                self._reader_thread.join(timeout=5)

            if hasattr(self, "_closed_tracker"):
                self._closed_tracker.maybe_flush(handler, force=True)

            self.logger.debug("Message reader stopped")

    def _read_loop_with_reconnect(self, handler):
        """Read loop that automatically reconnects on failure.

        Args:
            handler: Dagster message handler

        """
        reconnect_count = 0
        total_message_count = 0
        return_code: int = 0
        tracker = getattr(self, "_closed_tracker", _ClosedMessageTracker())

        while not self._stop_flag.is_set():
            try:
                # Start tail process
                ssh_cmd_optional = self._build_ssh_tail_command()

                if ssh_cmd_optional is None:
                    handled, reconnect_count, total_message_count, stop_loop = (
                        self._poll_messages_with_pool(
                            handler,
                            reconnect_count,
                            total_message_count,
                            tracker,
                        )
                    )
                    if stop_loop:
                        break
                    if handled:
                        continue

                assert ssh_cmd_optional is not None
                ssh_cmd: list[str] = ssh_cmd_optional

                self.logger.debug(
                    f"Starting tail (attempt {reconnect_count + 1}): "
                    f"{' '.join(str(x) for x in ssh_cmd)}"
                )

                self._proc = None
                if self._requires_password_auth() and not self.control_path:
                    child = self._spawn_tail_with_password(ssh_cmd)
                    self._pexpect_child = child
                    reconnect_count = 0
                    message_count = 0

                    for line in self._iter_pexpect_lines(child):
                        if self._stop_flag.is_set():
                            break

                        line = line.strip()
                        if not line:
                            continue

                        try:
                            message = json.loads(line)
                            if tracker.observe(message):
                                continue
                            if (
                                isinstance(message, dict)
                                and message.get("method") == "log_external_stream"
                            ):
                                params = message.get("params", {}) or {}
                                stream = params.get("stream")
                                text = params.get("text", "")
                                if (
                                    isinstance(stream, str)
                                    and stream in self._forwarded_lines
                                ):
                                    line_count = len(str(text).splitlines())
                                    if text and not text.endswith("\n"):
                                        line_count = max(line_count, 1)
                                    if line_count:
                                        self._forwarded_lines[stream] += line_count
                                self.logger.debug(
                                    "SSHMessageReader stdio chunk: %s",
                                    message.get("params", {}).get("text", "")[:200],
                                )
                            handler.handle_message(message)
                            message_count += 1
                            total_message_count += 1
                            self.total_messages = total_message_count
                        except json.JSONDecodeError as je:
                            self.logger.warning(
                                f"Malformed JSON message: {line[:100]}... Error: {je}"
                            )
                        except Exception as e:
                            self.logger.warning(
                                f"Error handling message: {e}", exc_info=True
                            )

                    if child.isalive():
                        child.close(force=True)

                    self._pexpect_child = None
                    return_code = (
                        child.exitstatus if child.exitstatus is not None else 0
                    )
                    stderr = ""
                else:
                    self._proc = subprocess.Popen(
                        ssh_cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        bufsize=1,
                    )

                # Reset reconnect counter on successful start
                reconnect_count = 0
                message_count = 0

                if self._proc:
                    for line in self._proc.stdout:  # type: ignore
                        if self._stop_flag.is_set():
                            break

                        line = line.strip()
                        if not line:
                            continue

                        try:
                            message = json.loads(line)
                            if tracker.observe(message):
                                continue
                            if (
                                isinstance(message, dict)
                                and message.get("method") == "log_external_stream"
                            ):
                                params = message.get("params", {}) or {}
                                stream = params.get("stream")
                                text = params.get("text", "")
                                if (
                                    isinstance(stream, str)
                                    and stream in self._forwarded_lines
                                ):
                                    line_count = len(str(text).splitlines())
                                    if text and not text.endswith("\n"):
                                        line_count = max(line_count, 1)
                                    if line_count:
                                        self._forwarded_lines[stream] += line_count
                                self.logger.debug(
                                    "SSHMessageReader stdio chunk: %s",
                                    message.get("params", {}).get("text", "")[:200],
                                )
                            handler.handle_message(message)
                            message_count += 1
                            total_message_count += 1
                            self.total_messages = total_message_count
                        except json.JSONDecodeError as je:
                            self.logger.warning(
                                f"Malformed JSON message: {line[:100]}... Error: {je}"
                            )
                        except Exception as e:
                            self.logger.warning(
                                f"Error handling message: {e}", exc_info=True
                            )

                    return_code = self._proc.wait()
                    stderr = self._proc.stderr.read() if self._proc.stderr else ""
                else:
                    stderr = ""

                if self._stop_flag.is_set():
                    # Normal shutdown
                    # self.logger.debug(
                    #     f"Tail process stopped normally "
                    #     f"(read {message_count} messages this session, "
                    #     f"{total_message_count} total)"
                    # )
                    break

                # Unexpected exit - try to reconnect
                if message_count > 0:
                    # We received messages, so connection was working
                    self.logger.info(
                        f"Tail process exited (code {return_code}). "
                        f"Read {message_count} messages. Reconnecting..."
                    )
                    reconnect_count = 0  # Reset since we made progress
                else:
                    # No messages received
                    self.logger.warning(
                        f"Tail process exited unexpectedly (code {return_code}). "
                        f"No messages received. stderr: {stderr}"
                    )
                    reconnect_count += 1

                if reconnect_count >= self.max_reconnect_attempts:
                    self.logger.error(
                        f"Max reconnect attempts ({self.max_reconnect_attempts}) reached. "
                        f"Total messages received: {total_message_count}"
                    )
                    break

                # Wait before reconnecting
                if not self._stop_flag.is_set():
                    self.logger.debug(f"Reconnecting in {self.reconnect_interval}s...")
                    self._stop_flag.wait(self.reconnect_interval)

                if tracker.maybe_flush(handler):
                    self._stop_flag.set()
                    break

            except Exception as e:
                if self._stop_flag.is_set():
                    break

                self.logger.error(f"Error in tail process: {e}", exc_info=True)
                reconnect_count += 1

                if reconnect_count >= self.max_reconnect_attempts:
                    self.logger.error(
                        f"Max reconnect attempts reached. "
                        f"Total messages received: {total_message_count}"
                    )
                    break

                if not self._stop_flag.is_set():
                    self._stop_flag.wait(self.reconnect_interval)

                if tracker.maybe_flush(handler):
                    self._stop_flag.set()
                    break

        # self.logger.info(
        #     f"Message reader finished. Total messages received: {total_message_count}"
        # )

    def _poll_messages_with_pool(
        self,
        handler,
        reconnect_count: int,
        total_message_count: int,
        tracker: _ClosedMessageTracker,
    ) -> tuple[bool, int, int, bool]:
        """Poll messages.jsonl via SSH pool when ControlMaster is unavailable."""
        if not self._ssh_pool:
            self.logger.error(
                "SSHMessageReader fallback requires an SSHConnectionPool "
                "when ControlMaster is unavailable."
            )
            self._stop_flag.wait(self.reconnect_interval)
            return True, reconnect_count, total_message_count, self._stop_flag.is_set()

        quoted_path = shlex.quote(self.remote_path)
        next_line = self._fallback_next_line
        message_count = 0

        self.logger.debug(
            "Polling %s for Pipes messages via SSH pool fallback",
            self.remote_path,
        )

        while not self._stop_flag.is_set():
            try:
                output = self._ssh_pool.run(
                    f"tail -n +{next_line} {quoted_path} 2>/dev/null || true"
                )
            except Exception as exc:
                if not self._stop_flag.is_set():
                    self.logger.debug("Polling messages file failed: %s", exc)
                break

            if output:
                lines = [line.strip() for line in output.splitlines()]
                lines = [line for line in lines if line]
                if lines:
                    processed_lines = 0
                    for line in lines:
                        processed_lines += 1
                        try:
                            message = json.loads(line)
                            if tracker.observe(message):
                                continue
                            if (
                                isinstance(message, dict)
                                and message.get("method") == "log_external_stream"
                            ):
                                params = message.get("params", {}) or {}
                                stream = params.get("stream")
                                text = params.get("text", "")
                                if (
                                    isinstance(stream, str)
                                    and stream in self._forwarded_lines
                                ):
                                    line_count = len(str(text).splitlines())
                                    if text and not text.endswith("\n"):
                                        line_count = max(line_count, 1)
                                    if line_count:
                                        self._forwarded_lines[stream] += line_count
                                self.logger.debug(
                                    "SSHMessageReader stdio chunk (fallback): %s",
                                    message.get("params", {}).get("text", "")[:200],
                                )
                            handler.handle_message(message)
                            message_count += 1
                            total_message_count += 1
                            self.total_messages = total_message_count
                        except json.JSONDecodeError as je:
                            self.logger.warning(
                                f"Malformed JSON message: {line[:100]}... Error: {je}"
                            )
                        except Exception as e:
                            self.logger.warning(
                                f"Error handling message: {e}", exc_info=True
                            )
                    next_line += processed_lines

            self._stop_flag.wait(self.reconnect_interval)

            if tracker.maybe_flush(handler):
                self._stop_flag.set()
                break

        self._fallback_next_line = next_line

        if tracker.maybe_flush(handler):
            self._stop_flag.set()
            return True, reconnect_count, total_message_count, True

        if message_count == 0:
            reconnect_count += 1
            if reconnect_count >= self.max_reconnect_attempts:
                self.logger.error(
                    f"Max reconnect attempts ({self.max_reconnect_attempts}) reached. "
                    f"Total messages received: {total_message_count}"
                )
                return True, reconnect_count, total_message_count, True
        else:
            reconnect_count = 0

        if self._stop_flag.is_set():
            return True, reconnect_count, total_message_count, True

        return True, reconnect_count, total_message_count, False

    def _build_ssh_tail_command(self) -> Optional[list[str]]:
        """Build SSH command to tail the remote messages file.

        Returns:
            List of command arguments for subprocess.Popen

        """
        base_cmd = [
            "ssh",
            "-p",
            str(self.ssh_config.port),
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "LogLevel=ERROR",
            "-o",
            "ServerAliveInterval=15",
            "-o",
            "ServerAliveCountMax=3",
        ]

        # Use ControlMaster if available (required for password auth)
        if self.control_path:
            base_cmd.extend(
                [
                    "-o",
                    f"ControlPath={self.control_path}",
                    "-o",
                    "ControlMaster=no",
                ]
            )
            self.logger.debug(f"Using ControlMaster: {self.control_path}")
        elif self.ssh_config.uses_key_auth:
            # Key auth - add key
            base_cmd.extend(
                [
                    "-i",
                    self.ssh_config.key_path,
                    "-o",
                    "IdentitiesOnly=yes",
                    "-o",
                    "BatchMode=yes",
                ]
            )
        else:
            if self._requires_password_auth():
                if self._ssh_pool:
                    return None
                base_cmd.extend(
                    [
                        "-o",
                        "PreferredAuthentications=password,keyboard-interactive",
                        "-o",
                        "NumberOfPasswordPrompts=3",
                    ]
                )
            elif self.ssh_config.uses_key_auth:
                key_path_opt = self.ssh_config.key_path
                if not key_path_opt:
                    raise RuntimeError(
                        "SSH key authentication requires key_path to be set"
                    )
                key_path = cast(str, key_path_opt)
                base_cmd.extend(
                    [
                        "-i",
                        key_path,
                        "-o",
                        "IdentitiesOnly=yes",
                        "-o",
                        "BatchMode=yes",
                    ]
                )
            else:
                raise RuntimeError(
                    "Password authentication requires ControlMaster. "
                    "Pass control_path to SSHMessageReader constructor."
                )

        # Add extra options
        base_cmd.extend(self.ssh_config.extra_opts)

        # Add target
        base_cmd.append(f"{self.ssh_config.user}@{self.ssh_config.host}")

        # Tail command with retry logic
        # -F: follow by name (handles log rotation)
        # --retry: keep trying if file doesn't exist yet
        # -n +1: start from beginning of file
        tail_cmd = f"tail -F --retry -n +1 {self.remote_path} 2>/dev/null || tail -f {self.remote_path}"
        base_cmd.append(tail_cmd)

        return base_cmd

    def no_messages_debug_text(self) -> str:
        """Return debug text shown when no messages received."""
        return (
            f"SSHMessageReader: {self.ssh_config.user}@{self.ssh_config.host}:"
            f"{self.remote_path}\n"
            f"ControlPath: {self.control_path or 'not set'}\n"
            f"Auth method: {'key' if self.ssh_config.uses_key_auth else 'password'}\n\n"
            f"Check if the remote process is writing messages to the file:\n"
            f"  ssh {self.ssh_config.user}@{self.ssh_config.host} -p {self.ssh_config.port} "
            f"'cat {self.remote_path}'"
        )

    def _requires_password_auth(self) -> bool:
        if self.ssh_config.uses_password_auth:
            return True
        if self.ssh_config.jump_host and self.ssh_config.jump_host.uses_password_auth:
            return True
        return False

    def _collect_passwords(self) -> list[str]:
        passwords: list[str] = []
        if self.ssh_config.jump_host and self.ssh_config.jump_host.password:
            passwords.append(self.ssh_config.jump_host.password)
        if self.ssh_config.password:
            passwords.append(self.ssh_config.password)
        return passwords

    def _spawn_tail_with_password(self, ssh_cmd: list[str]):
        try:
            import pexpect  # type: ignore
        except ImportError as exc:  # pragma: no cover - dependency guard
            raise RuntimeError(
                "Password-based log tailing requires the 'pexpect' package. "
                "Install it with 'pip install pexpect'."
            ) from exc

        cmd_str = " ".join(shlex.quote(part) for part in ssh_cmd)
        self.logger.debug(f"Spawning password-based tail: {cmd_str}")
        child = pexpect.spawn(cmd_str, timeout=120, encoding="utf-8")
        child.delaybeforesend = 0
        passwords = self._collect_passwords()
        if not passwords:
            child.close(force=True)
            raise RuntimeError(
                "Password authentication requested but no password provided."
            )

        pw_index = 0
        fallback_password = passwords[-1]
        fallback_attempts = 0
        prompt_timeout = 120

        while True:
            index = child.expect(
                [
                    r"(?i)password:",
                    r"(?i)passphrase",
                    r"(?i)verification code",
                    r"(?i)otp",
                    pexpect.EOF,
                    pexpect.TIMEOUT,
                ],
                timeout=prompt_timeout,
            )

            if index in (0, 1):
                if pw_index < len(passwords):
                    child.sendline(passwords[pw_index])
                    pw_index += 1
                    continue
                if fallback_password and fallback_attempts < 3:
                    child.sendline(fallback_password)
                    fallback_attempts += 1
                    continue
                child.close(force=True)
                raise RuntimeError("Authentication failed: no password remaining")
            if index in (2, 3):  # OTP or verification code
                # Prompt user interactively via /dev/tty if possible.
                try:
                    with (
                        open("/dev/tty", "w", encoding="utf-8", buffering=1) as tty_out,
                        open("/dev/tty", "r", encoding="utf-8", buffering=1) as tty_in,
                    ):
                        tty_out.write(
                            f"Enter verification code for {self.ssh_config.host}: "
                        )
                        tty_out.flush()
                        code = tty_in.readline().strip()
                except OSError:
                    child.close(force=True)
                    raise RuntimeError(
                        "OTP required but no interactive TTY is available."
                    )
                if not code:
                    child.close(force=True)
                    raise RuntimeError("Empty OTP provided; aborting tail session")
                child.sendline(code)
                continue
            if index == 4:  # EOF
                break
            if index == 5:  # TIMEOUT -> assume command started
                break

        child.timeout = 5
        return child

    def _iter_pexpect_lines(self, child):
        import pexpect  # type: ignore

        while not self._stop_flag.is_set():
            try:
                chunk = child.readline()
                if not chunk:
                    continue
                yield chunk
            except pexpect.TIMEOUT:
                continue
            except pexpect.EOF:
                break
