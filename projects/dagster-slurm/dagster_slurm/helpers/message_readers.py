"""Dagster Pipes message readers for local and SSH execution."""

import json
import os
import subprocess
import threading
import time
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional

from dagster import PipesMessageReader, get_dagster_logger
from dagster_pipes import PipesDefaultMessageWriter


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
        while not self._stop.is_set():
            try:
                with open(self.messages_path, "r", encoding="utf-8") as f:
                    # Handle file truncation
                    try:
                        size = os.path.getsize(self.messages_path)
                        if pos > size:
                            pos = 0  # File was truncated
                    except Exception:
                        pass

                    if pos > 0:
                        f.seek(pos)

                    for line in f:
                        if self._stop.is_set():
                            break

                        line = line.strip()
                        if not line:
                            continue

                        try:
                            msg = json.loads(line)
                            handler.handle_message(msg)
                        except json.JSONDecodeError:
                            # Ignore non-JSON lines
                            pass
                        except Exception as e:
                            logger.warning(f"Error handling message: {e}")

                    pos = f.tell()

            except Exception as e:
                logger.warning(f"Error reading messages: {e}")

            self._stop.wait(self.poll_interval)

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
        self._stop_flag = threading.Event()
        self._reader_thread: Optional[threading.Thread] = None

    @contextmanager
    def read_messages(self, handler) -> Iterator[dict]:
        """Context manager that tails remote messages file with auto-reconnect.

        Yields:
            Params dict with message file path for remote process

        """
        self.logger.debug(f"Starting SSH message reader for {self.remote_path}")

        # Start reader thread with auto-reconnect
        self._reader_thread = threading.Thread(
            target=self._read_loop_with_reconnect,
            args=(handler,),
            daemon=True,
        )
        self._reader_thread.start()

        try:
            # Yield the params that the remote process needs
            yield {"path": self.remote_path}

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

            # Wait for reader thread to finish
            if self._reader_thread and self._reader_thread.is_alive():
                self._reader_thread.join(timeout=5)

            self.logger.debug("Message reader stopped")

    def _read_loop_with_reconnect(self, handler):
        """Read loop that automatically reconnects on failure.

        Args:
            handler: Dagster message handler

        """
        reconnect_count = 0
        total_message_count = 0

        while not self._stop_flag.is_set():
            try:
                # Start tail process
                ssh_cmd = self._build_ssh_tail_command()

                if not all(ssh_cmd):
                    self.logger.error(f"SSH command contains None values: {ssh_cmd}")
                    return

                self.logger.debug(
                    f"Starting tail (attempt {reconnect_count + 1}): "
                    f"{' '.join(str(x) for x in ssh_cmd)}"
                )

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

                # Read messages from tail
                for line in self._proc.stdout:  # type: ignore
                    if self._stop_flag.is_set():
                        break

                    line = line.strip()
                    if not line:
                        continue

                    try:
                        # Parse JSON message
                        message = json.loads(line)

                        # Pass parsed dict to handler
                        handler.handle_message(message)
                        message_count += 1
                        total_message_count += 1

                        # self.logger.debug(
                        #     f"Received message {total_message_count}: "
                        #     f"{message.get('method', 'unknown')}"
                        # )

                    except json.JSONDecodeError as je:
                        # Log malformed JSON but continue
                        self.logger.warning(
                            f"Malformed JSON message: {line[:100]}... Error: {je}"
                        )
                    except Exception as e:
                        self.logger.warning(
                            f"Error handling message: {e}", exc_info=True
                        )

                # Process exited
                return_code = self._proc.wait()

                if self._stop_flag.is_set():
                    # Normal shutdown
                    # self.logger.debug(
                    #     f"Tail process stopped normally "
                    #     f"(read {message_count} messages this session, "
                    #     f"{total_message_count} total)"
                    # )
                    break

                # Unexpected exit - try to reconnect
                stderr = self._proc.stderr.read() if self._proc.stderr else ""

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

        # self.logger.info(
        #     f"Message reader finished. Total messages received: {total_message_count}"
        # )

    def _build_ssh_tail_command(self) -> list[str]:
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
            # Password auth without ControlMaster won't work
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
