"""Dagster Pipes message readers for local and SSH execution."""

import json
import os
import shlex
import subprocess
import threading
import time
from contextlib import contextmanager
from typing import Iterator, Optional, Dict, Any
from pathlib import Path
from dagster import PipesMessageReader, get_dagster_logger
from dagster_pipes import PipesDefaultMessageWriter
from ..resources.ssh import SSHConnectionResource
from .ssh_pool import SSHConnectionPool


class LocalMessageReader(PipesMessageReader):
    """
    Tails a local messages file.
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

    @contextmanager
    def read_messages(self, handler) -> Iterator[Dict[str, Any]]:
        """Context manager that tails messages file."""

        params = {
            PipesDefaultMessageWriter.FILE_PATH_KEY: self.messages_path,
            PipesDefaultMessageWriter.INCLUDE_STDIO_IN_MESSAGES_KEY: self.include_stdio,
        }

        def tail_file():
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
                        logger.warning(
                            f"Messages file not readable: {self.messages_path}"
                        )
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

        # Start background thread
        self._stop.clear()
        self._thread = threading.Thread(
            target=tail_file, daemon=True, name="local-pipes-reader"
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
    """
    Read Pipes messages from remote file via SSH tail with auto-reconnect.

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
        """
        Args:
            remote_path: Path to messages.jsonl on remote host
            ssh_config: SSHConnectionResource instance
            control_path: Path to ControlMaster socket (required for password auth)
            reconnect_interval: Seconds to wait before reconnecting
            max_reconnect_attempts: Maximum reconnection attempts
        """
        self.remote_path = remote_path
        self.ssh_config = ssh_config
        self.control_path = control_path
        self.reconnect_interval = reconnect_interval
        self.max_reconnect_attempts = max_reconnect_attempts
        self.logger = get_dagster_logger()
        self._proc = None
        self._stop_flag = threading.Event()
        self._reader_thread = None

    @contextmanager
    def read_messages(self, handler) -> Iterator[dict]:
        """
        Context manager that tails remote messages file with auto-reconnect.

        Yields:
            Empty dict (no additional params needed)
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
            # Yield control back to Dagster
            yield {}

            # Wait a bit for final messages
            time.sleep(1.0)

        finally:
            # Signal stop and cleanup
            self._stop_flag.set()

            if self._proc:
                try:
                    self._proc.terminate()
                    self._proc.wait(timeout=5)
                except Exception as e:
                    self.logger.debug(f"Error terminating tail process: {e}")
                    try:
                        self._proc.kill()
                    except:
                        pass

            # Wait for reader thread to finish
            if self._reader_thread and self._reader_thread.is_alive():
                self._reader_thread.join(timeout=5)

    def _read_loop_with_reconnect(self, handler):
        """
        Read loop that automatically reconnects on failure.

        Args:
            handler: Dagster message handler
        """
        reconnect_count = 0
        last_message_count = 0

        while not self._stop_flag.is_set():
            try:
                # Start tail process
                ssh_cmd = self._build_ssh_tail_command()

                if not all(ssh_cmd):
                    self.logger.error(f"SSH command contains None values: {ssh_cmd}")
                    return

                self.logger.debug(
                    f"Starting tail (attempt {reconnect_count + 1}): {' '.join(str(x) for x in ssh_cmd)}"
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
                for line in self._proc.stdout:
                    if self._stop_flag.is_set():
                        break

                    line = line.strip()
                    if line:
                        try:
                            handler.handle_message(line)
                            message_count += 1
                        except Exception as e:
                            self.logger.warning(f"Error handling message: {e}")

                # Process exited
                return_code = self._proc.wait()

                if self._stop_flag.is_set():
                    # Normal shutdown
                    self.logger.debug(
                        f"Tail process stopped normally (read {message_count} messages)"
                    )
                    break

                # Unexpected exit - try to reconnect
                stderr = self._proc.stderr.read() if self._proc.stderr else ""
                self.logger.warning(
                    f"Tail process exited unexpectedly (code {return_code}). "
                    f"Read {message_count} messages. stderr: {stderr}"
                )

                # Check if we're making progress (receiving messages)
                if message_count > last_message_count:
                    # Reset counter if we received new messages
                    reconnect_count = 0
                    last_message_count = message_count
                else:
                    reconnect_count += 1

                if reconnect_count >= self.max_reconnect_attempts:
                    self.logger.error(
                        f"Max reconnect attempts ({self.max_reconnect_attempts}) reached. "
                        "Giving up on message reading."
                    )
                    break

                # Wait before reconnecting
                self.logger.info(f"Reconnecting in {self.reconnect_interval}s...")
                self._stop_flag.wait(self.reconnect_interval)

            except Exception as e:
                if self._stop_flag.is_set():
                    break

                self.logger.error(f"Error in tail process: {e}")
                reconnect_count += 1

                if reconnect_count >= self.max_reconnect_attempts:
                    self.logger.error("Max reconnect attempts reached. Giving up.")
                    break

                self._stop_flag.wait(self.reconnect_interval)

    def _build_ssh_tail_command(self) -> list[str]:
        """
        Build SSH command to tail the remote messages file.

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
            "ServerAliveInterval=15",  # Keep connection alive
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
        tail_cmd = f"tail -F --retry -n +1 {self.remote_path} 2>/dev/null || tail -f {self.remote_path}"
        base_cmd.append(tail_cmd)

        return base_cmd

    def no_messages_debug_text(self) -> str:
        """
        Return debug text shown when no messages received.
        """
        return (
            f"SSHMessageReader: {self.ssh_config.user}@{self.ssh_config.host}:"
            f"{self.remote_path}\n"
            f"ControlPath: {self.control_path or 'not set'}\n"
            f"Auth method: {'key' if self.ssh_config.uses_key_auth else 'password'}"
        )
