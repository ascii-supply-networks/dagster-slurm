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
    Tails a remote messages file via SSH.
    Used for Slurm modes - starts 'tail -F' over SSH.
    """

    def __init__(
        self,
        messages_path: str,
        ssh_resource: "SSHConnectionResource",
        include_stdio: bool = True,
    ):
        self.messages_path = messages_path
        self.ssh_resource = ssh_resource
        self.include_stdio = include_stdio
        self._proc: Optional[subprocess.Popen] = None
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    @contextmanager
    def read_messages(self, handler) -> Iterator[Dict[str, Any]]:
        """Context manager that tails remote file via SSH."""

        params = {
            PipesDefaultMessageWriter.FILE_PATH_KEY: self.messages_path,
            PipesDefaultMessageWriter.INCLUDE_STDIO_IN_MESSAGES_KEY: self.include_stdio,
        }

        # Start SSH tail process
        tail_cmd = (
            f"tail -n +1 -F {shlex.quote(self.messages_path)} 2>/dev/null || true"
        )
        remote_cmd = f"bash --noprofile --norc -c {shlex.quote(tail_cmd)}"

        ssh_cmd = [
            "ssh",
            "-p",
            str(self.ssh_resource.port),
            "-i",
            self.ssh_resource.key_path,
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "BatchMode=yes",
            "-o",
            "LogLevel=ERROR",
            f"{self.ssh_resource.user}@{self.ssh_resource.host}",
            remote_cmd,
        ]

        self._proc = subprocess.Popen(
            ssh_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

        def pump_messages():
            """Read from SSH stdout and call handler."""
            logger = get_dagster_logger()

            if not self._proc or not self._proc.stdout:
                return

            for line in self._proc.stdout:
                if self._stop.is_set():
                    break

                line = line.strip()
                if not line:
                    continue

                try:
                    msg = json.loads(line)
                    handler.handle_message(msg)
                except json.JSONDecodeError:
                    # Ignore non-JSON lines (tail startup messages)
                    pass
                except Exception as e:
                    logger.warning(f"Error handling message: {e}")

        # Start pump thread
        self._stop.clear()
        self._thread = threading.Thread(
            target=pump_messages, daemon=True, name="ssh-pipes-reader"
        )
        self._thread.start()

        try:
            yield params
        finally:
            self._stop.set()

            # Clean up SSH process
            if self._proc:
                try:
                    self._proc.terminate()
                    self._proc.wait(timeout=5)
                except Exception:
                    try:
                        self._proc.kill()
                    except Exception:
                        pass

            # Wait for thread
            if self._thread:
                self._thread.join(timeout=2)

    def no_messages_debug_text(self) -> str:
        return (
            f"SSHMessageReader: "
            f"{self.ssh_resource.user}@{self.ssh_resource.host}:{self.messages_path}"
        )
