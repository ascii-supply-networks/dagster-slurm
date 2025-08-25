import json, shlex, subprocess, threading
from contextlib import contextmanager
from dagster import PipesMessageReader
from dagster_pipes import PipesDefaultMessageWriter
from typing import Iterator

from .ssh_helpers import SSH_HOST, SSH_PORT, SSH_USER, COMMON_SSH_OPTS, SSH_KEY

class SshExecTailMessageReader(PipesMessageReader):
    """Tails a Pipes messages file over SSH (`tail -F` on the remote host)."""
    def __init__(self, remote_messages_path: str, include_stdio_in_messages: bool = True):
        self._remote_path = remote_messages_path
        self._include_stdio = include_stdio_in_messages
        self._proc: subprocess.Popen | None = None
        self._pump_thread: threading.Thread | None = None
        self._stop = threading.Event()

    @contextmanager
    def read_messages(self, handler) -> Iterator[dict]:
        # start `tail -F` remotely
        tail_cmd = f"tail -n +1 -F -- {shlex.quote(self._remote_path)}"
        remote = "bash --noprofile --norc -lc " + shlex.quote(tail_cmd)

        self._proc = subprocess.Popen(
            ["ssh", "-p", str(SSH_PORT), "-i", SSH_KEY, *COMMON_SSH_OPTS,
             f"{SSH_USER}@{SSH_HOST}", remote],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

        def pump():
            assert self._proc and self._proc.stdout
            for line in self._proc.stdout:
                if self._stop.is_set():
                    break
                try:
                    msg = json.loads(line)
                    handler.handle_message(msg)
                except Exception:
                    # ignore non-Pipes lines
                    pass

        self._pump_thread = threading.Thread(target=pump, daemon=True)
        self._pump_thread.start()

        try:
            # tell Dagster to configure default File writer at the *remote* path
            yield {
                PipesDefaultMessageWriter.FILE_PATH_KEY: self._remote_path,
                PipesDefaultMessageWriter.INCLUDE_STDIO_IN_MESSAGES_KEY: self._include_stdio,
            }
        finally:
            self._stop.set()
            if self._proc:
                try:
                    self._proc.terminate()
                    self._proc.wait(timeout=5)
                except Exception:
                    try:
                        self._proc.kill()
                    except Exception:
                        pass
            if self._pump_thread:
                self._pump_thread.join(timeout=2)

    def no_messages_debug_text(self) -> str:
        return f"SshExecTailMessageReader tails {SSH_USER}@{SSH_HOST}:{self._remote_path}"