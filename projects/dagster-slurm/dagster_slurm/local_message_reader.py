# local_message_reader.py
import json
import os
import threading
import time
from contextlib import contextmanager
from typing import Dict, Mapping, Any, Optional

from dagster import PipesMessageReader

class LocalExecTailMessageReader(PipesMessageReader):
    """
    Tails a local messages.jsonl file and forwards each JSON line to the Dagster
    Pipes message handler. MUST yield immediately so the caller can start the child.
    """

    def __init__(
        self,
        messages_path: str,
        include_stdio_in_messages: bool = True,
        poll_interval: float = 0.2,
        creation_timeout: float = 30.0,
    ) -> None:
        self._messages_path = messages_path
        self._include_stdio = include_stdio_in_messages
        self._poll_interval = poll_interval
        self._creation_timeout = creation_timeout

        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def no_messages_debug_text(self) -> str:
        return (
            f"LocalExecTailMessageReader: messages file '{self._messages_path}' "
            f"not created yet"
        )

    @contextmanager
    def read_messages(self, message_handler):
        # Provide env to the child right away.
        params: Dict[str, Dict[str, str]] = {
            "env": {
                "DAGSTER_PIPES_MESSAGES": self._messages_path,
                "DAGSTER_PIPES_INCLUDE_STDIO": "1" if self._include_stdio else "0",
            }
        }

        # Start a background tailer that feeds messages to Dagster while the
        # with-block is active.
        def _tail():
            pos = 0
            first_seen = None
            while not self._stop.is_set():
                try:
                    with open(self._messages_path, "r", encoding="utf-8") as f:
                        # remember when the file first appears
                        if first_seen is None:
                            first_seen = time.time()
                        # continue from last offset
                        f.seek(pos, os.SEEK_SET)
                        for line in f:
                            pos = f.tell()
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                msg: Mapping[str, Any] = json.loads(line)
                            except Exception:
                                # ignore malformed/incomplete lines
                                continue

                            # call the handler in the most compatible way
                            if hasattr(message_handler, "handle_message"):
                                message_handler.handle_message(msg)
                            elif hasattr(message_handler, "consume_message"):
                                message_handler.consume_message(msg)
                            else:
                                # Fallback: if the handler API changes, fail loudly
                                raise RuntimeError(
                                    "Unsupported Pipes message handler interface"
                                )

                except FileNotFoundError:
                    # Wait for file to appear, but honor a creation timeout so Dagster
                    # can render a helpful “no messages” warning.
                    if first_seen is None:
                        # haven’t seen the file yet
                        if self._creation_timeout > 0 and (
                            time.time() - (first_seen or time.time())
                        ) > self._creation_timeout:
                            # Let the outer session decide what to do; just idle here.
                            pass
                    # Nothing to tail yet.

                # Small sleep to avoid tight loop
                self._stop.wait(self._poll_interval)

        self._stop.clear()
        self._thread = threading.Thread(target=_tail, name="pipes-local-tail", daemon=True)
        self._thread.start()
        try:
            # Yield immediately so the caller can start the child process.
            yield params
        finally:
            # Stop tailing when the session ends.
            self._stop.set()
            if self._thread is not None:
                self._thread.join(timeout=5.0)
                self._thread = None
