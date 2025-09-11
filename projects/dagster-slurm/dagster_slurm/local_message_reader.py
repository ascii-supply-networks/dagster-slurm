# local_message_reader.py
import json
import os
import threading
import time
from contextlib import contextmanager
from typing import Mapping, Any, Optional, Dict

from dagster import PipesMessageReader


class LocalExecTailMessageReader(PipesMessageReader):
    """
    Tail a local messages.jsonl file and forward each JSON line to Dagster.
    Yields immediately so the caller can start the child process.
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
        # Return **message writer params** expected by dagster_pipes (not env!)
        # The context injector will take these and encode/inject them for the payload.
        messages_params: Dict[str, Any] = {"path": self._messages_path}
        if self._include_stdio:
            messages_params["stdio"] = True

        def _tail():
            pos = 0
            first_seen: Optional[float] = None

            while not self._stop.is_set():
                try:
                    with open(self._messages_path, "r", encoding="utf-8", errors="replace") as f:
                        if first_seen is None:
                            first_seen = time.time()

                        # If file truncated/rotated, reset pos
                        try:
                            size = os.fstat(f.fileno()).st_size
                        except Exception:
                            size = None
                        if pos > 0 and size is not None and pos > size:
                            pos = 0

                        # Continue from last offset
                        if pos:
                            f.seek(pos, os.SEEK_SET)

                        # Use readline() loop so tell() is legal
                        line = f.readline()
                        while line and not self._stop.is_set():
                            pos = f.tell()  # after reading the line
                            s = line.strip()
                            if s:
                                try:
                                    msg: Mapping[str, Any] = json.loads(s)
                                except Exception:
                                    # Ignore partial/garbage lines
                                    pass
                                else:
                                    if hasattr(message_handler, "handle_message"):
                                        message_handler.handle_message(msg)
                                    elif hasattr(message_handler, "consume_message"):
                                        message_handler.consume_message(msg)
                                    else:
                                        raise RuntimeError(
                                            "Unsupported Pipes message handler interface"
                                        )
                            line = f.readline()

                except FileNotFoundError:
                    # Wait for the file to appear; allow a grace period.
                    if first_seen is None:
                        first_seen = time.time()
                    # (We deliberately don't raise; Dagster will show your debug text.)

                # Avoid a tight loop
                self._stop.wait(self._poll_interval)

        self._stop.clear()
        self._thread = threading.Thread(target=_tail, name="pipes-local-tail", daemon=True)
        self._thread.start()
        try:
            # Yield immediately so the caller can start the child
            yield messages_params
        finally:
            self._stop.set()
            if self._thread is not None:
                self._thread.join(timeout=5.0)
                self._thread = None
