"""Ray dashboard URL discovery from launcher output."""

import threading
from collections.abc import Callable
from urllib.parse import urlsplit

RAY_DASHBOARD_URL_MARKER = "DAGSTER_SLURM_RAY_DASHBOARD_URL="


def ray_dashboard_url_from_line(line: str) -> str | None:
    """Return a validated dashboard URL embedded in a launcher log line."""
    marker_index = line.find(RAY_DASHBOARD_URL_MARKER)
    if marker_index < 0:
        return None

    url = line[marker_index + len(RAY_DASHBOARD_URL_MARKER) :].strip()
    if any(character.isspace() for character in url):
        return None

    try:
        parsed = urlsplit(url)
        _ = parsed.port
    except ValueError:
        return None
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return None
    return url


class RayDashboardLogEmitter:
    """Log each observed Ray dashboard URL once."""

    def __init__(self, log_info: Callable[[str], object]):
        self._log_info = log_info
        self._seen: set[str] = set()
        self._lock = threading.Lock()

    def process_line(self, line: str) -> None:
        url = ray_dashboard_url_from_line(line)
        if url is None:
            return
        with self._lock:
            if url in self._seen:
                return
            self._seen.add(url)
        self._log_info(f"Ray head node web UI: {url}")
