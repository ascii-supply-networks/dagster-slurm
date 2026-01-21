from __future__ import annotations

from typing import Protocol


class AuthProvider(Protocol):
    """Interface for pluggable auth providers used before SSH connections."""

    def ensure(self) -> None:
        """Ensure credentials are valid (refresh if needed)."""
        ...
