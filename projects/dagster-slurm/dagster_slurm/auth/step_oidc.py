from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass
class StepOIDCAuthProvider:
    """Auth provider that refreshes a Step SSH certificate via OIDC client credentials."""

    token_url: str
    client_id: str
    username: str
    app_password: str
    scope: str = "profile"
    token_field: str = "access_token"
    context: str = "asc"
    refresh_skew_minutes: int = 30
    cert_path: Optional[str] = None
    step_binary: str = "step"
    extra_step_args: list[str] = field(default_factory=list)
    bootstrap_ca_url: Optional[str] = None
    bootstrap_fingerprint: Optional[str] = None

    def ensure(self) -> None:
        """Refresh Step SSH certificate if missing or expiring soon."""
        cert_valid_until = self._read_cert_valid_until()
        if cert_valid_until and not self._is_expiring_soon(cert_valid_until):
            return

        token = self._fetch_bearer_token()
        if self.bootstrap_ca_url and self.bootstrap_fingerprint:
            self._bootstrap_step_ca()
        self._step_login(token)

    def _read_cert_valid_until(self) -> Optional[datetime]:
        if not self.cert_path:
            return None
        cert_path = self._resolve_cert_path(self.cert_path)
        if not cert_path or not os.path.exists(cert_path):
            return None
        try:
            result = subprocess.run(
                ["ssh-keygen", "-L", "-f", cert_path],
                check=True,
                capture_output=True,
                text=True,
            )
        except Exception:
            return None
        for line in result.stdout.splitlines():
            if "Valid:" in line and "to" in line:
                # Example: Valid: from 2026-01-21T10:00:00 to 2026-01-21T16:00:00
                parts = line.split("to", maxsplit=1)
                if len(parts) != 2:
                    continue
                candidate = parts[1].strip().split()[0]
                try:
                    dt = datetime.fromisoformat(candidate)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
                except ValueError:
                    continue
        return None

    def _resolve_cert_path(self, raw_path: str) -> Optional[str]:
        cert_path = os.path.expanduser(raw_path)
        if os.path.exists(cert_path):
            return cert_path
        # If a private key was provided, try its paired cert
        if not cert_path.endswith((".crt", "-cert.pub")):
            candidate = cert_path + "-cert.pub"
            if os.path.exists(candidate):
                return candidate
        # Fall back to default step cert location for the context
        step_cert = os.path.expanduser(f"~/.step/ssh/certs/{self.context}.crt")
        if os.path.exists(step_cert):
            return step_cert
        return None

    def _is_expiring_soon(self, valid_until: datetime) -> bool:
        return (
            datetime.now(timezone.utc) + timedelta(minutes=self.refresh_skew_minutes)
            >= valid_until
        )

    def _fetch_bearer_token(self) -> str:
        payload = urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "username": self.username,
                "password": self.app_password,
                "scope": self.scope,
            }
        ).encode()
        request = Request(
            self.token_url,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        with urlopen(request) as response:
            data = json.loads(response.read().decode())
        token = data.get(self.token_field)
        if not token:
            raise RuntimeError(
                f"Token field '{self.token_field}' not found in response: {data}"
            )
        return token

    def _bootstrap_step_ca(self) -> None:
        if self.bootstrap_ca_url is None or self.bootstrap_fingerprint is None:
            return
        force = os.getenv("ASC_STEP_BOOTSTRAP_FORCE", "").strip().lower() in (
            "1",
            "true",
            "yes",
        )
        if not force:
            defaults_path = os.path.expanduser(
                f"~/.step/authorities/{self.context}/config/defaults.json"
            )
            if os.path.exists(defaults_path):
                return
        cmd = [
            self.step_binary,
            "ca",
            "bootstrap",
            "--context",
            self.context,
            "--ca-url",
            self.bootstrap_ca_url,
            "--fingerprint",
            self.bootstrap_fingerprint,
        ]
        if force:
            cmd.append("--force")
        subprocess.run(cmd, check=True)

    def _step_login(self, token: str) -> None:
        cmd = [
            self.step_binary,
            "ssh",
            "login",
            "--context",
            self.context,
            "--token",
            token,
            *self.extra_step_args,
        ]
        subprocess.run(cmd, check=True)
