from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .authentik import (
    AuthentikCredentialRotator,
    AuthentikCredentials,
    credential_store_from_env,
)


class StepSSHLoginError(RuntimeError):
    """Raised when the Step CLI cannot mint or install an SSH certificate."""


@dataclass
class StepOIDCAuthProvider:
    """Auth provider that refreshes a Step SSH certificate via OIDC client credentials."""

    token_url: str
    client_id: str
    username: str
    app_password: Optional[str] = None
    scope: str = "profile"
    token_field: str = "access_token"
    context: str = "asc"
    refresh_skew_minutes: int = 30
    cert_path: Optional[str] = None
    ssh_key_path: Optional[str] = None
    step_binary: str = "step"
    extra_step_args: list[str] = field(default_factory=list)
    bootstrap_ca_url: Optional[str] = None
    bootstrap_fingerprint: Optional[str] = None
    authentik_api_base: Optional[str] = None
    authentik_api_token: Optional[str] = None
    authentik_token_identifier: str = "dagster-slurm"
    authentik_token_expires_days: int = 30
    authentik_credentials_file: Optional[str] = None
    authentik_refresh_skew_days: int = 7

    def ensure(self) -> None:
        """Refresh Step SSH certificate if missing or expiring soon."""
        self._sync_credentials_from_store()
        if self._should_rotate_managed_credentials():
            self.rotate_authentik_credentials()

        cert_valid_until = self._read_cert_valid_until()
        if cert_valid_until and not self._is_expiring_soon(cert_valid_until):
            return

        token = self._fetch_bearer_token()
        if self.bootstrap_ca_url and self.bootstrap_fingerprint:
            self._bootstrap_step_ca()
        self._step_login(token)

    def _read_cert_valid_until(self) -> Optional[datetime]:
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

    def _resolve_cert_path(self, raw_path: str | None) -> Optional[str]:
        for candidate in self._candidate_cert_paths(raw_path):
            if candidate.exists():
                return str(candidate)
        return None

    def _candidate_cert_paths(self, raw_path: str | None) -> list[Path]:
        candidates: list[Path] = []
        seen: set[str] = set()

        def add(candidate: Path) -> None:
            candidate_str = str(candidate)
            if candidate_str in seen:
                return
            seen.add(candidate_str)
            candidates.append(candidate)

        if raw_path:
            expanded = Path(raw_path).expanduser()
            expanded_str = str(expanded)
            if expanded_str.endswith((".crt", "-cert.pub")):
                add(expanded)
            else:
                add(Path(f"{expanded_str}-cert.pub"))

        ssh_key_path = self._resolve_ssh_key_path()
        if ssh_key_path:
            add(Path(f"{ssh_key_path}-cert.pub"))

        add(Path(f"~/.step/ssh/certs/{self.context}.crt").expanduser())
        return candidates

    def _resolve_ssh_key_path(self) -> Optional[str]:
        candidates: list[Path] = []
        if self.ssh_key_path:
            candidates.append(Path(self.ssh_key_path).expanduser())
        if self.cert_path:
            cert_candidate = Path(self.cert_path).expanduser()
            cert_candidate_str = str(cert_candidate)
            if cert_candidate_str.endswith("-cert.pub"):
                candidates.append(Path(cert_candidate_str.removesuffix("-cert.pub")))
            elif not cert_candidate_str.endswith(".crt"):
                candidates.append(cert_candidate)

        seen: set[str] = set()
        for candidate in candidates:
            candidate_str = str(candidate)
            if candidate_str in seen:
                continue
            seen.add(candidate_str)
            if candidate.exists():
                return candidate_str
        return None

    def _is_expiring_soon(self, valid_until: datetime) -> bool:
        return (
            datetime.now(timezone.utc) + timedelta(minutes=self.refresh_skew_minutes)
            >= valid_until
        )

    def _fetch_bearer_token(self) -> str:
        self._ensure_app_password()
        assert self.app_password is not None
        try:
            return self._fetch_bearer_token_with_password(self.app_password)
        except HTTPError as exc:
            if not self._should_retry_with_rotated_password(exc):
                raise
        self.rotate_authentik_credentials()
        assert self.app_password is not None
        return self._fetch_bearer_token_with_password(self.app_password)

    def _ensure_app_password(self) -> None:
        if self.app_password:
            return
        if not self._can_rotate_credentials():
            raise RuntimeError(
                "Missing managed Authentik credentials. Configure "
                "ASC_AUTHENTIK_CREDENTIALS_FILE or provide a valid "
                "ASC_AUTHENTIK_API_TOKEN for bootstrap."
            )
        self.rotate_authentik_credentials()

    def _should_retry_with_rotated_password(self, exc: HTTPError) -> bool:
        return self._can_rotate_credentials() and exc.code in {400, 401, 403}

    def _fetch_bearer_token_with_password(self, app_password: str) -> str:
        payload = urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "username": self.username,
                "password": app_password,
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

    def rotate_authentik_credentials(self) -> AuthentikCredentials:
        if not self._can_rotate_credentials():
            raise RuntimeError(
                "Missing ASC_AUTHENTIK_API_BASE or a current Authentik API token "
                "for credential rotation."
            )
        current_credentials = self._current_credentials()
        current_api_token = current_credentials.api_token or self.authentik_api_token
        assert self.authentik_api_base is not None
        assert current_api_token is not None

        rotator = AuthentikCredentialRotator(
            base_url=self.authentik_api_base,
            api_token=current_api_token,
            identifier_prefix=self.authentik_token_identifier,
            expires_days=self.authentik_token_expires_days,
        )
        new_credentials = rotator.rotate()
        store = self._credential_store()
        if store is not None:
            store.save(new_credentials)
        rotator.cleanup_previous(current_credentials)
        self._apply_credentials(new_credentials)
        return new_credentials

    def _can_rotate_credentials(self) -> bool:
        credentials = self._current_credentials()
        return bool(
            self.authentik_api_base
            and (credentials.api_token or self.authentik_api_token)
        )

    def _sync_credentials_from_store(self) -> None:
        store = self._credential_store()
        if store is None:
            return
        self._apply_credentials(
            store.load().with_defaults(
                api_token=self.authentik_api_token,
                app_password=self.app_password,
            )
        )

    def _should_rotate_managed_credentials(self) -> bool:
        if not self._credential_store_enabled():
            return False
        if not self._can_rotate_credentials():
            return False

        store = self._credential_store()
        assert store is not None
        if not store.exists():
            return True

        credentials = self._current_credentials()
        if not credentials.api_token or not credentials.app_password:
            return True
        if credentials.api_token_expires_at is None:
            return True
        if credentials.app_password_expires_at is None:
            return True
        return any(
            self._is_credential_expiring_soon(expires_at)
            for expires_at in (
                credentials.api_token_expires_at,
                credentials.app_password_expires_at,
            )
        )

    def _is_credential_expiring_soon(self, value: datetime | None) -> bool:
        if value is None:
            return True
        return (
            datetime.now(timezone.utc)
            + timedelta(days=self.authentik_refresh_skew_days)
            >= value
        )

    def _credential_store(self):
        return credential_store_from_env(self.authentik_credentials_file)

    def _credential_store_enabled(self) -> bool:
        return self._credential_store() is not None

    def _current_credentials(self) -> AuthentikCredentials:
        store = self._credential_store()
        if store is None:
            return AuthentikCredentials(
                api_token=self.authentik_api_token,
                app_password=self.app_password,
            )
        return store.load().with_defaults(
            api_token=self.authentik_api_token,
            app_password=self.app_password,
        )

    def _apply_credentials(self, credentials: AuthentikCredentials) -> None:
        self.authentik_api_token = credentials.api_token or self.authentik_api_token
        self.app_password = credentials.app_password or self.app_password

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
        ssh_key_path = self._resolve_ssh_key_path()
        if ssh_key_path:
            public_key_path = self._ensure_public_key_path(ssh_key_path)
            cmd = [
                self.step_binary,
                "ssh",
                "certificate",
                self.username,
                public_key_path,
                "--sign",
                "--private-key",
                ssh_key_path,
                "--token",
                token,
                "--context",
                self.context,
                "--force",
                *self.extra_step_args,
            ]
        else:
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
        self._run_step_command(cmd)

    def _ensure_public_key_path(self, private_key_path: str) -> str:
        public_key_path = Path(f"{private_key_path}.pub")
        if public_key_path.exists():
            return str(public_key_path)

        try:
            result = subprocess.run(
                ["ssh-keygen", "-y", "-f", private_key_path],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as exc:
            details = "\n".join(
                part.strip()
                for part in (exc.stdout, exc.stderr)
                if part and part.strip()
            )
            raise StepSSHLoginError(
                "Step SSH certificate refresh could not derive the public key for "
                f"{private_key_path}: {details or 'ssh-keygen -y failed.'}"
            ) from None

        public_key_path.write_text(result.stdout)
        public_key_path.chmod(0o644)
        return str(public_key_path)

    def _run_step_command(self, cmd: list[str]) -> None:
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as exc:
            details = "\n".join(
                part.strip()
                for part in (exc.stdout, exc.stderr)
                if part and part.strip()
            )
            if "disabled due to an initialization error" in details:
                raise StepSSHLoginError(
                    "Step SSH certificate refresh failed after credential rotation. "
                    "The ASC Step CA rejected the OIDC token because the remote "
                    "OIDC provisioner appears uninitialized. The local credentials "
                    "file was still updated successfully. "
                    "Ask ASC to fix the Step/Authentik provisioner, or verify the "
                    "interactive flow separately with `step ssh login --context asc --console`."
                ) from None
            if details:
                raise StepSSHLoginError(
                    f"Step SSH certificate refresh failed: {details}"
                ) from None
            raise StepSSHLoginError(
                "Step SSH certificate refresh failed with no diagnostic output."
            ) from None
