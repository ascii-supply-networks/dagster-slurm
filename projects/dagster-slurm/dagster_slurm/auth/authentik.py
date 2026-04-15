from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen


TOKEN_API_MISCONFIG_MESSAGE = (
    "Ensure ASC_AUTHENTIK_API_TOKEN is an authentik *Token* (not an App "
    "Password). In authentik user settings, use 'New Token' — not 'New App "
    "Password'."
)

API_TOKEN_ENV_KEY = "ASC_AUTHENTIK_API_TOKEN"
API_TOKEN_ID_ENV_KEY = "ASC_AUTHENTIK_API_TOKEN_ID"
API_TOKEN_EXPIRES_AT_ENV_KEY = "ASC_AUTHENTIK_API_TOKEN_EXPIRES_AT"
APP_PASSWORD_ENV_KEY = "ASC_OIDC_APP_PASSWORD"
APP_PASSWORD_ID_ENV_KEY = "ASC_OIDC_APP_PASSWORD_ID"
APP_PASSWORD_EXPIRES_AT_ENV_KEY = "ASC_OIDC_APP_PASSWORD_EXPIRES_AT"


@dataclass
class AuthentikCredentials:
    api_token: str | None = None
    api_token_id: str | None = None
    api_token_expires_at: datetime | None = None
    app_password: str | None = None
    app_password_id: str | None = None
    app_password_expires_at: datetime | None = None

    def with_defaults(
        self, *, api_token: str | None, app_password: str | None
    ) -> AuthentikCredentials:
        return AuthentikCredentials(
            api_token=self.api_token or api_token,
            api_token_id=self.api_token_id,
            api_token_expires_at=self.api_token_expires_at,
            app_password=self.app_password or app_password,
            app_password_id=self.app_password_id,
            app_password_expires_at=self.app_password_expires_at,
        )


@dataclass
class AuthentikCredentialStore:
    path: Path

    def exists(self) -> bool:
        return self.path.exists()

    def load(self) -> AuthentikCredentials:
        if not self.exists():
            return AuthentikCredentials()

        values: dict[str, str | None] = {}
        for raw_line in self.path.read_text().splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            key, separator, value = line.partition("=")
            if not separator:
                continue
            values[key.strip()] = _parse_env_value(value.strip())

        return AuthentikCredentials(
            api_token=values.get(API_TOKEN_ENV_KEY),
            api_token_id=values.get(API_TOKEN_ID_ENV_KEY),
            api_token_expires_at=_parse_timestamp(
                values.get(API_TOKEN_EXPIRES_AT_ENV_KEY)
            ),
            app_password=values.get(APP_PASSWORD_ENV_KEY),
            app_password_id=values.get(APP_PASSWORD_ID_ENV_KEY),
            app_password_expires_at=_parse_timestamp(
                values.get(APP_PASSWORD_EXPIRES_AT_ENV_KEY)
            ),
        )

    def save(self, credentials: AuthentikCredentials) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            "# Rotated by dagster-slurm; keep this file mode 0600.",
            f"{API_TOKEN_ENV_KEY}={json.dumps(credentials.api_token)}",
            f"{API_TOKEN_ID_ENV_KEY}={json.dumps(credentials.api_token_id)}",
            (
                f"{API_TOKEN_EXPIRES_AT_ENV_KEY}="
                f"{json.dumps(_format_timestamp(credentials.api_token_expires_at))}"
            ),
            f"{APP_PASSWORD_ENV_KEY}={json.dumps(credentials.app_password)}",
            f"{APP_PASSWORD_ID_ENV_KEY}={json.dumps(credentials.app_password_id)}",
            (
                f"{APP_PASSWORD_EXPIRES_AT_ENV_KEY}="
                f"{json.dumps(_format_timestamp(credentials.app_password_expires_at))}"
            ),
            "",
        ]
        tmp_path = self.path.with_suffix(f"{self.path.suffix}.tmp")
        tmp_path.write_text("\n".join(lines))
        os.chmod(tmp_path, 0o600)
        tmp_path.replace(self.path)


@dataclass
class AuthentikCredentialRotator:
    base_url: str
    api_token: str
    identifier_prefix: str = "dagster-slurm"
    expires_days: int = 30

    def rotate(self) -> AuthentikCredentials:
        base_url = self.base_url.rstrip("/")
        user_id = self._lookup_current_user_id(base_url)
        expires_at = datetime.now(timezone.utc) + timedelta(days=self.expires_days)
        suffix = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        app_password_id = f"{self.identifier_prefix}-app-{suffix}"
        api_token_id = f"{self.identifier_prefix}-api-{suffix}"

        try:
            app_password = self._create_token_with_key(
                base_url=base_url,
                user_id=user_id,
                identifier=app_password_id,
                intent="app_password",
                expires_at=expires_at,
            )
            api_token = self._create_token_with_key(
                base_url=base_url,
                user_id=user_id,
                identifier=api_token_id,
                intent="api",
                expires_at=expires_at,
            )
        except Exception:
            self.delete_token(base_url, app_password_id)
            self.delete_token(base_url, api_token_id)
            raise

        return AuthentikCredentials(
            api_token=api_token,
            api_token_id=api_token_id,
            api_token_expires_at=expires_at,
            app_password=app_password,
            app_password_id=app_password_id,
            app_password_expires_at=expires_at,
        )

    def cleanup_previous(self, credentials: AuthentikCredentials) -> None:
        base_url = self.base_url.rstrip("/")
        for identifier in (
            credentials.app_password_id,
            credentials.api_token_id,
        ):
            self.delete_token(base_url, identifier)

    def delete_token(self, base_url: str, identifier: str | None) -> None:
        if not identifier:
            return
        request = Request(
            f"{base_url}/api/v3/core/tokens/{identifier}/",
            headers=self._headers(),
            method="DELETE",
        )
        try:
            with urlopen(request):
                return
        except HTTPError as exc:
            if exc.code == 404:
                return
            raise

    def _create_token_with_key(
        self,
        *,
        base_url: str,
        user_id: int,
        identifier: str,
        intent: str,
        expires_at: datetime,
    ) -> str:
        self._create_token(
            base_url=base_url,
            user_id=user_id,
            identifier=identifier,
            intent=intent,
            expires_at=expires_at,
        )
        return self._retrieve_token_key(base_url, identifier)

    def _create_token(
        self,
        *,
        base_url: str,
        user_id: int,
        identifier: str,
        intent: str,
        expires_at: datetime,
    ) -> None:
        payload: dict[str, str | int | bool] = {
            "identifier": identifier,
            "intent": intent,
            "user": user_id,
            "expiring": True,
            "expires": expires_at.isoformat(),
        }
        request = Request(
            f"{base_url}/api/v3/core/tokens/",
            data=json.dumps(payload).encode(),
            headers=self._headers("application/json"),
            method="POST",
        )
        self._open_json(request, forbidden_context="creating token")

    def _lookup_current_user_id(self, base_url: str) -> int:
        request = Request(
            f"{base_url}/api/v3/core/users/me/",
            headers=self._headers(),
            method="GET",
        )
        data = self._open_json(request, forbidden_context="looking up user")
        user_data = data.get("user", data)
        return user_data["pk"]

    def _retrieve_token_key(self, base_url: str, identifier: str) -> str:
        request = Request(
            f"{base_url}/api/v3/core/tokens/{identifier}/view_key/",
            headers=self._headers(),
            method="GET",
        )
        data = self._open_json(request)
        key = data.get("key")
        if not key:
            raise RuntimeError(f"Token created but no key returned: {data}")
        return key

    def _headers(self, content_type: str | None = None) -> dict[str, str]:
        headers = {"Authorization": f"Bearer {self.api_token}"}
        if content_type:
            headers["Content-Type"] = content_type
        return headers

    def _open_json(
        self, request: Request, *, forbidden_context: str | None = None
    ) -> dict[str, Any]:
        try:
            with urlopen(request) as response:
                return json.loads(response.read().decode())
        except HTTPError as exc:
            if exc.code == 403 and forbidden_context:
                raise RuntimeError(
                    f"403 Forbidden {forbidden_context}. {TOKEN_API_MISCONFIG_MESSAGE}"
                ) from exc
            raise


def credential_store_from_env(path: str | None) -> AuthentikCredentialStore | None:
    if not path:
        return None
    return AuthentikCredentialStore(Path(path).expanduser())


def _parse_env_value(raw_value: str) -> str | None:
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError:
        return raw_value
    if parsed is None:
        return None
    return str(parsed)


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _format_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()
