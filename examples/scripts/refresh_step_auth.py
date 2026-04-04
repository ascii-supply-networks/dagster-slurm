import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from dagster_slurm.auth.step_oidc import StepOIDCAuthProvider


def _get_env(name: str, required: bool = True) -> str | None:
    value = os.getenv(name)
    if required and not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Refresh Step SSH auth and optionally rotate app password."
    )
    parser.add_argument(
        "--rotate-app-password",
        action="store_true",
        help="Rotate the authentik app password via API.",
    )
    args = parser.parse_args()
    context_name = os.getenv("ASC_STEP_CONTEXT", "asc")
    cert_path = os.getenv("ASC_STEP_CERT_PATH")
    if not cert_path:
        candidates = [
            f"~/.step/ssh/certs/{context_name}.crt",
            "~/.ssh/id_musica-cert.pub",
            "~/.ssh/id_ed25519-cert.pub",
            "~/.ssh/id_rsa-cert.pub",
        ]
        for candidate in candidates:
            expanded = os.path.expanduser(candidate)
            if os.path.exists(expanded):
                cert_path = expanded
                break
    elif not cert_path.endswith((".crt", "-cert.pub")):
        key_path = os.path.expanduser(cert_path)
        cert_candidate = key_path + "-cert.pub"
        if os.path.exists(cert_candidate):
            cert_path = cert_candidate

    print(f"Using ASC_STEP_CERT_PATH: {cert_path}")
    provider = StepOIDCAuthProvider(
        token_url=_get_env(
            "ASC_OIDC_TOKEN_URL", required=False
        )
        or "https://auth.asc.ac.at/application/o/token/",
        client_id=_get_env("ASC_OIDC_CLIENT_ID"),
        username=_get_env("ASC_OIDC_USERNAME"),
        app_password=_get_env("ASC_OIDC_APP_PASSWORD"),
        scope=os.getenv("ASC_OIDC_SCOPE", "profile"),
        token_field=os.getenv("ASC_OIDC_TOKEN_FIELD", "access_token"),
        context=context_name,
        refresh_skew_minutes=int(os.getenv("ASC_STEP_REFRESH_SKEW", "30")),
        cert_path=cert_path,
        bootstrap_ca_url=os.getenv("ASC_STEP_CA_URL"),
        bootstrap_fingerprint=os.getenv("ASC_STEP_FINGERPRINT"),
    )

    before = provider._read_cert_valid_until()
    if before:
        print(f"Cert valid until (before): {before.isoformat()}")
    else:
        print("Cert not found or unreadable (before)")

    provider.ensure()

    after = provider._read_cert_valid_until()
    if after:
        if after.tzinfo is None:
            after = after.replace(tzinfo=timezone.utc)
        print(f"Cert valid until (after): {after.isoformat()}")
        remaining = after - datetime.now(timezone.utc)
        print(f"Remaining: {remaining}")
    else:
        print("Cert not found or unreadable (after)")

    if args.rotate_app_password:
        _rotate_app_password()


def _rotate_app_password() -> None:
    """Rotate the authentik app password via API.

    ASC_AUTHENTIK_API_TOKEN must be an authentik **Token** (not an App
    Password).  Create one via "New Token" at the authentik user settings page.
    See https://github.com/ascii-supply-networks/dagster-slurm/issues/91.
    """
    base_url = os.getenv("ASC_AUTHENTIK_API_BASE")
    api_token = os.getenv("ASC_AUTHENTIK_API_TOKEN")
    if not base_url or not api_token:
        raise RuntimeError(
            "Missing ASC_AUTHENTIK_API_BASE or ASC_AUTHENTIK_API_TOKEN"
        )

    base_url = base_url.rstrip("/")
    user_id = _lookup_current_user_id(base_url, api_token)
    identifier = os.getenv("ASC_AUTHENTIK_TOKEN_IDENTIFIER", "dagster-slurm")

    _delete_token_if_exists(base_url, api_token, identifier)
    _create_token(base_url, api_token, identifier, user_id)

    token_value = _retrieve_token_key(base_url, api_token, identifier)
    output_path = os.getenv("ASC_AUTHENTIK_APP_PASSWORD_OUTPUT")
    if output_path:
        expanded = os.path.expanduser(output_path)
        with open(expanded, "w") as handle:
            handle.write(token_value)
        os.chmod(expanded, 0o600)
        print(f"Wrote new app password to: {output_path}")
    else:
        print("New app password (store securely as ASC_OIDC_APP_PASSWORD):")
        print(token_value)


def _create_token(
    base_url: str, api_token: str, identifier: str, user_id: int
) -> None:
    """Create a new authentik token via the API."""
    payload: dict[str, str | int] = {
        "identifier": identifier,
        "intent": os.getenv("ASC_AUTHENTIK_TOKEN_INTENT", "app_password"),
        "user": user_id,
    }
    expires_days = os.getenv("ASC_AUTHENTIK_TOKEN_EXPIRES_DAYS", "30")
    try:
        days = int(expires_days)
        payload["expires"] = (
            datetime.now(timezone.utc) + timedelta(days=days)
        ).isoformat()
    except ValueError:
        pass

    req = Request(
        f"{base_url}/api/v3/core/tokens/",
        data=json.dumps(payload).encode(),
        headers={
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlopen(req) as response:
            json.loads(response.read().decode())
    except HTTPError as exc:
        if exc.code == 403:
            raise RuntimeError(
                "403 Forbidden creating token. Ensure ASC_AUTHENTIK_API_TOKEN is "
                "an authentik *Token* (not an App Password). In authentik user "
                "settings, use 'New Token' — not 'New App Password'."
            ) from exc
        raise


def _lookup_current_user_id(base_url: str, api_token: str) -> int:
    """Look up the authenticated user's ID via the /me/ endpoint.

    This requires fewer permissions than listing all users.
    """
    url = f"{base_url}/api/v3/core/users/me/"
    req = Request(
        url,
        headers={"Authorization": f"Bearer {api_token}"},
        method="GET",
    )
    try:
        with urlopen(req) as response:
            data = json.loads(response.read().decode())
    except HTTPError as exc:
        if exc.code == 403:
            raise RuntimeError(
                "403 Forbidden looking up user. Ensure ASC_AUTHENTIK_API_TOKEN is "
                "an authentik *Token* (not an App Password). In authentik user "
                "settings, use 'New Token' — not 'New App Password'."
            ) from exc
        raise
    user_data = data.get("user", data)
    return user_data["pk"]


def _delete_token_if_exists(
    base_url: str, api_token: str, identifier: str
) -> None:
    """Delete an existing token by identifier (ignores 404)."""
    url = f"{base_url}/api/v3/core/tokens/{identifier}/"
    req = Request(
        url,
        headers={"Authorization": f"Bearer {api_token}"},
        method="DELETE",
    )
    try:
        with urlopen(req):
            print(f"Deleted existing token: {identifier}")
    except HTTPError as exc:
        if exc.code == 404:
            pass
        else:
            print(f"Warning: failed to delete old token {identifier}: {exc}")


def _retrieve_token_key(base_url: str, api_token: str, identifier: str) -> str:
    """Retrieve the actual token secret via the view_key endpoint."""
    url = f"{base_url}/api/v3/core/tokens/{identifier}/view_key/"
    req = Request(
        url,
        headers={"Authorization": f"Bearer {api_token}"},
        method="GET",
    )
    with urlopen(req) as response:
        data = json.loads(response.read().decode())
    key = data.get("key")
    if not key:
        raise RuntimeError(f"Token created but no key returned: {data}")
    return key


if __name__ == "__main__":
    main()
