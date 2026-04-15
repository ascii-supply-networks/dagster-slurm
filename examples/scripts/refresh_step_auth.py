import argparse
import os
from datetime import datetime, timezone

from dagster_slurm.auth.authentik import AuthentikCredentials
from dagster_slurm.auth.step_oidc import StepOIDCAuthProvider


def _get_env(name: str, required: bool = True) -> str | None:
    value = os.getenv(name)
    if required and not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def main() -> None:
    args = _parse_args()
    context_name = os.getenv("ASC_STEP_CONTEXT", "asc")
    cert_path = _resolve_cert_path(context_name)
    print(f"Using ASC_STEP_CERT_PATH: {cert_path}")
    provider = _build_provider(
        context_name=context_name,
        cert_path=cert_path,
        app_password=os.getenv("ASC_OIDC_APP_PASSWORD"),
    )
    rotated_credentials, skip_step_refresh = _maybe_rotate_credentials(args, provider)
    if skip_step_refresh:
        return

    step_refresh_completed, cert_refreshed = _refresh_step_certificate(
        provider, rotated_credentials
    )
    if not step_refresh_completed:
        return

    if rotated_credentials is not None:
        if cert_refreshed:
            print("Used the newly rotated Authentik credentials for Step/OIDC refresh.")
        else:
            print(
                "Rotated Authentik credentials successfully. Existing Step SSH "
                "certificate is still valid; no certificate refresh was needed."
            )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh Step SSH auth and optionally rotate Authentik credentials."
    )
    parser.add_argument(
        "--rotate-credentials",
        "--rotate-app-password",
        dest="rotate_credentials",
        action="store_true",
        help="Rotate the authentik API token and app password via API.",
    )
    parser.add_argument(
        "--refresh-step-after-rotate",
        action="store_true",
        help="After rotating credentials, also refresh the Step SSH certificate.",
    )
    return parser.parse_args()


def _maybe_rotate_credentials(
    args: argparse.Namespace,
    provider: StepOIDCAuthProvider | None,
) -> tuple[AuthentikCredentials | None, bool]:
    if not args.rotate_credentials:
        return None, False

    if provider is None:
        rotated_credentials = _rotate_credentials_without_provider()
    else:
        rotated_credentials = provider.rotate_authentik_credentials()
        _print_rotated_credentials(rotated_credentials)

    if args.refresh_step_after_rotate:
        return rotated_credentials, False

    print(
        "Rotated Authentik credentials successfully. "
        "Skipping Step certificate refresh. Pass "
        "--refresh-step-after-rotate to refresh the Step SSH certificate too."
    )
    return rotated_credentials, True


def _resolve_cert_path(context_name: str) -> str | None:
    cert_path = os.getenv("ASC_STEP_CERT_PATH")
    if cert_path:
        return _normalize_cert_path(cert_path)

    ssh_key_path = os.getenv("SLURM_EDGE_NODE_KEY_PATH")
    if ssh_key_path:
        return _normalize_cert_path(ssh_key_path)

    return _find_existing_path(_default_cert_candidates(context_name))


def _refresh_step_certificate(
    provider: StepOIDCAuthProvider | None,
    rotated_credentials: AuthentikCredentials | None,
) -> tuple[bool, bool]:
    provider = _require_provider_or_skip(provider, rotated_credentials)
    if provider is None:
        return False, False

    before_valid_until = provider._read_cert_valid_until()
    _print_cert_valid_until(before_valid_until, label="before")
    provider.ensure()
    after_valid_until = provider._read_cert_valid_until()
    _print_cert_valid_until(after_valid_until, label="after")
    return True, _cert_was_refreshed(before_valid_until, after_valid_until)


def _default_cert_candidates(context_name: str) -> list[str]:
    return [
        f"~/.step/ssh/certs/{context_name}.crt",
        "~/.ssh/id_musica-cert.pub",
        "~/.ssh/id_ed25519-cert.pub",
        "~/.ssh/id_rsa-cert.pub",
    ]


def _find_existing_path(candidates: list[str]) -> str | None:
    for candidate in candidates:
        expanded = os.path.expanduser(candidate)
        if os.path.exists(expanded):
            return expanded
    return None


def _normalize_cert_path(cert_path: str) -> str:
    if cert_path.endswith((".crt", "-cert.pub")):
        return cert_path

    key_path = os.path.expanduser(cert_path)
    cert_candidate = key_path + "-cert.pub"
    if os.path.exists(cert_candidate):
        return cert_candidate
    return cert_path


def _require_provider_or_skip(
    provider: StepOIDCAuthProvider | None,
    rotated_credentials: AuthentikCredentials | None,
) -> StepOIDCAuthProvider | None:
    if provider is not None:
        return provider
    if rotated_credentials is None:
        raise RuntimeError(
            "Missing OIDC credentials. Provide ASC_OIDC_CLIENT_ID plus "
            "ASC_OIDC_USERNAME and configure ASC_AUTHENTIK_CREDENTIALS_FILE "
            "or a bootstrap ASC_AUTHENTIK_API_TOKEN."
        )
    print(
        "Rotated Authentik credentials successfully. Skipping Step certificate refresh "
        "because ASC_OIDC_CLIENT_ID or ASC_OIDC_USERNAME is missing."
    )
    return None


def _print_cert_valid_until(valid_until: datetime | None, *, label: str) -> None:
    if valid_until is None:
        print(f"Cert not found or unreadable ({label})")
        return
    if label == "after" and valid_until.tzinfo is None:
        valid_until = valid_until.replace(tzinfo=timezone.utc)
    print(f"Cert valid until ({label}): {valid_until.isoformat()}")
    if label == "after":
        remaining = valid_until - datetime.now(timezone.utc)
        print(f"Remaining: {remaining}")


def _cert_was_refreshed(
    before_valid_until: datetime | None, after_valid_until: datetime | None
) -> bool:
    if after_valid_until is None:
        return False
    if before_valid_until is None:
        return True
    return _normalize_datetime(after_valid_until) != _normalize_datetime(
        before_valid_until
    )


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _build_provider(
    *,
    context_name: str,
    cert_path: str | None,
    app_password: str | None,
) -> StepOIDCAuthProvider | None:
    """Create the Step OIDC provider when all required inputs are available."""
    client_id = os.getenv("ASC_OIDC_CLIENT_ID")
    username = os.getenv("ASC_OIDC_USERNAME")
    authentik_api_base = os.getenv("ASC_AUTHENTIK_API_BASE")
    authentik_api_token = os.getenv("ASC_AUTHENTIK_API_TOKEN")
    authentik_credentials_file = os.getenv("ASC_AUTHENTIK_CREDENTIALS_FILE")
    has_rotation_fallback = bool(authentik_api_base and authentik_api_token)
    if not client_id or not username or (
        not app_password
        and not has_rotation_fallback
        and not authentik_credentials_file
    ):
        return None

    return StepOIDCAuthProvider(
        token_url=_get_env(
            "ASC_OIDC_TOKEN_URL", required=False
        )
        or "https://auth.asc.ac.at/application/o/token/",
        client_id=client_id,
        username=username,
        app_password=app_password,
        scope=os.getenv("ASC_OIDC_SCOPE", "profile"),
        token_field=os.getenv("ASC_OIDC_TOKEN_FIELD", "access_token"),
        context=context_name,
        refresh_skew_minutes=int(os.getenv("ASC_STEP_REFRESH_SKEW", "30")),
        cert_path=cert_path,
        ssh_key_path=os.getenv("SLURM_EDGE_NODE_KEY_PATH"),
        bootstrap_ca_url=os.getenv("ASC_STEP_CA_URL"),
        bootstrap_fingerprint=os.getenv("ASC_STEP_FINGERPRINT"),
        authentik_api_base=authentik_api_base,
        authentik_api_token=authentik_api_token,
        authentik_token_identifier=os.getenv(
            "ASC_AUTHENTIK_TOKEN_IDENTIFIER", "dagster-slurm"
        ),
        authentik_token_expires_days=int(
            os.getenv("ASC_AUTHENTIK_TOKEN_EXPIRES_DAYS", "30")
        ),
        authentik_credentials_file=authentik_credentials_file,
        authentik_refresh_skew_days=int(
            os.getenv("ASC_AUTHENTIK_REFRESH_SKEW_DAYS", "7")
        ),
    )


def _rotate_credentials_without_provider() -> AuthentikCredentials:
    provider = StepOIDCAuthProvider(
        token_url=_get_env("ASC_OIDC_TOKEN_URL", required=False)
        or "https://auth.asc.ac.at/application/o/token/",
        client_id=_get_env("ASC_OIDC_CLIENT_ID", required=False) or "",
        username=_get_env("ASC_OIDC_USERNAME", required=False) or "",
        app_password=os.getenv("ASC_OIDC_APP_PASSWORD"),
        scope=os.getenv("ASC_OIDC_SCOPE", "profile"),
        token_field=os.getenv("ASC_OIDC_TOKEN_FIELD", "access_token"),
        context=os.getenv("ASC_STEP_CONTEXT", "asc"),
        refresh_skew_minutes=int(os.getenv("ASC_STEP_REFRESH_SKEW", "30")),
        cert_path=os.getenv("ASC_STEP_CERT_PATH"),
        ssh_key_path=os.getenv("SLURM_EDGE_NODE_KEY_PATH"),
        bootstrap_ca_url=os.getenv("ASC_STEP_CA_URL"),
        bootstrap_fingerprint=os.getenv("ASC_STEP_FINGERPRINT"),
        authentik_api_base=_get_env("ASC_AUTHENTIK_API_BASE"),
        authentik_api_token=os.getenv("ASC_AUTHENTIK_API_TOKEN"),
        authentik_token_identifier=os.getenv(
            "ASC_AUTHENTIK_TOKEN_IDENTIFIER", "dagster-slurm"
        ),
        authentik_token_expires_days=int(
            os.getenv("ASC_AUTHENTIK_TOKEN_EXPIRES_DAYS", "30")
        ),
        authentik_credentials_file=os.getenv("ASC_AUTHENTIK_CREDENTIALS_FILE"),
        authentik_refresh_skew_days=int(
            os.getenv("ASC_AUTHENTIK_REFRESH_SKEW_DAYS", "7")
        ),
    )
    credentials = provider.rotate_authentik_credentials()
    _print_rotated_credentials(credentials)
    return credentials


def _print_rotated_credentials(credentials: AuthentikCredentials) -> None:
    credentials_path = os.getenv("ASC_AUTHENTIK_CREDENTIALS_FILE")
    if credentials_path:
        print(f"Wrote rotated Authentik credentials to: {credentials_path}")
        return

    print("New Authentik API token:")
    print(credentials.api_token)
    print("New Authentik app password:")
    print(credentials.app_password)


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from None
