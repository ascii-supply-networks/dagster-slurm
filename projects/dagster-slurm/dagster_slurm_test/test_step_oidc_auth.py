from __future__ import annotations

import importlib.util
import subprocess
from datetime import datetime, timedelta, timezone
from email.message import Message
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError

from dagster_slurm.auth.authentik import AuthentikCredentialStore, AuthentikCredentials
from dagster_slurm.auth.step_oidc import StepOIDCAuthProvider


EXAMPLE_RESOURCES_PATH = (
    Path(__file__).resolve().parents[3]
    / "examples"
    / "projects"
    / "dagster-slurm-example"
    / "dagster_slurm_example"
    / "resources"
    / "__init__.py"
)
TEST_AUTHENTIK_API_TOKEN = "__TEST_ONLY_AUTHENTIK_API_TOKEN__"
TEST_BOOTSTRAP_API_TOKEN = "__TEST_ONLY_BOOTSTRAP_API_TOKEN__"
TEST_ROTATED_API_TOKEN = "__TEST_ONLY_ROTATED_API_TOKEN__"
TEST_ROTATED_API_TOKEN_ID = "__TEST_ONLY_ROTATED_API_TOKEN_ID__"
TEST_APP_CREDENTIAL = "__TEST_ONLY_APP_CREDENTIAL__"
TEST_STALE_APP_CREDENTIAL = "__TEST_ONLY_STALE_APP_CREDENTIAL__"
TEST_ROTATED_APP_CREDENTIAL = "__TEST_ONLY_ROTATED_APP_CREDENTIAL__"
TEST_ROTATED_APP_CREDENTIAL_ID = "__TEST_ONLY_ROTATED_APP_CREDENTIAL_ID__"
TEST_BEARER_TOKEN = "__TEST_ONLY_BEARER_TOKEN__"
TEST_STORED_API_TOKEN_ID = "__TEST_ONLY_STORED_API_TOKEN_ID__"
TEST_STORED_APP_CREDENTIAL_ID = "__TEST_ONLY_STORED_APP_CREDENTIAL_ID__"


def _load_example_resources_module():
    spec = importlib.util.spec_from_file_location(
        "dagster_slurm_example_resources_test",
        EXAMPLE_RESOURCES_PATH,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _http_error(code: int, body: bytes = b"") -> HTTPError:
    return HTTPError(
        url="https://auth.asc.ac.at/application/o/token/",
        code=code,
        msg="failure",
        hdrs=Message(),
        fp=BytesIO(body),
    )


def test_provider_rotates_when_app_password_missing(monkeypatch):
    provider = StepOIDCAuthProvider(
        token_url="https://auth.asc.ac.at/application/o/token/",
        client_id="client-id",
        username="xy12345",
        app_password=None,
        authentik_api_base="https://auth.asc.ac.at",
        authentik_api_token=TEST_AUTHENTIK_API_TOKEN,
    )
    calls: list[str] = []

    monkeypatch.setattr(provider, "_read_cert_valid_until", lambda: None)
    monkeypatch.setattr(
        provider,
        "rotate_authentik_credentials",
        lambda: (
            calls.append("rotate")
            or provider._apply_credentials(
                AuthentikCredentials(
                    api_token=TEST_ROTATED_API_TOKEN,
                    app_password=TEST_ROTATED_APP_CREDENTIAL,
                )
            )
            or AuthentikCredentials(
                api_token=TEST_ROTATED_API_TOKEN,
                app_password=TEST_ROTATED_APP_CREDENTIAL,
            )
        ),
    )
    monkeypatch.setattr(
        provider,
        "_fetch_bearer_token_with_password",
        lambda password: calls.append(f"token:{password}") or TEST_BEARER_TOKEN,
    )
    monkeypatch.setattr(
        provider, "_bootstrap_step_ca", lambda: calls.append("bootstrap")
    )
    monkeypatch.setattr(
        provider, "_step_login", lambda token: calls.append(f"step:{token}")
    )

    provider.ensure()

    assert provider.app_password == TEST_ROTATED_APP_CREDENTIAL
    assert calls == [
        "rotate",
        f"token:{TEST_ROTATED_APP_CREDENTIAL}",
        f"step:{TEST_BEARER_TOKEN}",
    ]


def test_provider_rotates_after_invalid_app_password(monkeypatch):
    provider = StepOIDCAuthProvider(
        token_url="https://auth.asc.ac.at/application/o/token/",
        client_id="client-id",
        username="xy12345",
        app_password=TEST_STALE_APP_CREDENTIAL,
        authentik_api_base="https://auth.asc.ac.at",
        authentik_api_token=TEST_AUTHENTIK_API_TOKEN,
    )
    attempts: list[str] = []

    def fake_fetch(password: str) -> str:
        attempts.append(password)
        if password == TEST_STALE_APP_CREDENTIAL:
            raise _http_error(400, b'{"error":"invalid_grant"}')
        return TEST_BEARER_TOKEN

    monkeypatch.setattr(provider, "_read_cert_valid_until", lambda: None)
    monkeypatch.setattr(provider, "_fetch_bearer_token_with_password", fake_fetch)
    monkeypatch.setattr(
        provider,
        "rotate_authentik_credentials",
        lambda: (
            provider._apply_credentials(
                AuthentikCredentials(
                    api_token=TEST_ROTATED_API_TOKEN,
                    app_password=TEST_ROTATED_APP_CREDENTIAL,
                )
            )
            or AuthentikCredentials(
                api_token=TEST_ROTATED_API_TOKEN,
                app_password=TEST_ROTATED_APP_CREDENTIAL,
            )
        ),
    )
    monkeypatch.setattr(
        provider, "_step_login", lambda token: attempts.append(f"step:{token}")
    )

    provider.ensure()

    assert provider.app_password == TEST_ROTATED_APP_CREDENTIAL
    assert attempts == [
        TEST_STALE_APP_CREDENTIAL,
        TEST_ROTATED_APP_CREDENTIAL,
        f"step:{TEST_BEARER_TOKEN}",
    ]


def test_authentik_credential_store_round_trip(tmp_path):
    store = AuthentikCredentialStore(tmp_path / "musica-auth.env")
    credentials = AuthentikCredentials(
        api_token=TEST_AUTHENTIK_API_TOKEN,
        api_token_id=TEST_STORED_API_TOKEN_ID,
        app_password=TEST_APP_CREDENTIAL,
        app_password_id=TEST_STORED_APP_CREDENTIAL_ID,
    )

    store.save(credentials)
    loaded = store.load()

    assert loaded.api_token == TEST_AUTHENTIK_API_TOKEN
    assert loaded.api_token_id == TEST_STORED_API_TOKEN_ID
    assert loaded.app_password == TEST_APP_CREDENTIAL
    assert loaded.app_password_id == TEST_STORED_APP_CREDENTIAL_ID


def test_provider_bootstraps_credentials_file_when_missing(monkeypatch, tmp_path):
    credentials_path = tmp_path / "musica-auth.env"
    provider = StepOIDCAuthProvider(
        token_url="https://auth.asc.ac.at/application/o/token/",
        client_id="client-id",
        username="xy12345",
        app_password=None,
        authentik_api_base="https://auth.asc.ac.at",
        authentik_api_token=TEST_BOOTSTRAP_API_TOKEN,
        authentik_credentials_file=str(credentials_path),
    )

    class FakeRotator:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def rotate(self):
            return AuthentikCredentials(
                api_token=TEST_ROTATED_API_TOKEN,
                api_token_id=TEST_ROTATED_API_TOKEN_ID,
                app_password=TEST_ROTATED_APP_CREDENTIAL,
                app_password_id=TEST_ROTATED_APP_CREDENTIAL_ID,
            )

        def cleanup_previous(self, credentials):
            return None

    monkeypatch.setattr(
        "dagster_slurm.auth.step_oidc.AuthentikCredentialRotator",
        FakeRotator,
    )
    monkeypatch.setattr(
        provider,
        "_read_cert_valid_until",
        lambda: datetime.now(timezone.utc) + timedelta(hours=4),
    )

    provider.ensure()

    stored = AuthentikCredentialStore(credentials_path).load()
    assert stored.api_token == TEST_ROTATED_API_TOKEN
    assert stored.app_password == TEST_ROTATED_APP_CREDENTIAL
    assert provider.authentik_api_token == TEST_ROTATED_API_TOKEN
    assert provider.app_password == TEST_ROTATED_APP_CREDENTIAL


def test_step_login_sanitizes_known_provisioner_failure(monkeypatch):
    provider = StepOIDCAuthProvider(
        token_url="https://auth.asc.ac.at/application/o/token/",
        client_id="client-id",
        username="xy12345",
        app_password=TEST_APP_CREDENTIAL,
    )

    def fail(*args, **kwargs):
        raise subprocess.CalledProcessError(
            1,
            ["step", "ssh", "login", "--token", "secret-token"],
            output="",
            stderr=(
                'provisioner "auth.asc.ac.at" is disabled due to an initialization error'
            ),
        )

    monkeypatch.setattr(subprocess, "run", fail)

    try:
        provider._step_login("secret-token")
    except RuntimeError as exc:
        message = str(exc)
    else:
        raise AssertionError("expected _step_login to fail")

    assert "remote OIDC provisioner appears uninitialized" in message
    assert "credentials file was still updated successfully" in message
    assert "secret-token" not in message


def test_step_login_sanitizes_generic_failure(monkeypatch):
    provider = StepOIDCAuthProvider(
        token_url="https://auth.asc.ac.at/application/o/token/",
        client_id="client-id",
        username="xy12345",
        app_password=TEST_APP_CREDENTIAL,
    )

    def fail(*args, **kwargs):
        raise subprocess.CalledProcessError(
            1,
            ["step", "ssh", "login", "--token", "secret-token"],
            output="stdout details",
            stderr="stderr details",
        )

    monkeypatch.setattr(subprocess, "run", fail)

    try:
        provider._step_login("secret-token")
    except RuntimeError as exc:
        message = str(exc)
    else:
        raise AssertionError("expected _step_login to fail")

    assert "stdout details" in message
    assert "stderr details" in message
    assert "secret-token" not in message


def test_step_login_signs_existing_ssh_key_pair(monkeypatch, tmp_path):
    provider = StepOIDCAuthProvider(
        token_url="https://auth.asc.ac.at/application/o/token/",
        client_id="client-id",
        username="xy12345",
        app_password="app-password",
        ssh_key_path=str(tmp_path / "id_musica"),
    )
    private_key_path = tmp_path / "id_musica"
    public_key_path = tmp_path / "id_musica.pub"
    private_key_path.write_text("private-key")
    public_key_path.write_text("public-key\n")

    commands: list[list[str]] = []

    def fake_run(cmd, **kwargs):
        commands.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(subprocess, "run", fake_run)

    provider._step_login("secret-token")

    assert commands == [
        [
            "step",
            "ssh",
            "certificate",
            "xy12345",
            str(public_key_path),
            "--sign",
            "--private-key",
            str(private_key_path),
            "--token",
            "secret-token",
            "--context",
            "asc",
            "--force",
        ]
    ]


def test_musica_resources_accept_credentials_file_without_env_token(
    monkeypatch, tmp_path
):
    resources_module = _load_example_resources_module()
    ssh_key_path = tmp_path / "id_musica"
    credentials_path = tmp_path / "musica-auth.env"
    ssh_key_path.write_text("test-key")
    AuthentikCredentialStore(credentials_path).save(
        AuthentikCredentials(
            app_password=TEST_APP_CREDENTIAL,
            app_password_id=TEST_STORED_APP_CREDENTIAL_ID,
        )
    )

    monkeypatch.setenv("DAGSTER_DEPLOYMENT", "staging_supercomputer")
    monkeypatch.setenv("SLURM_SUPERCOMPUTER_SITE", "musica")
    monkeypatch.setenv("SLURM_EDGE_NODE_HOST", "musica.vie.asc.ac.at")
    monkeypatch.setenv("SLURM_EDGE_NODE_USER", "xy12345")
    monkeypatch.setenv("SLURM_EDGE_NODE_KEY_PATH", str(ssh_key_path))
    monkeypatch.setenv("ASC_OIDC_CLIENT_ID", "client-id")
    monkeypatch.setenv("ASC_OIDC_USERNAME", "xy12345")
    monkeypatch.setenv("ASC_AUTHENTIK_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.delenv("ASC_OIDC_APP_PASSWORD", raising=False)
    monkeypatch.delenv("ASC_AUTHENTIK_API_TOKEN", raising=False)

    resources = resources_module.get_resources()
    auth_provider = resources["compute"].slurm._auth_provider

    assert isinstance(auth_provider, StepOIDCAuthProvider)
    assert auth_provider.app_password is None
    assert auth_provider._current_credentials().app_password == TEST_APP_CREDENTIAL
    assert auth_provider.authentik_credentials_file == str(credentials_path)
    assert auth_provider.ssh_key_path == str(ssh_key_path)


def test_musica_resources_accept_existing_cert_without_oidc_bootstrap(
    monkeypatch, tmp_path
):
    resources_module = _load_example_resources_module()
    ssh_key_path = tmp_path / "id_musica"
    ssh_key_path.write_text("test-key")

    monkeypatch.setenv("DAGSTER_DEPLOYMENT", "staging_supercomputer")
    monkeypatch.setenv("SLURM_SUPERCOMPUTER_SITE", "musica")
    monkeypatch.setenv("SLURM_EDGE_NODE_HOST", "musica.vie.asc.ac.at")
    monkeypatch.setenv("SLURM_EDGE_NODE_USER", "xy12345")
    monkeypatch.setenv("SLURM_EDGE_NODE_KEY_PATH", str(ssh_key_path))
    monkeypatch.delenv("ASC_OIDC_CLIENT_ID", raising=False)
    monkeypatch.delenv("ASC_OIDC_USERNAME", raising=False)
    monkeypatch.delenv("ASC_OIDC_APP_PASSWORD", raising=False)
    monkeypatch.delenv("ASC_AUTHENTIK_API_BASE", raising=False)
    monkeypatch.delenv("ASC_AUTHENTIK_API_TOKEN", raising=False)
    monkeypatch.delenv("ASC_AUTHENTIK_CREDENTIALS_FILE", raising=False)
    monkeypatch.setattr(
        StepOIDCAuthProvider,
        "_read_cert_valid_until",
        lambda self: datetime.now(timezone.utc) + timedelta(hours=4),
    )

    resources = resources_module.get_resources()

    assert resources["compute"].slurm._auth_provider is None


def test_musica_resources_raise_clear_error_without_cert_or_bootstrap(
    monkeypatch, tmp_path
):
    resources_module = _load_example_resources_module()
    ssh_key_path = tmp_path / "id_musica"
    ssh_key_path.write_text("test-key")

    monkeypatch.setenv("DAGSTER_DEPLOYMENT", "staging_supercomputer")
    monkeypatch.setenv("SLURM_SUPERCOMPUTER_SITE", "musica")
    monkeypatch.setenv("SLURM_EDGE_NODE_HOST", "musica.vie.asc.ac.at")
    monkeypatch.setenv("SLURM_EDGE_NODE_USER", "xy12345")
    monkeypatch.setenv("SLURM_EDGE_NODE_KEY_PATH", str(ssh_key_path))
    monkeypatch.delenv("ASC_OIDC_CLIENT_ID", raising=False)
    monkeypatch.delenv("ASC_OIDC_USERNAME", raising=False)
    monkeypatch.delenv("ASC_OIDC_APP_PASSWORD", raising=False)
    monkeypatch.delenv("ASC_AUTHENTIK_API_BASE", raising=False)
    monkeypatch.delenv("ASC_AUTHENTIK_API_TOKEN", raising=False)
    monkeypatch.delenv("ASC_AUTHENTIK_CREDENTIALS_FILE", raising=False)

    try:
        resources_module.get_resources()
    except RuntimeError as exc:
        message = str(exc)
    else:
        raise AssertionError("expected MUSICA bootstrap validation to fail")

    assert "MUSICA authentication is not bootstrapped" in message
    assert "ASC_AUTHENTIK_CREDENTIALS_FILE containing current credentials" in message
