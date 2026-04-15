from __future__ import annotations

import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).resolve().parents[3]
    / "examples"
    / "scripts"
    / "refresh_step_auth.py"
)
TEST_ROTATED_API_TOKEN = "__TEST_ONLY_ROTATED_API_TOKEN__"
TEST_ROTATED_APP_CREDENTIAL = "__TEST_ONLY_ROTATED_APP_CREDENTIAL__"


def _load_refresh_step_auth_module():
    spec = importlib.util.spec_from_file_location("refresh_step_auth_test", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_rotate_skips_step_refresh_by_default(monkeypatch, capsys):
    module = _load_refresh_step_auth_module()
    calls: dict[str, object] = {}

    class FakeProvider:
        def __init__(self, **kwargs):
            calls["app_password"] = kwargs["app_password"]
            calls["credentials_file"] = kwargs["authentik_credentials_file"]
            calls["ssh_key_path"] = kwargs["ssh_key_path"]

        def rotate_authentik_credentials(self):
            calls["rotated"] = True
            return module.AuthentikCredentials(
                api_token=TEST_ROTATED_API_TOKEN,
                app_password=TEST_ROTATED_APP_CREDENTIAL,
            )

        def _read_cert_valid_until(self):
            return None

        def ensure(self) -> None:
            calls["ensure_called"] = True

    monkeypatch.setenv("ASC_OIDC_CLIENT_ID", "client-id")
    monkeypatch.setenv("ASC_OIDC_USERNAME", "xy12345")
    monkeypatch.setenv("ASC_AUTHENTIK_CREDENTIALS_FILE", "/tmp/musica-auth.env")
    monkeypatch.setenv("SLURM_EDGE_NODE_KEY_PATH", "~/.ssh/id_musica")
    monkeypatch.delenv("ASC_OIDC_APP_PASSWORD", raising=False)
    monkeypatch.setattr(module, "StepOIDCAuthProvider", FakeProvider)
    monkeypatch.setattr(
        sys,
        "argv",
        ["refresh_step_auth.py", "--rotate-credentials"],
    )

    module.main()

    assert calls == {
        "app_password": None,
        "credentials_file": "/tmp/musica-auth.env",
        "ssh_key_path": "~/.ssh/id_musica",
        "rotated": True,
    }
    assert "Skipping Step certificate refresh. Pass --refresh-step-after-rotate" in (
        capsys.readouterr().out
    )


def test_rotate_refreshes_step_when_explicitly_requested(monkeypatch, capsys):
    module = _load_refresh_step_auth_module()
    calls: dict[str, object] = {}
    app_passwords: list[object | None] = []
    refreshed_at = datetime(2026, 4, 17, 6, 39, 18, tzinfo=timezone.utc)

    class FakeProvider:
        def __init__(self, **kwargs):
            app_passwords.append(kwargs["app_password"])
            calls["app_passwords"] = app_passwords
            calls["credentials_file"] = kwargs["authentik_credentials_file"]
            calls["ssh_key_path"] = kwargs["ssh_key_path"]
            self._cert_valid_until = None

        def rotate_authentik_credentials(self):
            calls["rotated"] = True
            return module.AuthentikCredentials(
                api_token=TEST_ROTATED_API_TOKEN,
                app_password=TEST_ROTATED_APP_CREDENTIAL,
            )

        def _read_cert_valid_until(self):
            return self._cert_valid_until

        def ensure(self) -> None:
            calls["ensure_called"] = True
            self._cert_valid_until = refreshed_at

    monkeypatch.setenv("ASC_OIDC_CLIENT_ID", "client-id")
    monkeypatch.setenv("ASC_OIDC_USERNAME", "xy12345")
    monkeypatch.setenv("ASC_AUTHENTIK_CREDENTIALS_FILE", "/tmp/musica-auth.env")
    monkeypatch.setenv("SLURM_EDGE_NODE_KEY_PATH", "~/.ssh/id_musica")
    monkeypatch.delenv("ASC_OIDC_APP_PASSWORD", raising=False)
    monkeypatch.setattr(module, "StepOIDCAuthProvider", FakeProvider)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "refresh_step_auth.py",
            "--rotate-credentials",
            "--refresh-step-after-rotate",
        ],
    )

    module.main()

    assert calls == {
        "app_passwords": [None],
        "credentials_file": "/tmp/musica-auth.env",
        "ssh_key_path": "~/.ssh/id_musica",
        "rotated": True,
        "ensure_called": True,
    }
    assert "Used the newly rotated Authentik credentials for Step/OIDC refresh." in (
        capsys.readouterr().out
    )


def test_rotate_reports_when_step_certificate_refresh_is_not_needed(
    monkeypatch, capsys
):
    module = _load_refresh_step_auth_module()
    calls: dict[str, object] = {}
    app_passwords: list[object | None] = []
    valid_until = datetime(2026, 4, 17, 6, 39, 18, tzinfo=timezone.utc)

    class FakeProvider:
        def __init__(self, **kwargs):
            app_passwords.append(kwargs["app_password"])
            calls["app_passwords"] = app_passwords
            calls["credentials_file"] = kwargs["authentik_credentials_file"]
            calls["ssh_key_path"] = kwargs["ssh_key_path"]

        def rotate_authentik_credentials(self):
            calls["rotated"] = True
            return module.AuthentikCredentials(
                api_token=TEST_ROTATED_API_TOKEN,
                app_password=TEST_ROTATED_APP_CREDENTIAL,
            )

        def _read_cert_valid_until(self):
            return valid_until

        def ensure(self) -> None:
            calls["ensure_called"] = True

    monkeypatch.setenv("ASC_OIDC_CLIENT_ID", "client-id")
    monkeypatch.setenv("ASC_OIDC_USERNAME", "xy12345")
    monkeypatch.setenv("ASC_AUTHENTIK_CREDENTIALS_FILE", "/tmp/musica-auth.env")
    monkeypatch.setenv("SLURM_EDGE_NODE_KEY_PATH", "~/.ssh/id_musica")
    monkeypatch.delenv("ASC_OIDC_APP_PASSWORD", raising=False)
    monkeypatch.setattr(module, "StepOIDCAuthProvider", FakeProvider)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "refresh_step_auth.py",
            "--rotate-credentials",
            "--refresh-step-after-rotate",
        ],
    )

    module.main()

    assert calls == {
        "app_passwords": [None],
        "credentials_file": "/tmp/musica-auth.env",
        "ssh_key_path": "~/.ssh/id_musica",
        "rotated": True,
        "ensure_called": True,
    }
    assert "Existing Step SSH certificate is still valid" in (capsys.readouterr().out)


def test_rotate_can_skip_step_refresh_without_oidc_credentials(monkeypatch, capsys):
    module = _load_refresh_step_auth_module()

    monkeypatch.delenv("ASC_OIDC_CLIENT_ID", raising=False)
    monkeypatch.delenv("ASC_OIDC_USERNAME", raising=False)
    monkeypatch.delenv("ASC_OIDC_APP_PASSWORD", raising=False)
    monkeypatch.setattr(
        module,
        "_rotate_credentials_without_provider",
        lambda: module.AuthentikCredentials(
            api_token=TEST_ROTATED_API_TOKEN,
            app_password=TEST_ROTATED_APP_CREDENTIAL,
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["refresh_step_auth.py", "--rotate-credentials"],
    )

    module.main()

    assert "Skipping Step certificate refresh. Pass --refresh-step-after-rotate" in (
        capsys.readouterr().out
    )


def test_resolve_cert_path_prefers_matching_ssh_key_pair(monkeypatch, tmp_path):
    module = _load_refresh_step_auth_module()
    ssh_key_path = tmp_path / "id_musica"
    ssh_cert_path = tmp_path / "id_musica-cert.pub"
    ssh_key_path.write_text("test-key")
    ssh_cert_path.write_text("test-cert")

    monkeypatch.delenv("ASC_STEP_CERT_PATH", raising=False)
    monkeypatch.setenv("SLURM_EDGE_NODE_KEY_PATH", str(ssh_key_path))

    assert module._resolve_cert_path("asc") == str(ssh_cert_path)
