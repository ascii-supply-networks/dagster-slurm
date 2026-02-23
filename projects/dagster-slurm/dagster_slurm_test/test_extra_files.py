"""Tests for extra_files feature."""

from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

from dagster_slurm.launchers import BashLauncher
from dagster_slurm.config.runtime import RuntimeVariant
from dagster_slurm.launchers.base import ComputeLauncher, ExecutionPlan
from dagster_slurm.resources.compute import ComputeResource


def _make_compute(default_extra_files=None):
    """Create a minimal local-mode ComputeResource."""
    return ComputeResource(
        mode="local",
        default_launcher=BashLauncher(),
        default_extra_files=default_extra_files,
    )


def _mock_context(metadata_by_key=None):
    """Create a mock Dagster context with optional metadata."""
    ctx = MagicMock()
    if metadata_by_key is not None:
        ctx.has_assets_def.return_value = True
        ctx.assets_def.metadata_by_key = metadata_by_key
        # Set up asset keys based on metadata_by_key keys
        keys = list(metadata_by_key.keys())
        ctx.selected_asset_keys = set(keys)
    else:
        ctx.has_assets_def.return_value = False
    return ctx


# ── Unit tests: _resolve_extra_files ──


def test_resolve_extra_files_global_only(tmp_path):
    """Only default_extra_files are set."""
    f1 = tmp_path / "config.toml"
    f1.write_text("key = 'value'")

    compute = _make_compute(default_extra_files=[str(f1)])
    ctx = _mock_context()

    result = compute._resolve_extra_files(
        context=ctx, explicit_files=None, config_files=None
    )
    assert result is not None
    assert len(result) == 1
    assert result[0] == str(f1.resolve())


def test_resolve_extra_files_merge_all_sources(tmp_path):
    """All four sources contribute files, verify additive merge + dedup."""
    f_global = tmp_path / "global.toml"
    f_global.write_text("")
    f_meta = tmp_path / "meta.toml"
    f_meta.write_text("")
    f_config = tmp_path / "config.toml"
    f_config.write_text("")
    f_explicit = tmp_path / "explicit.toml"
    f_explicit.write_text("")

    asset_key = MagicMock()
    metadata_by_key = {
        asset_key: {"slurm_extra_files": [str(f_meta)]},
    }

    compute = _make_compute(default_extra_files=[str(f_global)])
    ctx = _mock_context(metadata_by_key)

    result = compute._resolve_extra_files(
        context=ctx,
        explicit_files=[str(f_explicit)],
        config_files=[str(f_config)],
    )
    assert result is not None
    assert len(result) == 4
    # Verify order: global, meta, config, explicit
    assert result[0] == str(f_global.resolve())
    assert result[1] == str(f_meta.resolve())
    assert result[2] == str(f_config.resolve())
    assert result[3] == str(f_explicit.resolve())


def test_resolve_extra_files_dedup(tmp_path):
    """Same file from multiple sources -> single entry."""
    f1 = tmp_path / "shared.toml"
    f1.write_text("")

    compute = _make_compute(default_extra_files=[str(f1)])
    ctx = _mock_context()

    result = compute._resolve_extra_files(
        context=ctx,
        explicit_files=[str(f1)],
        config_files=[str(f1)],
    )
    assert result is not None
    assert len(result) == 1


def test_resolve_extra_files_basename_collision_raises(tmp_path):
    """Two different files with same basename -> ValueError."""
    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()
    (dir_a / "config.toml").write_text("a")
    (dir_b / "config.toml").write_text("b")

    compute = _make_compute(default_extra_files=[str(dir_a / "config.toml")])
    ctx = _mock_context()

    with pytest.raises(ValueError, match="basename collision"):
        compute._resolve_extra_files(
            context=ctx,
            explicit_files=[str(dir_b / "config.toml")],
            config_files=None,
        )


def test_resolve_extra_files_none_when_empty():
    """No files anywhere -> None."""
    compute = _make_compute()
    ctx = _mock_context()

    result = compute._resolve_extra_files(
        context=ctx, explicit_files=None, config_files=None
    )
    assert result is None


# ── Integration tests: local mode ──


class _CaptureLauncher(ComputeLauncher):
    """Test launcher to capture resolved env and working dir."""

    captured_extra_env: dict[str, str] | None = None
    captured_working_dir: str | None = None

    def prepare_execution(
        self,
        payload_path: str,
        python_executable: str,
        working_dir: str,
        pipes_context: dict[str, str],
        extra_env: dict[str, str] | None = None,
        allocation_context: dict[str, object] | None = None,
        activation_script: str | None = None,
    ) -> ExecutionPlan:
        del (
            payload_path,
            python_executable,
            pipes_context,
            allocation_context,
            activation_script,
        )
        self.captured_working_dir = working_dir
        self.captured_extra_env = extra_env
        return ExecutionPlan(
            kind=RuntimeVariant.SHELL,
            payload=["#!/bin/bash", "true"],
            environment={},
            resources={},
        )


def _mock_run_context(run_id: str = "test_run") -> MagicMock:
    ctx = MagicMock()
    ctx.run.run_id = run_id
    ctx.op_execution_context = MagicMock()
    return ctx


@contextmanager
def _fake_open_pipes_session(*args, **kwargs):
    class _DummySession:
        def get_bootstrap_env_vars(self):
            return {}

    yield _DummySession()


def test_local_extra_files_copied_and_metaxy_config_set(tmp_path, monkeypatch):
    """run() copies files and injects METAXY_CONFIG for local discoverability."""
    from dagster_slurm.pipes_clients import local_pipes_client as lpc

    extra = tmp_path / "metaxy.toml"
    extra.write_text("[store]\npath = '/data'")
    payload = tmp_path / "payload.py"
    payload.write_text("print('ok')\n")

    launcher = _CaptureLauncher()
    client = lpc.LocalPipesClient(
        launcher=launcher,
        base_dir=str(tmp_path / "runs"),
        require_pixi=False,
    )

    monkeypatch.setattr(lpc, "open_pipes_session", _fake_open_pipes_session)
    execute_spy = MagicMock(return_value=123)
    monkeypatch.setattr(client.runner, "execute_script", execute_spy)

    context = _mock_run_context()
    client.run(
        context=context,
        payload_path=str(payload),
        extra_files=[str(extra)],
    )

    copied = tmp_path / "runs" / "test_run" / "metaxy.toml"
    assert copied.exists()
    assert copied.read_text() == "[store]\npath = '/data'"
    assert launcher.captured_extra_env is not None
    assert launcher.captured_extra_env["METAXY_CONFIG"] == str(copied)
    execute_spy.assert_called_once()


def test_local_extra_files_respects_explicit_metaxy_config(tmp_path, monkeypatch):
    """run() does not override explicit METAXY_CONFIG from extra_env."""
    from dagster_slurm.pipes_clients import local_pipes_client as lpc

    extra = tmp_path / "metaxy.toml"
    extra.write_text("[store]\npath = '/data'")
    payload = tmp_path / "payload.py"
    payload.write_text("print('ok')\n")

    launcher = _CaptureLauncher()
    client = lpc.LocalPipesClient(
        launcher=launcher,
        base_dir=str(tmp_path / "runs"),
        require_pixi=False,
    )

    monkeypatch.setattr(lpc, "open_pipes_session", _fake_open_pipes_session)
    monkeypatch.setattr(client.runner, "execute_script", MagicMock(return_value=123))

    context = _mock_run_context()
    explicit = "/tmp/explicit/metaxy.toml"
    client.run(
        context=context,
        payload_path=str(payload),
        extra_files=[str(extra)],
        extra_env={"METAXY_CONFIG": explicit},
    )

    assert launcher.captured_extra_env is not None
    assert launcher.captured_extra_env["METAXY_CONFIG"] == explicit


def test_extra_files_missing_raises_from_local_client(tmp_path, monkeypatch):
    """run() raises FileNotFoundError when an extra file path does not exist."""
    from dagster_slurm.pipes_clients import local_pipes_client as lpc

    payload = tmp_path / "payload.py"
    payload.write_text("print('ok')\n")

    client = lpc.LocalPipesClient(
        launcher=_CaptureLauncher(),
        base_dir=str(tmp_path / "runs"),
        require_pixi=False,
    )

    monkeypatch.setattr(lpc, "open_pipes_session", _fake_open_pipes_session)
    execute_spy = MagicMock(return_value=123)
    monkeypatch.setattr(client.runner, "execute_script", execute_spy)

    context = _mock_run_context()
    with pytest.raises(FileNotFoundError, match="Extra file not found"):
        client.run(
            context=context,
            payload_path=str(payload),
            extra_files=["/nonexistent/path/config.toml"],
        )

    execute_spy.assert_not_called()
