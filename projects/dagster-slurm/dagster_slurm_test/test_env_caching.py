"""Environment cache behavior and payload handling tests."""

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from dagster import AssetKey

from dagster_slurm import (
    BashLauncher,
    ComputeResource,
    SSHConnectionResource,
    SlurmQueueConfig,
    SlurmResource,
)
from dagster_slurm.config.environment import ExecutionMode
from dagster_slurm.helpers.env_packaging import compute_env_cache_key
from dagster_slurm.pipes_clients.slurm_pipes_client import SlurmPipesClient


class FakePool:
    """Minimal SSH pool stub used for unit tests."""

    def __init__(self, payload_exists: bool = True):
        self.commands: list[str] = []
        self.uploads: list[tuple[str, str]] = []
        self.control_path = "/tmp/ctrl"
        self.payload_exists = payload_exists

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def run(self, cmd, timeout=None):
        self.commands.append(cmd)
        if cmd.startswith("test -f") and not self.payload_exists:
            raise RuntimeError("missing payload")
        return ""

    def upload_file(self, src, dest):
        self.uploads.append((src, dest))


@contextmanager
def open_session_stub(*_args, **_kwargs):
    yield SimpleNamespace(get_bootstrap_env_vars=lambda: {})


class MessageReaderStub:
    def __init__(self, *args, **kwargs):
        pass


class MetricsStub:
    def collect_job_metrics(self, *args, **kwargs):
        return SimpleNamespace(
            node_hours=0, cpu_efficiency=0, max_rss_mb=0, elapsed_seconds=0
        )


def configure_client_for_local_run(
    client: SlurmPipesClient,
    monkeypatch: pytest.MonkeyPatch,
    pool: FakePool,
    *,
    activation: str = "/remote/base/env-cache/cache/activate.sh",
    python: str = "/remote/base/env-cache/cache/env/bin/python",
    job_id: int = 123,
) -> None:
    """Patch a Slurm client to run locally without touching SSH."""
    monkeypatch.setattr(
        "dagster_slurm.pipes_clients.slurm_pipes_client.SSHConnectionPool",
        lambda *_args, **_kwargs: pool,
    )
    monkeypatch.setattr(
        "dagster_slurm.pipes_clients.slurm_pipes_client.SSHMessageReader",
        MessageReaderStub,
    )
    monkeypatch.setattr(
        "dagster_slurm.pipes_clients.slurm_pipes_client.open_pipes_session",
        open_session_stub,
    )
    monkeypatch.setattr(
        client, "_get_remote_base", lambda run_id, _pool: "/remote/base"
    )
    monkeypatch.setattr(
        client, "_prepare_environment", lambda **_kwargs: (activation, python)
    )
    monkeypatch.setattr(client, "_execute_standalone", lambda **_kwargs: job_id)
    monkeypatch.setattr(client, "metrics_collector", MetricsStub())


def make_context(run_id: str = "run123") -> Any:
    return cast(
        Any,
        SimpleNamespace(run=SimpleNamespace(run_id=run_id), op_execution_context=None),
    )


def test_compute_env_cache_key_changes_with_lock(tmp_path: Path):
    lockfile = tmp_path / "pixi.lock"
    lockfile.write_text("first")
    key1 = compute_env_cache_key(["pixi", "run", "pack"], lockfile=lockfile)

    lockfile.write_text("second")
    key2 = compute_env_cache_key(["pixi", "run", "pack"], lockfile=lockfile)

    assert key1 is not None
    assert key2 is not None
    assert key1 != key2


def test_prepare_environment_reuses_cached_env(monkeypatch):
    client = SlurmPipesClient(
        slurm_resource=cast(Any, SimpleNamespace(ssh=None, queue=None)),
        launcher=BashLauncher(),
    )
    pool = FakePool()

    monkeypatch.setattr(
        client, "_compute_environment_cache_key", lambda pack_cmd: "abc123"
    )
    monkeypatch.setattr(client, "_cached_env_ready", lambda **_: True)
    monkeypatch.setattr(
        "dagster_slurm.pipes_clients.slurm_pipes_client.pack_environment_with_pixi",
        lambda **_: (_ for _ in ()).throw(AssertionError("should not pack")),
    )

    activation, python_exec = client._prepare_environment(
        ssh_pool=cast(Any, pool),
        remote_base="/remote/base",
        run_dir="/remote/base/runs/run1",
        force_env_push=False,
    )

    assert activation == "/remote/base/env-cache/abc123/activate.sh"
    assert python_exec == "/remote/base/env-cache/abc123/env/bin/python"
    assert not pool.uploads
    assert pool.commands == []


def test_prepare_environment_force_pushes(monkeypatch, tmp_path: Path):
    client = SlurmPipesClient(
        slurm_resource=cast(Any, SimpleNamespace(ssh=None, queue=None)),
        launcher=BashLauncher(),
    )
    pool = FakePool()

    pack_path = tmp_path / "environment.sh"
    pack_path.write_text("#!/bin/bash\necho hi\n")

    monkeypatch.setattr(
        client, "_compute_environment_cache_key", lambda pack_cmd: "force123"
    )
    monkeypatch.setattr(
        client, "_extract_environment", lambda **kwargs: kwargs["pack_file_path"]
    )
    monkeypatch.setattr(
        "dagster_slurm.pipes_clients.slurm_pipes_client.pack_environment_with_pixi",
        lambda **kwargs: pack_path,
    )

    activation, python_exec = client._prepare_environment(
        ssh_pool=cast(Any, pool),
        remote_base="/remote/base",
        run_dir="/remote/base/runs/run1",
        force_env_push=True,
    )

    assert activation.endswith("/environment.sh")
    assert python_exec == "/remote/base/env-cache/force123/env/bin/python"
    assert pool.uploads == [
        (str(pack_path), "/remote/base/env-cache/force123/environment.sh")
    ]
    assert pool.commands == ["mkdir -p /remote/base/env-cache/force123/env"]


def test_run_uploads_payload_when_reusing_env(monkeypatch, tmp_path: Path):
    """Even when the environment is cached, the payload should be uploaded each run."""
    pool = FakePool()
    client = SlurmPipesClient(
        slurm_resource=cast(
            Any,
            SimpleNamespace(
                ssh="dummy",
                queue=SimpleNamespace(num_nodes=1),
                remote_base="/remote/base",
            ),
        ),
        launcher=BashLauncher(),
    )
    configure_client_for_local_run(client, monkeypatch, pool)

    payload = tmp_path / "payload.py"
    payload.write_text("print('hello')")

    client.run(context=make_context(), payload_path=str(payload))

    assert any(str(payload) == src for src, _ in pool.uploads)


def test_skip_payload_upload_with_remote_path(monkeypatch, tmp_path: Path):
    pool = FakePool(payload_exists=True)
    client = SlurmPipesClient(
        slurm_resource=cast(
            Any,
            SimpleNamespace(
                ssh="dummy",
                queue=SimpleNamespace(num_nodes=1),
                remote_base="/remote/base",
            ),
        ),
        launcher=BashLauncher(),
    )
    configure_client_for_local_run(client, monkeypatch, pool)

    payload = tmp_path / "payload.py"
    payload.write_text("print('hello')")

    client.run(
        context=make_context(),
        payload_path=str(payload),
        skip_payload_upload=True,
        remote_payload_path="/remote/base/shared/payload.py",
    )

    assert pool.uploads == []
    assert any(
        cmd.startswith("test -f /remote/base/shared/payload.py")
        for cmd in pool.commands
    )


def test_skip_payload_upload_missing_remote_raises(monkeypatch, tmp_path: Path):
    pool = FakePool(payload_exists=False)
    client = SlurmPipesClient(
        slurm_resource=cast(
            Any,
            SimpleNamespace(
                ssh="dummy",
                queue=SimpleNamespace(num_nodes=1),
                remote_base="/remote/base",
            ),
        ),
        launcher=BashLauncher(),
    )
    configure_client_for_local_run(client, monkeypatch, pool)

    payload = tmp_path / "payload.py"
    payload.write_text("print('hello')")

    with pytest.raises(RuntimeError, match="skip_payload_upload"):
        client.run(
            context=make_context(),
            payload_path=str(payload),
            skip_payload_upload=True,
            remote_payload_path="/remote/base/shared/payload.py",
        )


def test_force_env_push_read_from_metadata():
    slurm_resource = SlurmResource(
        ssh=SSHConnectionResource(
            host="localhost", port=2222, user="test", password="secret"
        ),
        queue=SlurmQueueConfig(),
        remote_base="/tmp/dagster_test",
    )

    resource = ComputeResource(
        mode=ExecutionMode.SLURM,
        slurm=slurm_resource,
        default_launcher=BashLauncher(),
    )

    class DummyContext:
        def __init__(self, force: bool):
            self.asset_key = AssetKey("demo")
            self.assets_def = SimpleNamespace(
                metadata_by_key={self.asset_key: {"force_slurm_env_push": force}}
            )

        def has_assets_def(self):
            return True

    assert resource._should_force_env_push(DummyContext(True), None) is True
    assert resource._should_force_env_push(DummyContext(False), None) is False
