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
    SlurmRunConfig,
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


def test_compute_env_cache_key_hashes_inject_files(tmp_path: Path):
    """Cache key should change when inject file contents change."""
    lockfile = tmp_path / "pixi.lock"
    lockfile.write_text("lockfile content")

    inject_file = tmp_path / "my_package.whl"
    inject_file.write_text("package v1")

    pack_cmd = ["pixi-pack", "--inject", str(inject_file)]
    key1 = compute_env_cache_key(pack_cmd, lockfile=lockfile)

    # Change inject file content
    inject_file.write_text("package v2")
    key2 = compute_env_cache_key(pack_cmd, lockfile=lockfile)

    assert key1 is not None
    assert key2 is not None
    assert key1 != key2, "Cache key should change when inject file contents change"


def test_compute_env_cache_key_with_cache_inject_globs(tmp_path: Path):
    """cache_inject_globs should filter which inject files affect the cache key."""
    lockfile = tmp_path / "pixi.lock"
    lockfile.write_text("lockfile content")

    # Base package (should affect cache)
    base_pkg = tmp_path / "base_pkg.whl"
    base_pkg.write_text("base v1")

    # Workload package (should NOT affect cache when excluded)
    workload_pkg = tmp_path / "workload_pkg.whl"
    workload_pkg.write_text("workload v1")

    pack_cmd = [
        "pixi-pack",
        "--inject",
        str(base_pkg),
        "--inject",
        str(workload_pkg),
    ]

    # Only include base_pkg in cache key
    cache_inject_globs = [str(base_pkg)]

    key1 = compute_env_cache_key(
        pack_cmd, lockfile=lockfile, cache_inject_globs=cache_inject_globs
    )

    # Change workload_pkg - should NOT affect cache key
    workload_pkg.write_text("workload v2")
    key2 = compute_env_cache_key(
        pack_cmd, lockfile=lockfile, cache_inject_globs=cache_inject_globs
    )

    assert key1 == key2, "Workload package change should not affect cache key"

    # Change base_pkg - SHOULD affect cache key
    base_pkg.write_text("base v2")
    key3 = compute_env_cache_key(
        pack_cmd, lockfile=lockfile, cache_inject_globs=cache_inject_globs
    )

    assert key1 != key3, "Base package change should affect cache key"


def test_compute_env_cache_key_extracts_inject_args(tmp_path: Path):
    """Test that --inject arguments are correctly extracted from pack command."""
    lockfile = tmp_path / "pixi.lock"
    lockfile.write_text("lockfile content")

    inject1 = tmp_path / "pkg1.whl"
    inject1.write_text("pkg1")
    inject2 = tmp_path / "pkg2.conda"
    inject2.write_text("pkg2")

    # Test both --inject forms
    pack_cmd = [
        "pixi-pack",
        "--environment",
        "myenv",
        "--inject",
        str(inject1),
        f"--inject={inject2}",
        "--create-executable",
    ]

    key = compute_env_cache_key(pack_cmd, lockfile=lockfile)
    assert key is not None

    # Change one inject file
    inject1.write_text("pkg1 modified")
    key_changed = compute_env_cache_key(pack_cmd, lockfile=lockfile)

    assert key != key_changed, "Key should change when inject file changes"


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
            self.selected_asset_keys = {self.asset_key}
            self.assets_def = SimpleNamespace(
                metadata_by_key={self.asset_key: {"force_slurm_env_push": force}}
            )

        def has_assets_def(self):
            return True

    assert resource._should_force_env_push(DummyContext(True), None) is True
    assert resource._should_force_env_push(DummyContext(False), None) is False


class DummySlurmClient(SlurmPipesClient):
    def __init__(self):
        # Skip parent init; we only need isinstance checks to pass.
        self.kwargs = None

    def run(  # type: ignore[override]
        self,
        context,
        *,
        payload_path: str,
        extra_env=None,
        extras=None,
        use_session=False,
        extra_slurm_opts=None,
        force_env_push=None,
        skip_payload_upload=None,
        remote_payload_path=None,
        pack_cmd_override=None,
        pre_deployed_env_path_override=None,
        **kwargs,
    ):
        captured = dict(kwargs)
        captured["pack_cmd_override"] = pack_cmd_override
        captured["pre_deployed_env_path_override"] = pre_deployed_env_path_override
        captured["force_env_push"] = force_env_push
        captured["skip_payload_upload"] = skip_payload_upload
        captured["remote_payload_path"] = remote_payload_path
        self.kwargs = captured
        # Return a shape similar to real client
        return SimpleNamespace()


def test_pack_cmd_override_from_metadata(monkeypatch):
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
        def __init__(self):
            self.asset_key = AssetKey("demo")
            self.selected_asset_keys = {self.asset_key}
            self.assets_def = SimpleNamespace(
                metadata_by_key={
                    self.asset_key: {
                        "slurm_pack_cmd": ["pixi", "run", "--frozen", "pack-ml"]
                    }
                }
            )

        def has_assets_def(self):
            return True

    fake_client = DummySlurmClient()
    monkeypatch.setattr(
        ComputeResource,
        "get_pipes_client",
        lambda self, context, launcher=None: fake_client,
    )

    resource.run(context=DummyContext(), payload_path="script.py")

    assert fake_client.kwargs is not None
    assert fake_client.kwargs["pack_cmd_override"] == [
        "pixi",
        "run",
        "--frozen",
        "pack-ml",
    ]


def test_predeployed_env_override_from_metadata(monkeypatch):
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
        def __init__(self):
            self.asset_key = AssetKey("demo")
            self.selected_asset_keys = {self.asset_key}
            self.assets_def = SimpleNamespace(
                metadata_by_key={
                    self.asset_key: {"slurm_pre_deployed_env_path": "/prebuilt/envs/ml"}
                }
            )

        def has_assets_def(self):
            return True

    fake_client = DummySlurmClient()
    monkeypatch.setattr(
        ComputeResource,
        "get_pipes_client",
        lambda self, context, launcher=None: fake_client,
    )

    resource.run(context=DummyContext(), payload_path="script.py")

    assert fake_client.kwargs is not None
    assert fake_client.kwargs["pre_deployed_env_path_override"] == "/prebuilt/envs/ml"


def test_slurm_run_config_sets_force_env_push(monkeypatch):
    """SlurmRunConfig should set force_env_push when passed to compute.run()."""
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
        def __init__(self):
            self.asset_key = AssetKey("demo")
            self.assets_def = SimpleNamespace(metadata_by_key={})

        def has_assets_def(self):
            return True

    fake_client = DummySlurmClient()
    monkeypatch.setattr(
        ComputeResource,
        "get_pipes_client",
        lambda self, context, launcher=None: fake_client,
    )

    # Test with force_env_push=True in config
    config = SlurmRunConfig(force_env_push=True)
    resource.run(context=DummyContext(), payload_path="script.py", config=config)

    assert fake_client.kwargs is not None
    assert fake_client.kwargs["force_env_push"] is True


def test_slurm_run_config_sets_skip_payload_upload(monkeypatch):
    """SlurmRunConfig should set skip_payload_upload when passed to compute.run()."""
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
        def __init__(self):
            self.asset_key = AssetKey("demo")
            self.assets_def = SimpleNamespace(metadata_by_key={})

        def has_assets_def(self):
            return True

    fake_client = DummySlurmClient()
    monkeypatch.setattr(
        ComputeResource,
        "get_pipes_client",
        lambda self, context, launcher=None: fake_client,
    )

    # Test with skip_payload_upload=True and remote_payload_path in config
    config = SlurmRunConfig(
        skip_payload_upload=True, remote_payload_path="/remote/script.py"
    )
    resource.run(context=DummyContext(), payload_path="script.py", config=config)

    assert fake_client.kwargs is not None
    assert fake_client.kwargs["skip_payload_upload"] is True
    assert fake_client.kwargs["remote_payload_path"] == "/remote/script.py"


def test_explicit_params_override_config(monkeypatch):
    """Explicit parameters should override SlurmRunConfig values."""
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
        def __init__(self):
            self.asset_key = AssetKey("demo")
            self.assets_def = SimpleNamespace(metadata_by_key={})

        def has_assets_def(self):
            return True

    fake_client = DummySlurmClient()
    monkeypatch.setattr(
        ComputeResource,
        "get_pipes_client",
        lambda self, context, launcher=None: fake_client,
    )

    # Config says force=False, but explicit param says force=True
    config = SlurmRunConfig(force_env_push=False)
    resource.run(
        context=DummyContext(),
        payload_path="script.py",
        config=config,
        force_env_push=True,  # Explicit param should win
    )

    assert fake_client.kwargs is not None
    assert fake_client.kwargs["force_env_push"] is True
