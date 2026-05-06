"""Environment cache behavior and payload handling tests."""

import os
import shutil
import subprocess
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, cast

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
from dagster_slurm.pipes_clients.slurm_pipes_client import (
    SlurmPipesClient,
    _remote_join_under_root,
)


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
        if cmd == "uname -s":
            return "Linux\n"
        if cmd == "uname -m":
            return "x86_64\n"
        if cmd.startswith("test -f") and not self.payload_exists:
            raise RuntimeError("missing payload")
        return ""

    def upload_file(self, src, dest):
        self.uploads.append((src, dest))


class LocalShellPool(FakePool):
    """SSH pool test double that executes commands against the local filesystem."""

    def run(self, cmd, timeout=None):
        self.commands.append(cmd)
        result = subprocess.run(
            ["bash", "-c", cmd],
            capture_output=True,
            check=False,
            text=True,
            timeout=timeout,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr or result.stdout)
        return result.stdout

    def upload_file(self, src, dest):
        self.uploads.append((src, dest))
        destination = Path(dest)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, destination)


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


@pytest.fixture
def fake_pool() -> FakePool:
    return FakePool()


@pytest.fixture
def slurm_client_factory() -> Callable[..., SlurmPipesClient]:
    def factory(**kwargs) -> SlurmPipesClient:
        return SlurmPipesClient(
            slurm_resource=cast(Any, SimpleNamespace(ssh=None, queue=None)),
            launcher=BashLauncher(),
            **kwargs,
        )

    return factory


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


def test_compute_env_cache_key_changes_with_env_overrides(tmp_path: Path):
    """Platform overrides must affect the environment cache key."""
    lockfile = tmp_path / "pixi.lock"
    lockfile.write_text("lockfile content")

    pack_cmd = ["python", "scripts/pack_environment.py"]
    key_x86 = compute_env_cache_key(
        pack_cmd,
        lockfile=lockfile,
        env_overrides={"SLURM_PACK_PLATFORM": "linux-64"},
    )
    key_arm = compute_env_cache_key(
        pack_cmd,
        lockfile=lockfile,
        env_overrides={"SLURM_PACK_PLATFORM": "linux-aarch64"},
    )

    assert key_x86 is not None
    assert key_arm is not None
    assert key_x86 != key_arm


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


def test_prepare_environment_reuses_cached_env(
    monkeypatch: pytest.MonkeyPatch,
    fake_pool: FakePool,
    slurm_client_factory: Callable[..., SlurmPipesClient],
):
    client = slurm_client_factory()
    monkeypatch.setattr(
        client,
        "_compute_environment_cache_key",
        lambda pack_cmd, env_overrides=None: "abc123",
    )
    monkeypatch.setattr(client, "_cached_env_ready", lambda **_: True)
    monkeypatch.setattr(
        "dagster_slurm.pipes_clients.slurm_pipes_client.pack_environment_with_pixi",
        lambda **_: (_ for _ in ()).throw(AssertionError("should not pack")),
    )

    activation, python_exec = client._prepare_environment(
        ssh_pool=cast(Any, fake_pool),
        remote_base="/remote/base",
        run_dir="/remote/base/runs/run1",
        force_env_push=False,
    )

    assert activation == "/remote/base/env-cache/abc123/activate.sh"
    assert python_exec == "/remote/base/env-cache/abc123/env/bin/python"
    assert not fake_pool.uploads
    assert fake_pool.commands == ["uname -s", "uname -m"]


def test_prepare_environment_force_pushes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_pool: FakePool,
    slurm_client_factory: Callable[..., SlurmPipesClient],
):
    client = slurm_client_factory()
    pack_path = tmp_path / "environment.sh"
    pack_path.write_text("#!/bin/bash\necho hi\n")

    monkeypatch.setattr(
        client,
        "_compute_environment_cache_key",
        lambda pack_cmd, env_overrides=None: "force123",
    )
    monkeypatch.setattr(
        client, "_extract_environment", lambda **kwargs: kwargs["pack_file_path"]
    )
    monkeypatch.setattr(
        "dagster_slurm.pipes_clients.slurm_pipes_client.pack_environment_with_pixi",
        lambda **kwargs: pack_path,
    )

    activation, python_exec = client._prepare_environment(
        ssh_pool=cast(Any, fake_pool),
        remote_base="/remote/base",
        run_dir="/remote/base/runs/run1",
        force_env_push=True,
    )

    assert activation.endswith("/environment.sh")
    assert python_exec == "/remote/base/env-cache/force123/env/bin/python"
    # Pack is uploaded to a per-invocation temp path; _extract_environment then
    # atomically moves it into the shared slot under flock so concurrent runs
    # can't clobber each other's SCP or exec.
    assert len(fake_pool.uploads) == 1
    local_src, remote_dst = fake_pool.uploads[0]
    assert local_src == str(pack_path)
    assert remote_dst.startswith("/remote/base/env-cache/force123/environment.sh.")
    assert remote_dst.endswith(".tmp")
    assert fake_pool.commands == [
        "uname -s",
        "uname -m",
        "mkdir -p /remote/base/env-cache/force123/env",
    ]


def test_prepare_environment_remote_pack_does_not_upload_packed_env(
    monkeypatch: pytest.MonkeyPatch,
    fake_pool: FakePool,
    slurm_client_factory: Callable[..., SlurmPipesClient],
):
    client = slurm_client_factory(pack_on_remote=True, remote_pack_timeout=123)
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        client,
        "_compute_environment_cache_key",
        lambda pack_cmd, env_overrides=None: "remote123",
    )
    monkeypatch.setattr(client, "_cached_env_ready", lambda **_: False)
    monkeypatch.setattr(
        client,
        "_stage_remote_pack_workspace",
        lambda **kwargs: captured.update({"stage": kwargs}),
    )
    monkeypatch.setattr(
        "dagster_slurm.pipes_clients.slurm_pipes_client.pack_environment_with_pixi",
        lambda **_: (_ for _ in ()).throw(AssertionError("should not pack locally")),
    )

    activation, python_exec = client._prepare_environment(
        ssh_pool=cast(Any, fake_pool),
        remote_base="/remote/base",
        run_dir="/remote/base/runs/run1",
        force_env_push=True,
    )

    assert activation == "/remote/base/env-cache/remote123/activate.sh"
    assert python_exec == "/remote/base/env-cache/remote123/env/bin/python"
    assert fake_pool.uploads == []
    assert captured["stage"]["pack_cmds"] == [
        ["pixi", "run", "--frozen", "pack-only"],
    ]
    remote_pack_calls = [
        cmd
        for cmd in fake_pool.commands
        if "flock /remote/base/env-cache/remote123" in cmd
    ]
    assert len(remote_pack_calls) == 1
    assert "pixi run --frozen pack-only" in remote_pack_calls[0]


def test_prepare_environment_remote_pack_falls_back_to_local_pack_on_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_pool: FakePool,
    slurm_client_factory: Callable[..., SlurmPipesClient],
):
    client = slurm_client_factory(pack_on_remote=True, remote_pack_timeout=123)
    pack_path = tmp_path / "environment.sh"
    pack_path.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        client,
        "_compute_environment_cache_key",
        lambda pack_cmd, env_overrides=None: "remote123",
    )
    monkeypatch.setattr(client, "_cached_env_ready", lambda **_: False)
    monkeypatch.setattr(
        client,
        "_pack_environment_on_remote",
        lambda **_: (_ for _ in ()).throw(RuntimeError("remote has no internet")),
    )

    def fake_pack_environment_with_pixi(**kwargs):
        captured["pack"] = kwargs
        return pack_path

    def fake_extract_environment(**kwargs):
        captured["extract"] = kwargs
        return "/remote/base/env-cache/remote123/activate.sh"

    monkeypatch.setattr(
        "dagster_slurm.pipes_clients.slurm_pipes_client.pack_environment_with_pixi",
        fake_pack_environment_with_pixi,
    )
    monkeypatch.setattr(client, "_extract_environment", fake_extract_environment)

    activation, python_exec = client._prepare_environment(
        ssh_pool=cast(Any, fake_pool),
        remote_base="/remote/base",
        run_dir="/remote/base/runs/run1",
        force_env_push=True,
    )

    assert activation == "/remote/base/env-cache/remote123/activate.sh"
    assert python_exec == "/remote/base/env-cache/remote123/env/bin/python"
    assert captured["pack"]["pack_cmd"] == ["pixi", "run", "--frozen", "pack"]
    assert captured["pack"]["env_overrides"] == {"SLURM_PACK_PLATFORM": "linux-64"}
    assert len(fake_pool.uploads) == 1
    assert fake_pool.uploads[0][0] == str(pack_path)
    assert fake_pool.uploads[0][1].startswith(
        "/remote/base/env-cache/remote123/environment.sh."
    )
    assert fake_pool.uploads[0][1].endswith(".tmp")
    assert captured["extract"]["pack_file_path"] == (
        "/remote/base/env-cache/remote123/environment.sh"
    )
    assert captured["extract"]["pack_file_tmp_path"] == fake_pool.uploads[0][1]


def test_remote_pack_input_files_include_manifests_and_base_inputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    slurm_client_factory: Callable[..., SlurmPipesClient],
):
    (tmp_path / "pyproject.toml").write_text(
        """
[tool.pixi.tasks.pack]
cmd = "pixi-pack --inject dist/base-*.whl pyproject.toml"
"""
    )
    (tmp_path / "pixi.lock").write_text("lock")
    dist = tmp_path / "dist"
    dist.mkdir()
    artifact = dist / "base-1.0.0-py3-none-any.whl"
    artifact.write_text("wheel")
    package = tmp_path / "src" / "pkg"
    package.mkdir(parents=True)
    source = package / "__init__.py"
    source.write_text("VALUE = 1\n")

    monkeypatch.chdir(tmp_path)
    client = slurm_client_factory(cache_inject_globs=["src/**/*.py"])

    files = client._remote_pack_input_files(
        [["pixi", "run", "--frozen", "pack"]], project_dir=tmp_path
    )

    relative_paths = {relative for _, relative in files}
    assert relative_paths == {
        "pyproject.toml",
        "pixi.lock",
        "dist/base-1.0.0-py3-none-any.whl",
        "src/pkg/__init__.py",
    }


def test_remote_pack_input_files_detects_absolute_pixi_task(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    slurm_client_factory: Callable[..., SlurmPipesClient],
):
    (tmp_path / "pyproject.toml").write_text(
        """
[tool.pixi.tasks.pack]
cmd = "pixi-pack --inject dist/base-*.whl pyproject.toml"
"""
    )
    (tmp_path / "pixi.lock").write_text("lock")
    dist = tmp_path / "dist"
    dist.mkdir()
    artifact = dist / "base-1.0.0-py3-none-any.whl"
    artifact.write_text("wheel")

    monkeypatch.chdir(tmp_path)
    client = slurm_client_factory()

    files = client._remote_pack_input_files(
        [["/opt/pixi/bin/pixi", "run", "--frozen", "pack"]],
        project_dir=tmp_path,
    )

    relative_paths = {relative for _, relative in files}
    assert "dist/base-1.0.0-py3-none-any.whl" in relative_paths


def test_remote_pack_input_files_loads_pixi_task_from_project_dir(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    slurm_client_factory: Callable[..., SlurmPipesClient],
):
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / "pyproject.toml").write_text(
        """
[tool.pixi.tasks.pack]
cmd = "pixi-pack --inject dist/base-*.whl pyproject.toml"
"""
    )
    (project_dir / "pixi.lock").write_text("lock")
    dist = project_dir / "dist"
    dist.mkdir()
    artifact = dist / "base-1.0.0-py3-none-any.whl"
    artifact.write_text("wheel")

    other_dir = tmp_path / "other"
    other_dir.mkdir()
    (other_dir / "pyproject.toml").write_text(
        """
[tool.pixi.tasks.pack]
cmd = "pixi-pack --inject dist/wrong-*.whl pyproject.toml"
"""
    )
    monkeypatch.chdir(other_dir)
    client = slurm_client_factory()

    files = client._remote_pack_input_files(
        [["pixi", "run", "--frozen", "pack"]],
        project_dir=project_dir,
    )

    relative_paths = {relative for _, relative in files}
    assert "dist/base-1.0.0-py3-none-any.whl" in relative_paths
    assert not any("wrong" in relative for relative in relative_paths)


def test_remote_pack_input_files_error_on_missing_pattern(
    tmp_path: Path,
    slurm_client_factory: Callable[..., SlurmPipesClient],
):
    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'demo'\n")
    (tmp_path / "pixi.lock").write_text("lock")
    client = slurm_client_factory()

    with pytest.raises(FileNotFoundError, match="dist/missing-\\*.whl"):
        client._remote_pack_input_files(
            [["pixi-pack", "--inject", "dist/missing-*.whl"]],
            project_dir=tmp_path,
        )


def test_remote_pack_input_files_rejects_matches_outside_workspace(
    tmp_path: Path,
    slurm_client_factory: Callable[..., SlurmPipesClient],
):
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / "pyproject.toml").write_text("[project]\nname = 'demo'\n")
    (project_dir / "pixi.lock").write_text("lock")
    outside = tmp_path / "secret.txt"
    outside.write_text("do not stage")
    client = slurm_client_factory()

    with pytest.raises(ValueError, match="outside the local workspace root"):
        client._remote_pack_input_files(
            [["pixi-pack", "--inject", "../secret.txt"]],
            project_dir=project_dir,
        )


def test_remote_pack_join_rejects_paths_outside_workspace():
    remote_root = "/remote/base/env-cache/key/pack-work/token"
    remote_project_dir = f"{remote_root}/project"

    assert (
        _remote_join_under_root(
            root=remote_root,
            base=remote_project_dir,
            relative_path="../dist/base.whl",
        )
        == f"{remote_root}/dist/base.whl"
    )
    with pytest.raises(ValueError, match="escapes staging workspace"):
        _remote_join_under_root(
            root=remote_root,
            base=remote_project_dir,
            relative_path="../../dist/base.whl",
        )
    with pytest.raises(ValueError, match="must be relative"):
        _remote_join_under_root(
            root=remote_root,
            base=remote_project_dir,
            relative_path="/tmp/base.whl",
        )


def test_remote_pack_flow_stages_inputs_and_extracts_with_shell(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    slurm_client_factory: Callable[..., SlurmPipesClient],
):
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / "pyproject.toml").write_text(
        """
[tool.pixi.tasks.pack-only]
cmd = "pixi-pack --inject dist/base-*.whl pyproject.toml"
""",
        encoding="utf-8",
    )
    (project_dir / "pixi.lock").write_text("lock", encoding="utf-8")
    dist = project_dir / "dist"
    dist.mkdir()
    (dist / "base-1.0.0-py3-none-any.whl").write_text("wheel", encoding="utf-8")

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_pixi = fake_bin / "pixi"
    fake_pixi.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
cat > environment.sh <<'PACK'
#!/usr/bin/env bash
set -euo pipefail
mkdir -p env/bin
printf '#!/usr/bin/env bash\n' > env/bin/python
chmod +x env/bin/python
printf 'export PATH="$(pwd)/env/bin:$PATH"\n' > activate.sh
PACK
chmod +x environment.sh
""",
        encoding="utf-8",
    )
    fake_pixi.chmod(0o755)
    fake_flock = fake_bin / "flock"
    fake_flock.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
shift
exec "$@"
""",
        encoding="utf-8",
    )
    fake_flock.chmod(0o755)

    monkeypatch.chdir(project_dir)
    monkeypatch.setenv("PATH", f"{fake_bin}{os.pathsep}{os.environ['PATH']}")

    client = slurm_client_factory(pack_on_remote=True, remote_pack_timeout=10)
    pool = LocalShellPool()
    env_base_dir = tmp_path / "remote with spaces" / "env-cache" / "remote123"
    env_dir = env_base_dir / "env"

    activation_script = client._pack_environment_on_remote(
        ssh_pool=cast(Any, pool),
        env_base_dir=str(env_base_dir),
        env_dir=str(env_dir),
        pack_cmd=["pixi", "run", "--frozen", "pack-only"],
        env_overrides={"SLURM_PACK_PLATFORM": "linux-64"},
    )

    assert activation_script == str(env_base_dir / "activate.sh")
    assert (env_base_dir / "activate.sh").is_file()
    assert (env_dir / "bin" / "python").is_file()
    staged_destinations = {Path(dest).name for _, dest in pool.uploads}
    assert {"pyproject.toml", "pixi.lock", "base-1.0.0-py3-none-any.whl"} <= (
        staged_destinations
    )
    assert not any(Path(src).name == "environment.sh" for src, _ in pool.uploads)


def test_resolve_pack_platform_detects_apple_silicon_under_rosetta(
    monkeypatch: pytest.MonkeyPatch,
):
    client = SlurmPipesClient(
        slurm_resource=cast(Any, SimpleNamespace(ssh=None, queue=None)),
        launcher=BashLauncher(),
        auto_detect_platform=True,
    )

    monkeypatch.setattr(
        "dagster_slurm.pipes_clients.slurm_pipes_client.platform.system",
        lambda: "Darwin",
    )
    monkeypatch.setattr(
        "dagster_slurm.pipes_clients.slurm_pipes_client.platform.machine",
        lambda: "x86_64",
    )
    monkeypatch.setattr(
        "dagster_slurm.pipes_clients.slurm_pipes_client.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout="1\n"),
    )

    assert client._resolve_pack_platform() == "linux-aarch64"


def test_prepare_environment_prefers_remote_submitter_architecture(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    client = SlurmPipesClient(
        slurm_resource=cast(Any, SimpleNamespace(ssh=None, queue=None)),
        launcher=BashLauncher(),
        auto_detect_platform=True,
    )

    class RemoteArmPool(FakePool):
        def run(self, cmd, timeout=None):
            self.commands.append(cmd)
            if cmd == "uname -s":
                return "Linux\n"
            if cmd == "uname -m":
                return "aarch64\n"
            return super().run(cmd, timeout=timeout)

    pool = RemoteArmPool()
    pack_path = tmp_path / "environment-linux-aarch64.sh"
    pack_path.write_text("#!/bin/bash\necho hi\n")
    captured: dict[str, Any] = {}

    def fake_cache_key(pack_cmd: list[str], env_overrides=None) -> str:
        captured["cache_pack_cmd"] = pack_cmd
        captured["cache_env_overrides"] = env_overrides
        return "remotearm123"

    def fake_pack_environment_with_pixi(**kwargs):
        captured["pack_cmd"] = kwargs["pack_cmd"]
        captured["pack_env_overrides"] = kwargs["env_overrides"]
        return pack_path

    monkeypatch.setattr(client, "_compute_environment_cache_key", fake_cache_key)
    monkeypatch.setattr(client, "_cached_env_ready", lambda **_: False)
    monkeypatch.setattr(
        client, "_extract_environment", lambda **kwargs: kwargs["pack_file_path"]
    )
    monkeypatch.setattr(
        "dagster_slurm.pipes_clients.slurm_pipes_client.pack_environment_with_pixi",
        fake_pack_environment_with_pixi,
    )
    monkeypatch.setattr(
        "dagster_slurm.pipes_clients.slurm_pipes_client.platform.system",
        lambda: "Darwin",
    )
    monkeypatch.setattr(
        "dagster_slurm.pipes_clients.slurm_pipes_client.platform.machine",
        lambda: "x86_64",
    )

    activation, python_exec = client._prepare_environment(
        ssh_pool=cast(Any, pool),
        remote_base="/remote/base",
        run_dir="/remote/base/runs/run1",
        force_env_push=True,
        pack_cmd_override=[
            "pixi",
            "run",
            "-e",
            "opstooling",
            "--frozen",
            "python",
            "scripts/pack_environment.py",
            "--env",
            "workload-document-processing",
            "--build-missing",
        ],
    )

    assert activation.endswith("/environment-linux-aarch64.sh")
    assert python_exec == "/remote/base/env-cache/remotearm123/env/bin/python"
    assert captured["cache_env_overrides"] == {"SLURM_PACK_PLATFORM": "linux-aarch64"}
    assert captured["pack_env_overrides"] == {"SLURM_PACK_PLATFORM": "linux-aarch64"}
    assert captured["pack_cmd"][-2:] == ["--platform", "linux-aarch64"]
    assert pool.commands[:2] == ["uname -s", "uname -m"]


def test_custom_pack_environment_override_gets_explicit_platform():
    client = SlurmPipesClient(
        slurm_resource=cast(Any, SimpleNamespace(ssh=None, queue=None)),
        launcher=BashLauncher(),
        pack_platform="linux-aarch64",
        auto_detect_platform=False,
    )

    command = client._get_pack_command(
        override=[
            "pixi",
            "run",
            "-e",
            "opstooling",
            "--frozen",
            "python",
            "scripts/pack_environment.py",
            "--env",
            "workload-document-processing",
            "--build-missing",
        ],
        full_build=True,
    )

    assert command[-2:] == ["--platform", "linux-aarch64"]


def test_custom_pack_environment_override_preserves_existing_platform():
    client = SlurmPipesClient(
        slurm_resource=cast(Any, SimpleNamespace(ssh=None, queue=None)),
        launcher=BashLauncher(),
        pack_platform="linux-aarch64",
        auto_detect_platform=False,
    )

    command = client._get_pack_command(
        override=[
            "pixi",
            "run",
            "-e",
            "opstooling",
            "--frozen",
            "python",
            "scripts/pack_environment.py",
            "--env",
            "workload-document-processing",
            "--platform",
            "linux-64",
            "--build-missing",
        ],
        full_build=True,
    )

    assert command.count("--platform") == 1
    assert command[command.index("--platform") + 1] == "linux-64"


def test_pack_platform_env_override_wins(monkeypatch: pytest.MonkeyPatch):
    client = SlurmPipesClient(
        slurm_resource=cast(Any, SimpleNamespace(ssh=None, queue=None)),
        launcher=BashLauncher(),
        pack_platform="linux-64",
        auto_detect_platform=False,
    )

    monkeypatch.setenv("SLURM_PACK_PLATFORM", "linux-aarch64")

    assert client._resolve_pack_platform() == "linux-aarch64"


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

    def run(
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


def test_compute_resource_passes_remote_pack_options_to_slurm_client():
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
        pack_on_remote=True,
        remote_pack_timeout=321,
    )

    client = resource.get_pipes_client(cast(Any, SimpleNamespace()))

    assert isinstance(client, SlurmPipesClient)
    assert client.pack_on_remote is True
    assert client.remote_pack_timeout == 321


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
