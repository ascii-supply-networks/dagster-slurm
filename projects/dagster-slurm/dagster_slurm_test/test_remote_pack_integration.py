"""Docker-backed integration coverage for remote environment packaging."""

import os
import shlex
import uuid
from pathlib import Path
from typing import Any, cast

import pytest
from dagster_slurm import (
    BashLauncher,
    SSHConnectionResource,
    SlurmQueueConfig,
    SlurmResource,
)
from dagster_slurm.helpers.ssh_pool import SSHConnectionPool
from dagster_slurm.pipes_clients.slurm_pipes_client import SlurmPipesClient


pytestmark = pytest.mark.needs_slurm_docker


def test_remote_pack_on_docker_edge_node(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """Exercise remote packaging over real SSH into the Docker Slurm edge node."""
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

    fake_pixi = tmp_path / "pixi"
    fake_pixi.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
test -f pyproject.toml
test -f pixi.lock
test -f dist/base-1.0.0-py3-none-any.whl
cat > environment.sh <<'PACK'
#!/usr/bin/env bash
set -euo pipefail
mkdir -p env/bin
printf '#!/usr/bin/env bash\n' > env/bin/python
chmod +x env/bin/python
printf 'export PATH="$(pwd)/env/bin:$PATH"\n' > activate.sh
touch remote-pack-marker
PACK
chmod +x environment.sh
""",
        encoding="utf-8",
    )
    fake_pixi.chmod(0o755)

    remote_root = f"/home/submitter/dagster_ci_runs/remote-pack-test-{uuid.uuid4().hex}"
    remote_pixi = f"{remote_root}/bin/pixi"
    env_base_dir = f"{remote_root}/env-cache/docker123"
    env_dir = f"{env_base_dir}/env"

    ssh = SSHConnectionResource(
        host=os.environ.get("SLURM_EDGE_NODE_HOST", "127.0.0.1"),
        port=int(os.environ.get("SLURM_EDGE_NODE_PORT", "2223")),
        user=os.environ.get("SLURM_EDGE_NODE_USER", "submitter"),
        password=os.environ.get("SLURM_EDGE_NODE_PASSWORD", "submitter"),
    )
    slurm = SlurmResource(ssh=ssh, queue=SlurmQueueConfig(), remote_base=remote_root)
    client = SlurmPipesClient(
        slurm_resource=slurm,
        launcher=BashLauncher(),
        cache_inject_globs=["dist/*.whl"],
        pack_on_remote=True,
        remote_pack_timeout=60,
    )

    monkeypatch.chdir(project_dir)
    pool = SSHConnectionPool(ssh)
    with pool:
        try:
            pool.run(f"rm -rf {remote_root} && mkdir -p {remote_root}/bin")
            pool.run(
                "command -v pixi && pixi --version && "
                "command -v pixi-pack && pixi-pack --version"
            )
            pool.upload_file(str(fake_pixi), remote_pixi)
            pool.run(f"chmod +x {remote_pixi}")

            activation_script = client._pack_environment_on_remote(
                ssh_pool=cast(Any, pool),
                env_base_dir=env_base_dir,
                env_dir=env_dir,
                pack_cmd=[remote_pixi, "run", "--frozen", "pack-only"],
                env_overrides={"SLURM_PACK_PLATFORM": "linux-64"},
            )

            assert activation_script == f"{env_base_dir}/activate.sh"
            pool.run(
                " && ".join(
                    [
                        f"test -f {env_base_dir}/activate.sh",
                        f"test -x {env_dir}/bin/python",
                        f"test -f {env_base_dir}/remote-pack-marker",
                    ]
                )
            )
        finally:
            pool.run(f"rm -rf {remote_root}")


def test_project_setup_on_docker_edge_node(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """Install, activate, execute, reuse, and refresh two real Pixi environments."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / "pixi.toml").write_text(
        """[workspace]
channels = ["conda-forge"]
platforms = ["linux-64"]

[dependencies]
python = "3.12.*"

[feature.cpu.activation.env]
DAGSTER_SLURM_TEST_ENV = "cpu"

[feature.cpu.activation]
scripts = ["activate-cpu.sh"]

[feature.gpu.activation.env]
DAGSTER_SLURM_TEST_ENV = "gpu"

[feature.gpu.activation]
scripts = ["activate-gpu.sh"]

[environments]
cpu = ["cpu"]
gpu = ["gpu"]
""",
        encoding="utf-8",
    )
    for environment in ("cpu", "gpu"):
        (project_dir / f"activate-{environment}.sh").write_text(
            f"export DAGSTER_SLURM_TEST_SCRIPT={environment}-script\n",
            encoding="utf-8",
        )
    (project_dir / "setup.sh").write_text(
        """#!/usr/bin/env bash
set -euo pipefail
test "$SETUP_KIND" = "multi-environment"
pixi install --all
printf 'setup-ran\n' >> "$DAGSTER_SLURM_ENV_BASE_DIR/setup-count"
""",
        encoding="utf-8",
    )

    remote_root = f"/home/submitter/dagster_ci_runs/project-setup-{uuid.uuid4().hex}"
    ssh = SSHConnectionResource(
        host=os.environ.get("SLURM_EDGE_NODE_HOST", "127.0.0.1"),
        port=int(os.environ.get("SLURM_EDGE_NODE_PORT", "2223")),
        user=os.environ.get("SLURM_EDGE_NODE_USER", "submitter"),
        password=os.environ.get("SLURM_EDGE_NODE_PASSWORD", "submitter"),
    )
    slurm = SlurmResource(ssh=ssh, queue=SlurmQueueConfig(), remote_base=remote_root)
    client = SlurmPipesClient(
        slurm_resource=slurm,
        launcher=BashLauncher(),
        project_setup_cmd=["bash", "setup.sh"],
        project_setup_env={"SETUP_KIND": "multi-environment"},
        project_setup_input_globs=["setup.sh", "activate-*.sh"],
        remote_pack_timeout=60,
    )

    monkeypatch.chdir(project_dir)
    pool = SSHConnectionPool(ssh)
    with pool:
        try:
            activation_script, python_executable = client._prepare_environment(
                ssh_pool=cast(Any, pool),
                remote_base=remote_root,
                run_dir=f"{remote_root}/runs/run-1",
                force_env_push=False,
                environment_name="gpu",
            )

            assert activation_script.endswith("/envs/gpu/activate.sh")
            assert python_executable.endswith("/envs/gpu/bin/python")
            envs_dir = python_executable.removesuffix("/gpu/bin/python")
            env_base_dir = envs_dir.removesuffix("/envs")
            pool.run(
                " && ".join(
                    [
                        f"test -x {envs_dir}/cpu/bin/python",
                        f"test -x {envs_dir}/gpu/bin/python",
                        f"test -f {envs_dir}/cpu/activate.sh",
                        f"test -f {envs_dir}/gpu/activate.sh",
                        f"test $(wc -l < {env_base_dir}/setup-count) -eq 1",
                    ]
                )
            )

            def execute_in_environment(
                activation: str,
                expected_environment: str,
            ) -> str:
                python_code = (
                    "import os, sys; "
                    f"assert os.environ['DAGSTER_SLURM_TEST_ENV'] == "
                    f"{expected_environment!r}; "
                    f"assert os.environ['DAGSTER_SLURM_TEST_SCRIPT'] == "
                    f"{f'{expected_environment}-script'!r}; "
                    "print(sys.executable)"
                )
                activated_command = (
                    f"source {shlex.quote(activation)} && "
                    f"python -c {shlex.quote(python_code)}"
                )
                return pool.run(f"bash -c {shlex.quote(activated_command)}").strip()

            assert execute_in_environment(activation_script, "gpu").endswith(
                "/.pixi/envs/gpu/bin/python"
            )

            cached_activation, cached_python = client._prepare_environment(
                ssh_pool=cast(Any, pool),
                remote_base=remote_root,
                run_dir=f"{remote_root}/runs/run-2",
                force_env_push=False,
                environment_name="cpu",
            )
            assert cached_python.endswith("/envs/cpu/bin/python")
            assert execute_in_environment(cached_activation, "cpu").endswith(
                "/.pixi/envs/cpu/bin/python"
            )
            pool.run(f"test $(wc -l < {env_base_dir}/setup-count) -eq 1")

            refreshed_activation, _ = client._prepare_environment(
                ssh_pool=cast(Any, pool),
                remote_base=remote_root,
                run_dir=f"{remote_root}/runs/run-3",
                force_env_push=True,
                environment_name="gpu",
            )
            assert execute_in_environment(refreshed_activation, "gpu").endswith(
                "/.pixi/envs/gpu/bin/python"
            )
            pool.run(f"test $(wc -l < {env_base_dir}/setup-count) -eq 2")
        finally:
            pool.run(f"rm -rf {remote_root}")
