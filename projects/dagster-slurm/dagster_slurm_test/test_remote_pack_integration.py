"""Docker-backed integration coverage for remote environment packaging."""

import os
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
