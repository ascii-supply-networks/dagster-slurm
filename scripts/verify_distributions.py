"""Verify that built distributions install as usable packages."""

from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
from pathlib import Path

from packaging.version import Version

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib


IMPORT_CHECK = """
import sys
from importlib.metadata import version

from dagster_slurm import (
    BashLauncher,
    ComputeResource,
    LocalPipesClient,
    RayLauncher,
    SlurmPipesClient,
    SlurmResource,
    SSHConnectionResource,
)
from packaging.version import Version

assert Version(version("dagster-slurm")) == Version(sys.argv[1])
assert all(
    (
        BashLauncher,
        ComputeResource,
        LocalPipesClient,
        RayLauncher,
        SlurmPipesClient,
        SlurmResource,
        SSHConnectionResource,
    )
)
"""


def only_distribution(dist_dir: Path, pattern: str) -> Path:
    matches = list(dist_dir.glob(pattern))
    if len(matches) != 1:
        raise SystemExit(
            f"Expected exactly one {pattern} in {dist_dir}, found {len(matches)}"
        )
    return matches[0].resolve()


def verify_distribution(artifact: Path, expected_version: str) -> None:
    with tempfile.TemporaryDirectory(prefix="dagster-slurm-dist-") as temp_dir:
        venv = Path(temp_dir) / "venv"
        subprocess.run(
            ["uv", "venv", "--no-project", "--python", sys.executable, str(venv)],
            check=True,
        )
        python = venv / (
            "Scripts/python.exe" if sys.platform == "win32" else "bin/python"
        )
        subprocess.run(
            ["uv", "pip", "install", "--python", str(python), str(artifact)],
            check=True,
        )
        subprocess.run(
            ["uv", "pip", "check", "--python", str(python)],
            check=True,
        )
        subprocess.run(
            [str(python), "-c", IMPORT_CHECK, expected_version],
            check=True,
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("dist_dir", type=Path)
    args = parser.parse_args()

    with Path("projects/dagster-slurm/pyproject.toml").open("rb") as pyproject:
        expected_version = tomllib.load(pyproject)["project"]["version"]
    Version(expected_version)

    verify_distribution(only_distribution(args.dist_dir, "*.whl"), expected_version)
    verify_distribution(only_distribution(args.dist_dir, "*.tar.gz"), expected_version)


if __name__ == "__main__":
    main()
