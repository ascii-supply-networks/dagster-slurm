# dagster_slurm/helpers/env_packaging.py (UPDATED FOR PIXI)

"""Environment packaging using pixi pack."""

import os
import subprocess
import shlex
from pathlib import Path
from typing import Optional
from dagster import get_dagster_logger


def pack_environment_with_pixi(
    project_dir: Optional[Path] = None, pack_cmd=["pixi", "run", "--frozen", "pack"]
) -> Path:
    """
    Pack environment using 'pixi pack'.

    This creates a self-contained tarball with:
    - All conda packages
    - Python wheels
    - Environment activation script

    Args:
        project_dir: Path to project root (auto-detected if None)

    Returns:
        Path to project directory (contains packed artifacts)
    """
    logger = get_dagster_logger()

    if project_dir is None:
        project_dir = _find_project_root()

    project_dir = Path(project_dir).resolve()
    logger.info(f"Packing environment from: {project_dir}")

    # Verify pixi project
    if not (project_dir / "pixi.toml").exists():
        raise FileNotFoundError(f"No pixi.toml found in {project_dir}")

    # Clean environment
    env = os.environ.copy()
    env.pop("PIXI_ENVIRONMENT", None)
    env.pop("PIXI_PROJECT_MANIFEST", None)
    env.pop("PIXI_PROJECT_ROOT", None)

    logger.debug(f"Running: {shlex.join(pack_cmd)}")

    result = subprocess.run(
        pack_cmd,
        cwd=str(project_dir),
        env=env,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        logger.error(
            f"pixi pack failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )
        raise subprocess.CalledProcessError(
            result.returncode, pack_cmd, result.stdout, result.stderr
        )

    logger.info("Environment packed successfully")
    logger.debug(f"Pack output:\n{result.stdout}")

    # Find the packed tarball
    pack_file = _find_pack_file(project_dir)
    if not pack_file:
        raise FileNotFoundError(
            "pixi pack succeeded but no .tar.bz2 found. "
            f"Check {project_dir} for packed environment."
        )

    logger.info(f"Packed environment: {pack_file}")

    return pack_file


def _find_project_root() -> Path:
    """Find project root containing pixi.toml."""
    here = Path(__file__).resolve()

    # Walk up from current file
    for parent in [here, *here.parents]:
        if (parent / "pixi.toml").exists():
            return parent

    # Try CWD
    if (Path.cwd() / "pixi.toml").exists():
        return Path.cwd()

    raise FileNotFoundError(
        "Could not locate pixi.toml. "
        "Ensure you're running from project root or provide project_dir."
    )


def _find_pack_file(project_dir: Path) -> Optional[Path]:
    """Find the most recent .tar.bz2 pack file."""
    import glob

    # pixi pack creates files like: dagster-slurm-1.3.1-linux-64.tar.bz2
    pattern = str(project_dir / "*.tar.bz2")
    matches = glob.glob(pattern)

    if not matches:
        return None

    # Return most recent
    paths = [Path(m) for m in matches]
    return max(paths, key=lambda p: p.stat().st_mtime)
