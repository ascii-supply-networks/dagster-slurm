"""Environment packaging using pixi pack."""

import re
import shlex
import subprocess
from pathlib import Path
from typing import Optional

from dagster import get_dagster_logger


def pack_environment_with_pixi(
    project_dir: Optional[Path] = None,
    pack_cmd: Optional[list[str]] = None,
    timeout: int = 600,
) -> Path:
    """Pack environment using 'pixi pack'.

    This creates a self-contained environment package:
    - If --create-executable: self-extracting shell script (environment.sh)
    - Otherwise: tarball (.tar.bz2)

    Args:
        project_dir: Path to run pixi from (uses CWD if None, pixi walks up to find pixi.toml)
        pack_cmd: Custom pack command (defaults to ["pixi", "run", "--frozen", "pack"])
        timeout: Command timeout in seconds (default: 600)

    Returns:
        Path to packed file (environment.sh or .tar.bz2)

    Raises:
        RuntimeError: If pixi is not installed or command times out
        FileNotFoundError: If pixi.toml not found or packed file can't be located
        ValueError: If pack task not defined or environment not found
        subprocess.CalledProcessError: If pixi pack fails

    """
    logger = get_dagster_logger()

    if pack_cmd is None:
        pack_cmd = ["pixi", "run", "--frozen", "pack"]

    # Use provided directory or current working directory
    # pixi will automatically walk up to find pixi.toml
    cwd = str(project_dir.resolve()) if project_dir else None

    logger.debug(f"Command: {shlex.join(pack_cmd)}")
    logger.debug(f"Timeout: {timeout}s")

    try:
        result = subprocess.run(
            pack_cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except FileNotFoundError as e:
        raise RuntimeError(
            f"Failed to run pixi command. Is pixi installed?\n"
            f"Install it with: curl -fsSL https://pixi.sh/install.sh | bash\n"
            f"Error: {e}"
        ) from e
    except subprocess.TimeoutExpired:
        raise RuntimeError(
            f"pixi pack timed out after {timeout} seconds. "
            f"This may indicate a problem with the environment or network. "
            f"Consider increasing the timeout parameter."
        )

    # Log pixi output to Dagster logs
    if result.stdout:
        logger.info(f"pixi pack stdout:\n{result.stdout}")
    if result.stderr:
        # stderr might contain warnings/progress, log as debug
        logger.debug(f"pixi pack stderr:\n{result.stderr}")

    if result.returncode != 0:
        stderr = result.stderr.strip()
        stdout = result.stdout.strip()

        # Check for common errors
        if "could not find a `pixi.toml`" in stderr.lower():
            raise FileNotFoundError(
                f"pixi could not find a pixi.toml file.\n"
                f"Searched from: {cwd or Path.cwd()}\n"
                f"pixi automatically walks up directories to find pixi.toml.\n"
                f"Ensure you're running from within a pixi project or provide project_dir.\n\n"
                f"stderr: {stderr}"
            )

        if (
            "task 'pack' not found" in stderr.lower()
            or "unknown task" in stderr.lower()
        ):
            raise ValueError(
                f"The 'pack' task is not defined in your pixi.toml.\n"
                f"Add a pack task to your pyproject.toml or pixi.toml:\n\n"
                f"[tool.pixi.tasks.pack]\n"
                f'cmd = "pixi-pack --environment packaged-cluster ..."\n'
                f'description = "Pack environment for deployment"\n\n'
                f"stderr: {stderr}"
            )

        if "environment" in stderr.lower() and "not found" in stderr.lower():
            raise ValueError(
                f"pixi pack failed - environment not found.\n"
                f"Check that the target environment is defined in your pixi.toml.\n\n"
                f"stderr: {stderr}"
            )

        # Generic error
        logger.error(f"pixi pack failed with exit code {result.returncode}")
        raise subprocess.CalledProcessError(result.returncode, pack_cmd, stdout, stderr)

    logger.info("pixi pack completed successfully")

    # Find the packed file - could be executable or tarball
    search_dir = Path(cwd) if cwd else Path.cwd()

    # Try to extract path from pixi output first (most reliable)
    pack_file = _extract_pack_path_from_output(result.stdout, search_dir)

    if not pack_file:
        # Fallback: search for common pack files
        pack_file = _find_pack_file(search_dir)

    if not pack_file:
        raise FileNotFoundError(
            f"pixi pack succeeded but couldn't locate the packed file.\n"
            f"Expected environment.sh or .tar.bz2 file in: {search_dir}\n"
            f"pixi output:\n{result.stdout}\n{result.stderr}"
        )

    logger.info(
        f"Packed environment: {pack_file} ({_format_size(pack_file.stat().st_size)})"
    )
    return pack_file


def _extract_pack_path_from_output(output: str, base_dir: Path) -> Optional[Path]:
    """Extract pack file path from pixi-pack output.

    Examples:
        "Created pack at /path/to/environment.sh with size 318.74 MiB"
        "Packed environment to: /path/to/file.tar.bz2"

    """
    patterns = [
        # Match: "Created pack at <PATH> with size"
        r"Created pack at\s+([^\s]+(?:environment\.sh|\.tar\.bz2))\s+with size",
        # Match: "Packed|Created|Wrote <something>: <PATH>"
        r"(?:Packed|Created|Wrote).*?:\s*([^\s]+\.(?:tar\.bz2|sh))",
        # Match any .tar.bz2 or .sh path
        r"([^\s]+\.(?:tar\.bz2|sh))",
    ]

    for pattern in patterns:
        if match := re.search(pattern, output, re.IGNORECASE):
            path_str = match.group(1)
            path = Path(path_str)

            # Handle relative paths
            if not path.is_absolute():
                path = base_dir / path

            if path.exists():
                return path

    return None


def _find_pack_file(search_dir: Path) -> Optional[Path]:
    """Find the most recent pack file (executable or tarball)."""
    import glob

    # Look for both executable and tarball formats
    candidates = []

    # pixi-pack with --create-executable creates: environment.sh
    for pattern in ["environment.sh", "*.tar.bz2"]:
        matches = glob.glob(str(search_dir / pattern))
        candidates.extend(matches)

    if not candidates:
        return None

    # Return most recent
    paths = [Path(m) for m in candidates]
    return max(paths, key=lambda p: p.stat().st_mtime)


def _format_size(size_bytes: int) -> str:
    """Format bytes as human-readable size."""
    for unit in ["B", "KiB", "MiB", "GiB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0  # type: ignore
    return f"{size_bytes:.2f} TiB"
