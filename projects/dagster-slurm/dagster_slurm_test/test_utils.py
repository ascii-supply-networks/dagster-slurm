import os
import subprocess
from pathlib import Path
from typing import Dict


def run_dg_command(
    example_project_dir: Path,
    deployment: str,
    assets: str,
    env_overrides: Dict[str, str] = {},
    timeout: int = 300,
) -> subprocess.CompletedProcess:
    """Run a `dg launch` command with specified deployment mode.

    Args:
        example_project_dir: Path to examples directory
        deployment: DAGSTER_DEPLOYMENT value (development, STAGING_DOCKER, etc.)
        assets: Comma-separated asset names
        env_overrides: Additional environment variables
        timeout: Command timeout in seconds

    Returns:
        CompletedProcess with stdout/stderr
    """
    env = os.environ.copy()
    env["DAGSTER_DEPLOYMENT"] = deployment

    if env_overrides:
        env.update(env_overrides)

    cmd = [
        "pixi",
        "run",
        "-e",
        "dev",
        "dg",
        "--target-path",
        "examples",
        "launch",
        "--assets",
        assets,
    ]

    print(f"\n🚀 Running: {' '.join(cmd)}")
    print(f"   Deployment: {deployment}")
    print(f"   Assets: {assets}")

    result = subprocess.run(
        cmd,
        cwd=example_project_dir,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )

    print("\n--- STDOUT ---")
    print(result.stdout)
    if result.stderr:
        print("\n--- STDERR ---")
        print(result.stderr)

    return result


def assert_materialization_success(result: subprocess.CompletedProcess, assets: str):
    """Assert that materialization completed successfully."""
    # The main process should exit cleanly
    assert result.returncode == 0, (
        f"Command failed with exit code {result.returncode}\n"
        f"STDERR: {result.stderr}\n"
        f"STDOUT: {result.stdout}"
    )

    # FIX: Dagster CLI logs go to stderr. We must check there.
    logs = result.stderr

    # Check for success indicators
    for asset in assets.split(","):
        # Make the check more specific to the materialization event
        assert f"ASSET_MATERIALIZATION - Materialized value {asset}" in logs, (
            f"Materialization event for asset '{asset}' not found in stderr logs."
        )

    # Check for common failure patterns
    assert "STEP_FAILURE" not in logs, "Step failure detected in logs"
    assert "RUN_FAILURE" not in logs, "Run failure detected in logs"
    assert "CheckError" not in logs, "Check error detected in logs"
    assert "ConnectionError" not in logs, "Connection error detected in logs"
