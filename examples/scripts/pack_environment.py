#!/usr/bin/env python3
"""
Pack a pixi environment with local dependencies injected.

This script builds local packages and injects them into a pixi-pack environment.
It's designed to be placed in scripts/pack_environment.py in your pixi workspace.

Usage:
    python scripts/pack_environment.py --env packaged-cluster --platform linux-64
    python scripts/pack_environment.py --build-missing --debug

Directory structure assumed:
    your-workspace/
    ├── pyproject.toml          # Main pixi project
    ├── scripts/
    │   └── pack_environment.py # This script
    └── projects/               # Local packages to build and inject
        ├── your-package-1/
        ├── your-package-2/
        └── ...

To adapt for your project:
1. Update INJECT_PATTERNS below to match your local packages
2. Update BUILD_COMMANDS to define how to build each package
3. All paths are relative to the workspace root (where pyproject.toml lives)
"""
import argparse
import os
import platform
import subprocess
from pathlib import Path
import time

# ============================================================================
# CONFIGURATION - Customize for your project structure
# ============================================================================

# Patterns for built artifacts to inject into the packed environment
# Paths are relative to the workspace root (where pyproject.toml lives)
#
# Example for a simple project with just one package at root:
#   INJECT_PATTERNS = ["dist/my_package-*.conda"]
#
# Example for multiple packages in a projects/ directory:
#   INJECT_PATTERNS = [
#       "projects/package-a/dist/package_a-*.conda",
#       "projects/package-b/dist/package_b-*.whl",
#   ]
INJECT_PATTERNS = [
    # Local conda packages
    "projects/dagster-slurm-example-shared/dist/dagster_slurm_example_shared-*.conda",
    "projects/dagster-slurm-example-hpc-workload/dist/dagster_slurm_example_hpc_workload-*.conda",
    "projects/dagster-slurm-example/dist/dagster_slurm_example-*.conda",
    # External dependency (dagster-slurm library from parent repo) - remove if not needed
    "../dist/dagster_slurm-*-py3-none-any.whl",
]

# Build commands: (relative_dir, command_list)
# Commands are executed with cwd set to relative_dir (relative to workspace root)
#
# Example for a simple project at root:
#   BUILD_COMMANDS = [(".", ["pixi", "build", "-o", "dist"])]
#
# Example for multiple packages:
#   BUILD_COMMANDS = [
#       ("projects/package-a", ["pixi", "build", "-o", "dist"]),
#       ("projects/package-b", ["uv", "build", "-o", "dist"]),
#   ]
BUILD_COMMANDS = [
    # Build local conda packages
    ("projects/dagster-slurm-example-shared", ["pixi", "build", "-o", "dist"]),
    ("projects/dagster-slurm-example-hpc-workload", ["pixi", "build", "-o", "dist"]),
    ("projects/dagster-slurm-example", ["pixi", "build", "-o", "dist"]),
    # Build external dependency (dagster-slurm library from parent repo) - remove if not needed
    ("../projects", ["pixi", "run", "-e", "build", "--frozen", "build-lib"]),
]


def _detect_platform() -> str:
    system = platform.system().lower()
    machine = platform.machine().lower()
    if system == "linux" and ("aarch64" in machine or "arm" in machine):
        return "linux-aarch64"
    if system == "darwin" and "arm" in machine:
        return "osx-arm64"
    return "linux-64"


def _resolve_inject_args(base_dir: Path, allow_missing: bool) -> list[str]:
    """Resolve inject patterns to actual file paths."""
    args: list[str] = []
    for pattern in INJECT_PATTERNS:
        matches = sorted(
            [p for p in base_dir.glob(pattern) if p.is_file()],
            key=lambda p: p.stat().st_mtime,
        )
        if not matches:
            if allow_missing:
                print(f"⚠️  Skipping missing inject pattern: {pattern}")
                continue
            error_msg = (
                f"No files matched inject pattern: {pattern}\n"
                f"  Searched in: {base_dir}\n"
                f"  Tip: Use --build-missing to build artifacts automatically,\n"
                f"       or --allow-missing-injects to skip missing files."
            )
            raise FileNotFoundError(error_msg)
        # Use the most recent file if multiple matches
        selected = matches[-1]
        print(f"✓ Found: {selected.relative_to(base_dir)}")
        args.extend(["--inject", str(selected)])
    return args


def main() -> int:
    parser = argparse.ArgumentParser(description="Pack a workload-specific pixi environment.")
    parser.add_argument(
        "--env",
        default="packaged-cluster",
        help="Pixi environment name to pack.",
    )
    parser.add_argument(
        "--platform",
        choices=["linux-64", "linux-aarch64", "osx-arm64", "auto"],
        default="auto",
        help="Target platform for pixi-pack.",
    )
    parser.add_argument(
        "--build-missing",
        action="store_true",
        help="Build missing artifacts before packing.",
    )
    parser.add_argument(
        "--allow-missing-injects",
        action="store_true",
        help="Skip missing inject artifacts instead of failing.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the pixi-pack command and exit without executing.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print detailed resolution information before running.",
    )
    args = parser.parse_args()

    # Determine workspace root (assumes script is in workspace_root/scripts/)
    base_dir = Path(__file__).resolve().parents[1]

    if args.platform == "auto":
        env_platform = os.getenv("SLURM_PACK_PLATFORM")
        platform_value = env_platform or _detect_platform()
    else:
        platform_value = args.platform

    # Try to find all inject artifacts
    try:
        inject_args = _resolve_inject_args(base_dir, args.allow_missing_injects)
    except FileNotFoundError as exc:
        if not args.build_missing:
            raise

        # Build missing artifacts
        print(f"\n🔨 Building missing artifacts...")
        print(f"    {exc}\n")

        for build_dir, build_cmd in BUILD_COMMANDS:
            build_path = base_dir / build_dir
            if not build_path.exists():
                print(f"⚠️  Skipping {build_dir} (directory not found)")
                continue

            print(f"📦 Building in {build_dir}:")
            print(f"   $ {' '.join(build_cmd)}")

            build_result = subprocess.run(
                build_cmd,
                check=False,
                cwd=build_path,
                capture_output=True,
                text=True,
            )
            if build_result.stdout:
                print(build_result.stdout)
            if build_result.stderr:
                print(build_result.stderr)
            if build_result.returncode != 0:
                raise subprocess.CalledProcessError(
                    build_result.returncode, build_cmd, build_result.stdout, build_result.stderr
                )
            print(f"   ✓ Build completed\n")

        # Re-resolve inject args after building
        inject_args = _resolve_inject_args(base_dir, args.allow_missing_injects)

    cmd = [
        "pixi-pack",
        "--environment",
        args.env,
        "--platform",
        platform_value,
        "--create-executable",
        "--ignore-pypi-non-wheel",
        *inject_args,
        "pyproject.toml",
    ]

    if args.debug or args.dry_run:
        print(f"\n📋 Configuration:")
        print(f"   Environment: {args.env}")
        print(f"   Platform: {platform_value}")
        print(f"   Workspace: {base_dir}")
        print(f"   Injecting {len(inject_args)//2} artifacts")

    print(f"\n📦 Packing environment...")
    print(f"   $ {' '.join(cmd)}")

    if args.dry_run:
        print("\n✓ Dry run complete (no actual packing performed)")
        return 0

    result = subprocess.run(
        cmd,
        check=False,
        cwd=base_dir,
        capture_output=True,
        text=True,
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        print(f"\n❌ Packing failed with exit code {result.returncode}")
        return result.returncode

    # Rename the packed environment to avoid collisions across workloads/platforms
    packed_path = base_dir / "environment.sh"
    if packed_path.exists():
        stamp = time.strftime("%Y%m%d-%H%M%S")
        safe_env = args.env.replace("/", "_")
        target = base_dir / f"environment-{safe_env}-{platform_value}-{stamp}.sh"
        packed_path.rename(target)
        size_mb = target.stat().st_size / (1024 * 1024)
        print(f"\n✅ Successfully created packed environment:")
        print(f"   {target.relative_to(base_dir)}")
        print(f"   Size: {size_mb:.1f} MB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
