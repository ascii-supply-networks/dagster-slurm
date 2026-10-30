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
import sys
import time
from dataclasses import dataclass
from pathlib import Path

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
@dataclass(frozen=True)
class BuildTarget:
    pattern: str
    build_dir: str
    build_cmd: list[str]


BUILD_TARGETS = [
    BuildTarget(
        pattern="projects/dagster-slurm-example-shared/dist/dagster_slurm_example_shared-*.conda",
        build_dir="projects/dagster-slurm-example-shared",
        build_cmd=["pixi", "build", "-o", "dist"],
    ),
    BuildTarget(
        pattern="projects/dagster-slurm-example-hpc-workload/dist/dagster_slurm_example_hpc_workload-*.conda",
        build_dir="projects/dagster-slurm-example-hpc-workload",
        build_cmd=["pixi", "build", "-o", "dist"],
    ),
    BuildTarget(
        pattern="projects/dagster-slurm-example/dist/dagster_slurm_example-*.conda",
        build_dir="projects/dagster-slurm-example",
        build_cmd=["pixi", "build", "-o", "dist"],
    ),
    BuildTarget(
        pattern="../dist/dagster_slurm-*-py3-none-any.whl",
        build_dir="../projects",
        build_cmd=["pixi", "run", "-e", "build", "--frozen", "build-lib"],
    ),
]

INJECT_PATTERNS = [target.pattern for target in BUILD_TARGETS]

_IGNORED_SOURCE_DIRS = {
    ".git",
    ".hg",
    ".svn",
    ".pixi",
    ".venv",
    ".mypy_cache",
    ".pytest_cache",
    "__pycache__",
    "dist",
    "build",
}


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


def _latest_artifact(base_dir: Path, pattern: str) -> Path | None:
    matches = sorted(
        [path for path in base_dir.glob(pattern) if path.is_file()],
        key=lambda path: path.stat().st_mtime,
    )
    if not matches:
        return None
    return matches[-1]


def _latest_source_mtime(root: Path) -> float:
    latest_mtime = 0.0
    for path in root.rglob("*"):
        if any(part in _IGNORED_SOURCE_DIRS for part in path.parts):
            continue
        if path.is_file():
            latest_mtime = max(latest_mtime, path.stat().st_mtime)
    return latest_mtime


def _targets_requiring_build(base_dir: Path) -> list[BuildTarget]:
    targets: list[BuildTarget] = []
    for target in BUILD_TARGETS:
        build_dir = base_dir / target.build_dir
        if not build_dir.exists():
            continue

        artifact = _latest_artifact(base_dir, target.pattern)
        if artifact is None:
            targets.append(target)
            continue

        source_mtime = _latest_source_mtime(build_dir)
        if source_mtime > artifact.stat().st_mtime:
            targets.append(target)

    return targets


def _run_build(base_dir: Path, target: BuildTarget) -> None:
    build_dir = target.build_dir
    build_cmd = target.build_cmd
    build_path = base_dir / build_dir
    print(f"📦 Building in {build_dir}:")
    print(f"   $ {' '.join(build_cmd)}")

    build_process = subprocess.Popen(
        build_cmd,
        cwd=build_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    build_output_lines = []
    if build_process.stdout is not None:
        for line in build_process.stdout:
            print(line, end="")
            sys.stdout.flush()
            build_output_lines.append(line)

    build_returncode = build_process.wait()
    if build_returncode != 0:
        raise subprocess.CalledProcessError(
            build_returncode,
            build_cmd,
            "".join(build_output_lines),
            "",
        )
    print("   ✓ Build completed\n")


def main() -> int:  # noqa: C901
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
        help="Build missing or stale local artifacts before packing.",
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

    if args.build_missing:
        targets_to_build = _targets_requiring_build(base_dir)
        if targets_to_build:
            print("\n🔨 Building missing or stale artifacts...\n")
            for target in targets_to_build:
                _run_build(base_dir, target)

    # Try to find all inject artifacts
    try:
        inject_args = _resolve_inject_args(base_dir, args.allow_missing_injects)
    except FileNotFoundError as exc:
        if not args.build_missing:
            raise

        print("\n🔨 Building remaining missing artifacts...")
        print(f"    {exc}\n")

        for target in BUILD_TARGETS:
            build_path = base_dir / target.build_dir
            if not build_path.exists():
                print(f"⚠️  Skipping {target.build_dir} (directory not found)")
                continue
            _run_build(base_dir, target)

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
        print("\n📋 Configuration:")
        print(f"   Environment: {args.env}")
        print(f"   Platform: {platform_value}")
        print(f"   Workspace: {base_dir}")
        print(f"   Injecting {len(inject_args)//2} artifacts")

    print("\n📦 Packing environment...")
    print(f"   $ {' '.join(cmd)}")

    if args.dry_run:
        print("\n✓ Dry run complete (no actual packing performed)")
        return 0

    # Stream output while capturing (tee-like behavior)
    # Use unbuffered output to see progress immediately
    env = os.environ.copy()
    env['PYTHONUNBUFFERED'] = '1'

    process = subprocess.Popen(
        cmd,
        cwd=base_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=0,  # Unbuffered
        env=env,
    )

    output_lines = []
    if process.stdout is not None:
        for chunk in iter(lambda: process.stdout.read(1) if process.stdout else b'', b''):
            if not chunk:
                break
            char = chunk.decode('utf-8', errors='replace')
            print(char, end='')
            sys.stdout.flush()
            output_lines.append(char)

    returncode = process.wait()
    if returncode != 0:
        print(f"\n❌ Packing failed with exit code {returncode}")
        return returncode

    # Rename the packed environment to avoid collisions across workloads/platforms
    packed_path = base_dir / "environment.sh"
    if packed_path.exists():
        stamp = time.strftime("%Y%m%d-%H%M%S")
        safe_env = args.env.replace("/", "_")
        target = base_dir / f"environment-{safe_env}-{platform_value}-{stamp}.sh"
        packed_path.rename(target)
        size_mb = target.stat().st_size / (1024 * 1024)
        print("\n✅ Successfully created packed environment:")
        print(f"   {target.relative_to(base_dir)}")
        print(f"   Size: {size_mb:.1f} MB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
