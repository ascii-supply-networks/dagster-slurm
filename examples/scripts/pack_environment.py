import argparse
import os
import platform
import subprocess
from pathlib import Path
import time


INJECT_PATTERNS = [
    "projects/dagster-slurm-example-shared/dist/dagster_slurm_example_shared-*.conda",
    "../dist/dagster_slurm-*-py3-none-any.whl",
    "projects/dagster-slurm-example-hpc-workload/dist/dagster_slurm_example_hpc_workload-*.conda",
    "projects/dagster-slurm-example/dist/dagster_slurm_example-*.conda",
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
    args: list[str] = []
    for pattern in INJECT_PATTERNS:
        matches = sorted(
            [p for p in base_dir.glob(pattern) if p.is_file()],
            key=lambda p: p.stat().st_mtime,
        )
        if not matches:
            if allow_missing:
                print(f"Skipping missing inject pattern: {pattern}")
                continue
            raise FileNotFoundError(f"No files matched inject pattern: {pattern}")
        args.extend(["--inject", str(matches[-1])])
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

    base_dir = Path(__file__).resolve().parents[1]
    repo_root = base_dir.parent
    if args.platform == "auto":
        env_platform = os.getenv("SLURM_PACK_PLATFORM")
        platform_value = env_platform or _detect_platform()
    else:
        platform_value = args.platform
    try:
        inject_args = _resolve_inject_args(base_dir, args.allow_missing_injects)
    except FileNotFoundError as exc:
        if not args.build_missing:
            raise
        print(f"{exc} -> building missing artifacts")
        build_cmd = ["pixi", "run", "-e", "build", "--frozen", "build-lib"]
        print(f"Running build: {' '.join(build_cmd)}")
        build_result = subprocess.run(
            build_cmd, check=False, cwd=repo_root, capture_output=True, text=True
        )
        if build_result.stdout:
            print(build_result.stdout)
        if build_result.stderr:
            print(build_result.stderr)
        if build_result.returncode != 0 and "unknown environment 'build'" in build_result.stderr.lower():
            print("Build env not found in current manifest; retrying with repo root manifest")
            build_env = os.environ.copy()
            build_env["PIXI_PROJECT_MANIFEST"] = str(repo_root / "pyproject.toml")
            build_result = subprocess.run(
                build_cmd,
                check=False,
                cwd=repo_root,
                capture_output=True,
                text=True,
                env=build_env,
            )
            if build_result.stdout:
                print(build_result.stdout)
            if build_result.stderr:
                print(build_result.stderr)
        if build_result.returncode != 0:
            raise subprocess.CalledProcessError(
                build_result.returncode, build_cmd, build_result.stdout, build_result.stderr
            )
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
        print(f"Using environment: {args.env}")
        print(f"Resolved platform: {platform_value}")
        print(f"Inject args: {inject_args}")
    print(f"Running: {' '.join(cmd)}")
    if args.dry_run:
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
        return result.returncode

    # Rename the packed environment to avoid collisions across workloads/platforms.
    packed_path = base_dir / "environment.sh"
    if packed_path.exists():
        stamp = time.strftime("%Y%m%d-%H%M%S")
        safe_env = args.env.replace("/", "_")
        target = base_dir / f"environment-{safe_env}-{platform_value}-{stamp}.sh"
        packed_path.rename(target)
        print(f"Created pack at {target} with size {target.stat().st_size}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
