# Repository Guidelines

This file provides guidance when working with code in this repository.

## Project Overview

`dagster-slurm` integrates Dagster with Slurm HPC clusters, allowing the same asset code to run locally during development and on Slurm clusters in production. The core package lives in `projects/dagster-slurm/dagster_slurm/`.

## Build, Test, and Development Commands

```bash
# Environment setup
pixi run -e build --frozen sync              # Create/update environment
pixi run -e build --frozen sync-lib-with-upgrade  # Refresh pins

# Format and lint
pixi run -e build --frozen fmt               # Format (ruff + yamllint + taplo)
pixi run -e build --frozen lint              # Lint (ruff + yamllint + taplo + pyrefly)

# Testing
pixi run -e build --frozen test              # Unit tests (skips slurm/slow)
pixi run -e build --frozen test-integration  # Integration tests (needs docker slurm)
pixi run -e build --frozen test-integration-slow  # Extended integration tests

# Run a single test file
pixi run -e build --frozen -- uv run pytest projects/dagster-slurm/dagster_slurm_test/test_launchers.py -v

# Run a specific test
pixi run -e build --frozen -- uv run pytest projects/dagster-slurm/dagster_slurm_test/test_launchers.py::test_bash_launcher_prepare_execution -v

# Documentation
pixi run -e docs --frozen docs-serve         # Local preview
pixi run -e docs --frozen docs-build         # Build static output

# Run examples (needs slurm docker)
pixi run -e build --frozen start             # Start example in examples/
```

## Architecture

### Layered Design

```
Asset Layer (user code)
    ↓
ComputeResource (facade) ← Main entry point for users
    ↓
PipesClient (orchestration) ← SlurmPipesClient or LocalPipesClient
    ↓
Launcher (what to run) ← BashLauncher, RayLauncher, SparkLauncher
    ↓
SSH Pool + Slurm Execution
```

### Key Modules

- `resources/compute.py` - `ComputeResource`: main facade, hides local/slurm/session complexity
- `resources/slurm.py` - `SlurmResource`: slurm cluster configuration (SSH, queue settings)
- `pipes_clients/slurm_pipes_client.py` - `SlurmPipesClient`: handles job submission, log streaming, metrics
- `launchers/` - Execution plan generators:
  - `script.py` - `BashLauncher`: simple shell script execution
  - `ray.py` - `RayLauncher`: multi-node Ray cluster with robust startup/shutdown
  - `spark.py` - `SparkLauncher`: Spark support (WIP)
- `helpers/ssh_pool.py` - SSH connection pooling with ControlMaster
- `helpers/env_packaging.py` - `pixi-pack` based environment packaging for remote execution

### Execution Modes

1. **local** - No SSH/Slurm, direct execution (dev)
2. **slurm** - Each asset = separate sbatch job (prod)

HET job and session mode are experimental and not fully implemented yet.

### Typical Asset Pattern

```python
@dg.asset
def my_asset(context: dg.AssetExecutionContext, compute: ComputeResource):
    return compute.run(
        context=context,
        payload_path="script.py",
        extra_env={"KEY": "value"},
    ).get_results()
```

## Testing Guidelines

- Unit tests in `projects/dagster-slurm/dagster_slurm_test/`
- Use `@pytest.mark.needs_slurm_docker` for integration tests requiring slurm
- Use `@pytest.mark.slow` for long-running tests
- Include both local-mode and slurm-marked variants when adding HPC features

## Coding Conventions

- Python 3.12, 4-space indentation
- Type hints on public APIs
- Conventional Commits (`feat:`, `fix:`, `chore:`)
