---
sidebar_position: 2
title: Package environments
---

Reliable environment packaging ensures that assets behave the same on your laptop and on the cluster. `dagster-slurm` relies on pixi for reproducible builds and can either package environments on demand or reuse pre-deployed copies.

## Required environment variables

The packaging tasks read your SSH credentials and remote destination from the environment. Set these before running `pixi run deploy-prod-docker` (or your own wrapper script):

| Variable | Purpose | Notes |
| --- | --- | --- |
| `SLURM_EDGE_NODE_HOST` | SSH hostname of the edge/login node. | Example: `vsc5.vsc.ac.at`. |
| `SLURM_EDGE_NODE_PORT` | SSH port. | Defaults to `22`. |
| `SLURM_EDGE_NODE_USER` | Username for SSH and file uploads. | Must have write access to the deployment path. |
| `SLURM_EDGE_NODE_KEY_PATH` | Path to the SSH private key (key-based auth). | Expandable paths like `~/.ssh/id_ed25519`. |
| `SLURM_EDGE_NODE_PASSWORD` | Password for SSH (password auth). | Mutually exclusive with `SLURM_EDGE_NODE_KEY_PATH`. |
| `SLURM_DEPLOYMENT_BASE_PATH` | Root directory on the cluster where environments are stored. | Used by CI deploy scripts. e.g. `/home/user/dagster-slurm`. |
| `CI_DEPLOYED_ENVIRONMENT_PATH` | Path to pre-deployed environment (production mode). | Required when `DAGSTER_DEPLOYMENT=production_*`. Set by CI after deployment. |
| `DAGSTER_DEPLOYMENT` | Controls execution mode and resource configuration. | Values: `development`, `staging_docker`, `production_docker`, `staging_supercomputer`, `production_supercomputer`, etc. |

Only one of `SLURM_EDGE_NODE_KEY_PATH` or `SLURM_EDGE_NODE_PASSWORD` should be set. The packaging script validates this and fails fast if both or neither are supplied.

## Pack on demand

By default, `ComputeResource` packages the active pixi environment every time an asset runs in a Slurm-backed mode.

```python
compute = ComputeResource(
    mode="slurm",
    slurm=slurm_resource,
    default_launcher=BashLauncher(),
    auto_detect_platform=True,
)
```

- `auto_detect_platform=True` chooses the correct target (`linux-64`, `linux-aarch64`, etc.) based on the edge node architecture.
- Use `pack_platform="linux-64"` to override detection explicitly.
- Set `debug_mode=True` while iterating to keep build artefacts in place for inspection.

### ComputeResource configuration options

| Option | Default | Description |
|--------|---------|-------------|
| `pre_deployed_env_path` | `None` | Path to pre-deployed environment on cluster. When set, skips packing entirely. |
| `default_skip_payload_upload` | `False` | When `True`, skip uploading payload scripts. Remote path is derived as `{pre_deployed_env_path}/scripts/{filename}`. |
| `cache_inject_globs` | `None` | Glob patterns for inject files that affect cache key. If not set, all inject files are hashed. |
| `auto_detect_platform` | `False` | Auto-detect target platform from edge node architecture. |
| `pack_platform` | `None` | Explicit target platform (e.g., `linux-64`). |
| `debug_mode` | `False` | Keep build artefacts for inspection. |

## Environment caching

`dagster-slurm` automatically caches packed environments on the remote cluster to avoid re-uploading unchanged environments.
The cache key is computed from:

1. **pixi.lock** - The lockfile contents
2. **Pack command** - Platform, environment name, and other options
3. **Injected packages** - Contents of files passed via `--inject`

### Selective cache invalidation with `cache_inject_globs`

When using `pixi-pack --inject` to include local packages, you may want only *base* library changes to invalidate the cache, while *workload-specific* packages are excluded.
This is useful when:

- Your pack command injects multiple packages (base libs + workload code)
- Workload code changes frequently but shouldn't trigger full environment re-uploads
- The actual job script (payload) is uploaded separately each run

```python
compute = ComputeResource(
    mode="slurm",
    slurm=slurm_resource,
    default_launcher=BashLauncher(),
    # Only these inject patterns affect the cache key
    cache_inject_globs=[
        "../dist/dagster_slurm-*-py3-none-any.whl",
        "projects/dagster-slurm-example-shared/dist/*.conda",
    ],
)
```

With this configuration:

| What changed | Cache invalidated? | Action |
|--------------|-------------------|--------|
| `pixi.lock` | Yes | Re-pack and upload |
| `dagster_slurm-*.whl` (base lib) | Yes | Re-pack and upload |
| `shared/*.conda` (shared lib) | Yes | Re-pack and upload |
| `workload/*.conda` (excluded) | No | Reuse cached environment |
| `process.py` (payload script) | N/A | Always uploaded fresh |

If `cache_inject_globs` is not set, **all** `--inject` files from the pack command are hashed (safe default).

## Runtime configuration with SlurmRunConfig

Use `SlurmRunConfig` to make environment and payload settings configurable at job submission time via the Dagster launchpad.
This replaces hardcoded asset metadata with proper Dagster configuration.

```python
import dagster as dg
from dagster_slurm import ComputeResource, SlurmRunConfig

@dg.asset
def my_asset(
    context: dg.AssetExecutionContext,
    compute: ComputeResource,
    config: SlurmRunConfig,  # Configurable via launchpad
):
    return compute.run(
        context=context,
        payload_path="script.py",
        config=config,
    ).get_results()
```

### Available options

| Option | Default | Description |
|--------|---------|-------------|
| `force_env_push` | `False` | Force re-pack and upload the environment even when cached. Useful after manual changes to injected packages. |
| `skip_payload_upload` | `False` | Skip uploading the payload script. Use when the script already exists on the remote. |
| `remote_payload_path` | `None` | Custom remote path for the payload when `skip_payload_upload=True`. |

### When to use SlurmRunConfig

- **Development iteration**: Set `force_env_push=True` in the launchpad when you've changed base dependencies but the cache key didn't update.
- **Pre-deployed payloads**: Set `skip_payload_upload=True` when job scripts are deployed via CI/CD rather than uploaded per-run.
- **Debugging**: Override settings without modifying code.

### Precedence rules

Settings are resolved in this order (first match wins):

1. **Explicit parameters** passed to `compute.run(..., force_env_push=True)`
2. **SlurmRunConfig** values from the launchpad
3. **Asset metadata** (legacy approach, still supported)
4. **ComputeResource defaults** (`default_skip_payload_upload`)
5. **Built-in defaults** (`force_env_push=False`, `skip_payload_upload=False`)

## Pre-deployed environments (production)

For production workloads, deploy the environment via CI/CD instead of packaging on every run.

### CI deployment

```bash title="Deploy from CI"
pixi run deploy-prod-docker
```

The command writes the remote path to `deployment_metadata.json`. Your CI pipeline should:
1. Run the deploy command
2. Export `CI_DEPLOYED_ENVIRONMENT_PATH` with the deployed path

### Automatic production configuration

When `DAGSTER_DEPLOYMENT=production_*`, the example resources automatically:

1. Read `CI_DEPLOYED_ENVIRONMENT_PATH` as `pre_deployed_env_path`
2. Set `default_skip_payload_upload=True`
3. Derive payload paths as `{pre_deployed_env_path}/scripts/{filename}`

```python
# From resources/__init__.py - automatic when DAGSTER_DEPLOYMENT=production_*
if _is_production(deployment):
    pre_deployed_env_path = os.environ.get("CI_DEPLOYED_ENVIRONMENT_PATH")
    config["compute_config"]["pre_deployed_env_path"] = pre_deployed_env_path
    config["compute_config"]["default_skip_payload_upload"] = True
```

Assets require no special configuration:

```python
@dg.asset
def production_asset(
    context: dg.AssetExecutionContext,
    compute: ComputeResource,
    config: SlurmRunConfig,
):
    return compute.run(
        context=context,
        payload_path="process.py",  # Resolves to {CI_DEPLOYED_ENVIRONMENT_PATH}/scripts/process.py
        config=config,
    ).get_results()
```

:::note
When using `pre_deployed_env_path`, environment caching and `cache_inject_globs` have no effect—the pre-deployed environment is used directly.
:::

## Per-asset environment overrides

Different assets may require different environments (e.g., GPU vs CPU, different library versions). Use asset metadata to override the pack command or pre-deployed path per asset.

### Custom pack command per asset

Use `slurm_pack_cmd` metadata to specify a different pixi task for packaging:

```python
@dg.asset(
    metadata={
        "slurm_pack_cmd": ["pixi", "run", "--frozen", "pack-gpu"],  # Uses GPU environment
    }
)
def gpu_training(context: dg.AssetExecutionContext, compute: ComputeResource):
    return compute.run(
        context=context,
        payload_path="train_gpu.py",
    ).get_results()


@dg.asset(
    metadata={
        "slurm_pack_cmd": ["pixi", "run", "--frozen", "pack-cpu"],  # Uses CPU environment
    }
)
def cpu_inference(context: dg.AssetExecutionContext, compute: ComputeResource):
    return compute.run(
        context=context,
        payload_path="infer_cpu.py",
    ).get_results()
```

Define the corresponding tasks in your `pyproject.toml`:

```toml
[tool.pixi.tasks]
pack-gpu = "pixi-pack --environment gpu-cluster --platform linux-64 ..."
pack-cpu = "pixi-pack --environment cpu-cluster --platform linux-64 ..."
```

### Pre-deployed environment per asset

Use `slurm_pre_deployed_env_path` metadata to point specific assets at different pre-deployed environments:

```python
@dg.asset(
    metadata={
        "slurm_pre_deployed_env_path": "/opt/envs/ml-gpu-v2",
    }
)
def ml_training(context: dg.AssetExecutionContext, compute: ComputeResource):
    ...

@dg.asset(
    metadata={
        "slurm_pre_deployed_env_path": "/opt/envs/data-processing-v1",
    }
)
def data_processing(context: dg.AssetExecutionContext, compute: ComputeResource):
    ...
```

:::tip
Per-asset overrides take precedence over `ComputeResource` defaults. This allows mixing live-packaged and pre-deployed environments in the same pipeline.
:::

## Managing multiple environments

Use unique deployment paths per branch or release to test upgrades safely:

```bash
# Version the deployment path in CI
export CI_DEPLOYED_ENVIRONMENT_PATH="/home/submitter/deployments/my-project/v2024.02"
```

CI can update the environment by re-running `pixi run deploy-prod-docker` and bumping the version suffix.

## Common troubleshooting tips

- Ensure pixi is available inside your CI runner or deployment environment.
- If packaging fails with missing compilers, add them to your pixi environment (e.g. `gxx_linux-64`).
- When switching between GPU and CPU builds, clean the remote deployment directory or use different target paths to avoid stale binaries.
