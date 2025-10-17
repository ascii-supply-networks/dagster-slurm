---
sidebar_position: 2
title: Package environments
---

Reliable environment packaging ensures that assets behave the same on your laptop and on the cluster. `dagster-slurm` relies on pixi for reproducible builds and can either package environments on demand or reuse pre-deployed copies.

## Required environment variables

The packaging tasks read your SSH credentials and remote destination from the environment. Set these before running `pixi run deploy-prod-docker` (or your own wrapper script):

| Variable | Purpose | Notes |
| --- | --- | --- |
| `SLURM_EDGE_NODE_HOST` | SSH hostname of the edge/login node. | Example: `login.vsc5.tuwien.ac.at`. |
| `SLURM_EDGE_NODE_PORT` | SSH port. | Defaults to `22`; Leonardo uses `2222`. |
| `SLURM_EDGE_NODE_USER` | Username for SSH and file uploads. | Must have write access to the deployment path. |
| `SLURM_EDGE_NODE_KEY_PATH` | Path to the SSH private key (key-based auth). | Expandable paths like `~/.ssh/id_ed25519`. |
| `SLURM_EDGE_NODE_PASSWORD` | Password for SSH (password auth). | Mutually exclusive with `SLURM_EDGE_NODE_KEY_PATH`. |
| `SLURM_DEPLOYMENT_BASE_PATH` | Root directory on the cluster where environments are stored. | e.g. `/home/user/dagster-slurm`. |

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

## Reuse pre-built environments

For production workloads avoid packaging on every run by deploying the environment out-of-band.

```bash title="Deploy from CI"
pixi run deploy-prod-docker
```

The command writes metadata to `deployment_metadata.json` containing the remote path:

```bash
deployment_path="$(jq -er '.deployment_path' deployment_metadata.json)"
```

Point `ComputeResource` at that path:

```python
compute = ComputeResource(
    mode="slurm-session",
    slurm=slurm_resource,
    session=session_resource,
    default_launcher=RayLauncher(),
    pre_deployed_env_path=deployment_path,
)
```

Jobs now start immediately using the pre-installed environment.

## Managing multiple environments

Use unique deployment paths per branch or per release train to test upgrades safely:

```bash
export DAGSTER_PROD_ENV_PATH="/home/submitter/pipelines/deployments/my-project/v2024.02"
```

CI can update the environment by re-running `pixi run deploy-prod-docker` and bumping the version suffix.

## Common troubleshooting tips

- Ensure pixi is available inside your CI runner or deployment environment.
- If packaging fails with missing compilers, add them to your pixi environment (e.g. `gxx_linux-64`).
- When switching between GPU and CPU builds, clean the remote deployment directory or use different target paths to avoid stale binaries.
