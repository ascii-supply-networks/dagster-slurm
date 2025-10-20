---
sidebar_position: 1
---

# Getting started

`dagster-slurm` lets you take the same Dagster assets from a laptop to a Slurm-backed supercomputer with minimal configuration changes. This page walks through the demo environment bundled with the repository and highlights the key concepts you will reuse on your own cluster.

## Prerequisites

- [pixi](https://pixi.sh/latest/installation/) (`curl -fsSL https://pixi.sh/install.sh | sh`)
- `pixi global install git`
- Docker (or compatible runtime) with the `docker compose` plugin available

## Fetch the repository

```bash
git clone https://github.com/ascii-supply-networks/dagster-slurm.git
cd dagster-slurm
docker compose up -d
cd examples
```

The Docker compose stack starts a local Dagster control plane, a Slurm edge node, and a compute partition to mirror a typical HPC setup.

## 1. Develop locally (no Slurm)

For rapid iteration, execute assets directly on your workstation:

```bash
pixi run start
```

Navigate to [http://localhost:3000](http://localhost:3000) to view the Dagster UI with assets running in-process.

## 2. Exercise the bundled Slurm cluster

Switching to Slurm-backed execution only requires environment variables that describe the edge node. Create an `.env` file with:

```dotenv
SLURM_EDGE_NODE_HOST=localhost
SLURM_EDGE_NODE_PORT=2223
SLURM_EDGE_NODE_USER=submitter
SLURM_EDGE_NODE_PASSWORD=submitter
SLURM_DEPLOYMENT_BASE_PATH=/home/submitter/pipelines/deployments
```

Then run:

```bash
pixi run start-staging
```

Assets now submit through Slurm, and Dagster displays job logs, status, and resource metadata collected from the cluster.

## 3. Prepare production-style runs

Production environments typically reuse pre-built runtimes to avoid the startup cost of packaging dependencies on each run. The demo shows this by pre-deploying the pixi environment and pointing the resource configuration at it.

```bash
pixi run deploy-prod-docker  # builds and uploads the pixi environment
```

Inspect the generated metadata and persist the target path:

```bash
deployment_path="$(jq -er '.deployment_path' deployment_metadata.json)"
echo "DAGSTER_PROD_ENV_PATH=${deployment_path}" > .env.prod
export DAGSTER_PROD_ENV_PATH="${deployment_path}"
```

Then start Dagster in production mode:

```bash
pixi run start-prod-docker
```

Jobs now skip environment packaging and launch noticeably faster.

## 4. Point to your own HPC cluster

1. Update the SSH and Slurm configuration in
   [`examples/projects/dagster-slurm-example/dagster_slurm_example/resources/__init__.py`](https://github.com/ascii-supply-networks/dagster-slurm/blob/main/examples/projects/dagster-slurm-example/dagster_slurm_example/resources/__init__.py)
   (or your own equivalent module).
2. Provide the connection details via environment variables—`dagster-slurm` reads them at runtime so you can keep secrets out of the repository.

### Required environment variables

| Variable | Purpose | Notes |
| --- | --- | --- |
| `SLURM_EDGE_NODE_HOST` | SSH hostname of the login/edge node. | - |
| `SLURM_EDGE_NODE_PORT` | SSH port. | Defaults to `22` on most clusters. |
| `SLURM_EDGE_NODE_USER` | Username used for SSH and job submission. | Often tied to an LDAP or project account. |
| `SLURM_EDGE_NODE_PASSWORD` / `SLURM_EDGE_NODE_KEY_PATH` | Authentication method. | Prefer key-based auth; set whichever your site supports. |
| `SLURM_EDGE_NODE_FORCE_TTY` (optional) | Request a pseudo-terminal (`-tt`). | Set to `true` on clusters that insist on interactive sessions. Leave `false` when using a jump host. |
| `SLURM_EDGE_NODE_POST_LOGIN_COMMAND` (optional) | Command prefix run immediately after login. | Supports `{cmd}` placeholder; useful when you cannot use `ProxyJump`. |
| `SLURM_EDGE_NODE_JUMP_HOST` / `_USER` / `_PORT` / `_PASSWORD` (optional) | Configure an SSH jump host (uses `ssh -J`). | Lets you hop via `vmos`/bastion nodes; password-based auth is supported. |
| `SLURM_DEPLOYMENT_BASE_PATH` | Remote directory where dagster-slurm uploads job bundles. | Should be writable and have sufficient quota. |
| `SLURM_PARTITION` | Default partition/queue name. | Override per asset for specialised queues. |
| `SLURM_GPU_PARTITION` (optional) | GPU-enabled partition. | Useful when mixing CPU and GPU jobs. |
| `SLURM_QOS` (optional) | QoS or account string. | Required on clusters that enforce QoS selection. |
| `SLURM_SUPERCOMPUTER_SITE` (optional) | Enables site-specific overrides (`vsc5`, `leonardo`, …). | Adds TTY/post-login hops or queue defaults. |
| `DAGSTER_DEPLOYMENT` | Selects the resource preset (`development`, `staging_docker`, `production_supercomputer`, …). | See `Environment` enum in the example project. |
| `CI_DEPLOYED_ENVIRONMENT_PATH` (production only) | Path to a pre-built environment bundle on the cluster. | Required when using `production_supercomputer`. |
| `DAGSTER_SLURM_SSH_CONTROL_DIR` (optional) | Directory for SSH ControlMaster sockets. | Override when `/tmp` is not writable; defaults to `~/.ssh/dagster-slurm`. |

Set the variables in a `.env` file or your orchestrator’s secret store. Passwords are shown below for completeness, but most HPC centres require SSH keys or Kerberos tickets instead.

> **Note:** Some clusters (including VSC-5) forbid SSH ControlMaster sockets. When that happens `dagster-slurm` automatically switches to one-off SSH connections so jobs keep running—there’s no extra configuration needed, although log streaming may be slightly slower. Set `DAGSTER_SLURM_SSH_CONTROL_DIR` if your security policy restricts where control sockets can live.

### Sample configuration: Vienna Scientific Cluster (VSC-5)

```dotenv title=".env.vsc5"
# SSH / edge node access
SLURM_EDGE_NODE_HOST=vsc5.vsc.ac.at
SLURM_EDGE_NODE_PORT=22
SLURM_EDGE_NODE_USER=your_vsc_username
SLURM_EDGE_NODE_KEY_PATH=/Users/you/.ssh/id_ed25519_vsc5
SLURM_EDGE_NODE_JUMP_HOST=vmos.vsc.ac.at        # optional, but recommended
SLURM_EDGE_NODE_JUMP_USER=your_vsc_username
SLURM_EDGE_NODE_JUMP_PASSWORD=...              # only if vmos requires a password

# Deployment settings
SLURM_DEPLOYMENT_BASE_PATH=/home/your_vsc_username/dagster-slurm
SLURM_PARTITION=zen3_0512                      # pick a queue you can access
SLURM_QOS=zen3_0512_devel                      # optional QoS/account string
SLURM_RESERVATION=dagster-slurm_21             # optional reservation (if active)
SLURM_SUPERCOMPUTER_SITE=vsc5

# Dagster deployment selector
DAGSTER_DEPLOYMENT=production_supercomputer
```

VSC-5 prefers key-based authentication; ensure your SSH config allows agent forwarding or provide the key path above. Replace the queue/QoS/reservation values with the combinations granted to your project (for example `batch`, `short`, or a project-specific reservation).  
If your policies require password-only access, set `SLURM_EDGE_NODE_PASSWORD` and `SLURM_EDGE_NODE_JUMP_PASSWORD`; the same automation answers both prompts (you'll still need to handle one-time passcodes manually when they expire). Dagster prints `Enter … for vsc5.vsc.ac.at:` on your terminal—type the OTP there to continue. Password-based sessions automatically request a pseudo-TTY, so you only need `SLURM_EDGE_NODE_FORCE_TTY=true` if your site mandates it even for key-based authentication.

> **Cleanup behaviour:** Completed runs trigger an asynchronous `rm -rf` of the run directory on the edge node. This keeps quotas tidy without delaying Dagster shutdown. Set `debug_mode=True` on the relevant `ComputeResource` while debugging to keep run folders around for manual inspection.

### Sample configuration: Leonardo (CINECA)

```dotenv title=".env.leonardo"
SLURM_EDGE_NODE_HOST=login01-ext.leonardo.cineca.it
SLURM_EDGE_NODE_PORT=2222
SLURM_EDGE_NODE_USER=a08trb02               # replace with your CINECA username
SLURM_EDGE_NODE_KEY_PATH=/Users/you/.ssh/id_rsa_leonardo

SLURM_DEPLOYMENT_BASE_PATH=/leonardo/home/userexternal/a08trb02/dagster-slurm
SLURM_PARTITION=batch                        # default CPU queue
SLURM_QOS=normal
SLURM_SUPERCOMPUTER_SITE=leonardo
DAGSTER_DEPLOYMENT=production_supercomputer
```

Leonardo requires you to have an active project allocation; ensure the partition (`batch`, `cm`, `dcgpusr`, or `boost`) matches your access level. GPU queues also need the matching QoS (e.g. `dcgpuqos`). If your site enforces OTP/Kerberos, configure a local `~/.ssh/config` entry or a bastion jump host—`dagster-slurm` will reuse that setup automatically.

With the variables defined, restart your Dagster code location. To dry-run against the real scheduler while still allowing on-the-fly environment packaging, point `DAGSTER_DEPLOYMENT=staging_supercomputer` and run:

```bash
pixi run start-staging-supercomputer
```

For production workloads you should publish the environment bundle ahead of time (e.g. via CI using `python scripts/deploy_environment.py`). Once you export the uploaded path as `CI_DEPLOYED_ENVIRONMENT_PATH`, switch to `DAGSTER_DEPLOYMENT=production_supercomputer` and start Dagster:

```bash
pixi run start-production-supercomputer
```

The production preset refuses to start if `CI_DEPLOYED_ENVIRONMENT_PATH` is missing, ensuring clusters never build environments during business-critical runs.

To confirm the job landed on the expected queue, open an interactive shell on the cluster and run `squeue -j <jobid> -o '%i %P %q %R %T'`. The `Partition`, `QoS`, and `Reservation` columns should match your `.env` overrides.

## Execution modes

`ComputeResource` adapts to four execution modes controlled by the `ExecutionMode` enum:

| Mode | Description | Typical use |
| --- | --- | --- |
| `local` | Runs assets without SSH or Slurm. | Developer laptops and CI smoke tests. |
| `slurm` | Submits one Slurm job per asset execution. | Staging clusters and simple production setups. |
| `slurm-session` | Reuses Slurm allocations and (optionally) Ray/Spark clusters. | Long-running production deployments. |
| `slurm-hetjob` | Builds heterogeneous job submissions for mixed resource requirements. | Optimising GPU/CPU mixes in a single submission. |

Launchers (Bash, Ray, Spark, or custom) can be chosen globally or per asset to fit your workload.

## API examples

### Control plane (Dagster asset)

This code lives in your Dagster deployment (e.g. [local-data-stack](https://github.com/l-mds/local-data-stack/)):

```python
import dagster as dg
from dagster_slurm import ComputeResource


@dg.asset
def process_data(
    context: dg.AssetExecutionContext,
    compute: ComputeResource,
):
    """Simple dagster-slurm example asset."""
    script_path = dg.file_relative_path(
        __file__,
        "../../../../dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/shell/myfile.py",
    )
    completed_run = compute.run(
        context=context,
        payload_path=script_path,
        extra_env={"KEY_ENV": "value"},
        extras={"foo": "bar"},
    )
    yield from completed_run.get_results()
```

### User plane (remote workload)

```python
import os
from dagster_pipes import PipesContext, open_dagster_pipes


def main() -> None:
    context = PipesContext.get()
    context.log.info("Starting data processing...")
    context.log.debug(context.extras)
    key_env = os.environ.get("KEY_ENV")
    context.log.info(f"KEY_ENV: {key_env}")
    context.log.info(f"foo: {context.extras['foo']}")

    result = {"rows_processed": 1000}
    context.report_asset_materialization(
        metadata={
            "rows": result["rows_processed"],
            "processing_time": "10s",
        }
    )
    context.log.info("Processing complete!")


if __name__ == "__main__":
    with open_dagster_pipes() as context:
        main()
```

### Resource configuration

Configuration snippets for local, Docker-backed Slurm, and real HPC clusters are maintained in the [example project resources module](https://github.com/ascii-supply-networks/dagster-slurm/blob/main/examples/projects/dagster-slurm-example/dagster_slurm_example/resources/__init__.py). Adapt those templates to match your SSH endpoint, partitions, and queue limits.
