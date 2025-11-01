# dagster-slurm

[![image](https://img.shields.io/pypi/v/dagster-slurm.svg)](https://pypi.python.org/pypi/dagster-slurm)
[![image](https://img.shields.io/pypi/l/dagster-slurm.svg)](https://pypi.python.org/pypi/dagster-slurm)
[![image](https://img.shields.io/pypi/pyversions/dagster-slurm.svg)](https://pypi.python.org/pypi/dagster-slurm)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

---

Integration for running Dagster assets on Slurm HPC clusters.

`dagster-slurm` lets you take the same Dagster assets from a laptop to a Slurm-backed supercomputer with minimal configuration changes.

**An European sovereign GPU cloud does not come out of nowhere
maybe this project can support making HPC systems more accessible**.

See the [docs](https://dagster-slurm.geoheil.com/) for instructions.

> **Status:** Local execution and one-job-per-asset Slurm runs are production-ready today. Session-based cluster reuse and heterogeneous jobs are actively being developed and will land in a future release.

<img referrerpolicy="no-referrer-when-downgrade" src="https://telemetry.dagster-slurm.geoheil.com/a.png?x-pxid=994a20b8-4be7-4297-9f42-657b0d1f1a07&page=README.md" />

## ✨ features

Hydrating supercomputers with Dagster and Slurm to enhance developer experience.

![](docs/static/img/featured.png)

- **Unified API**: Same asset code works in dev (local) for rapid prototyping but just as well on a massive HPC system via SLURM.
- **Job launch**: One Slurm submission per Dagster asset keeps behaviour predictable. Operator-fusion/session reuse is on the roadmap (feedback welcome in [#22](https://github.com/ascii-supply-networks/dagster-slurm/issues/22)).
- **Pluggable Launchers**: Bash, Ray, Spark (WIP) - easy to add more
- **Environment Packaging**: Automatic `pixi`-based environment packaging for remote execution via `pixi-pack`
- **Connection Pooling**: SSH ControlMaster for efficient remote operations
- **Metrics Collection**: Automatic collection of Slurm job metrics (CPU efficiency, memory, node-hours)
- **Production-Prepared**: Proper error handling, cleanup, health checks

### 📊 Metrics 

Automatic metrics collection for all jobs: 

- Node-hours consumed
- CPU efficiency
- Max memory usage
- Elapsed time
     
Visible in Dagster UI metadata. 

![Screenshot comparing multiple Dagster runs](docs/static/img/process_data_run_view.png)


### 🏗️ Architecture

#### Components

- Your server coordinating HPC and non HPC jobs
- The HPC compute resource

![](./docs/static/img/arch-detail-dark.svg)

#### Layers of dagster-slurm

```
Asset Layer (user code)
  ↓
ComputeResource (facade)
  ↓
PipesClient (orchestration)
  ↓
Launcher (what to run) + SSH Pool (how to run)
  ↓
Slurm Execution
```

## Example

See [examples](./examples/) for a small tutorial which starts a slurm cluster in docker.
To use your own full-blown HPC deployment you would have to adapt the SSH connection configuration accordingly.

## Resources

- 2025-11 introduction blog post https://georgheiler.com/post/dagster-slurm/
- 2025-10 hackathon slides https://dagster-slurm.geoheil.com/docs/slides

## Supporters

This work was kindly supported by:
- [ASCII](https://ascii.ac.at/)
- [CSH](https://www.csh.ac.at/)
- [EUROCC AI Hackathon](https://www.openhackathons.org/s/siteevent/a0CUP000013Tp8f2AC/se000375): This work was completed in part at the EUROCC AI Hackathon, part of the Open Hackathons program. The authors would like to acknowledge OpenACC-Standard.org for their support.
- [Austrian Scientific Computing (ASC)](https://asc.ac.at/home/)
