# dagster-slurm

[![image](https://img.shields.io/pypi/v/dagster-slurm.svg)](https://pypi.python.org/pypi/dagster-slurm)
[![image](https://img.shields.io/pypi/l/dagster-slurm.svg)](https://pypi.python.org/pypi/dagster-slurm)
[![image](https://img.shields.io/pypi/pyversions/dagster-slurm.svg)](https://pypi.python.org/pypi/dagster-slurm)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

---

Integration for running Dagster assets on Slurm HPC clusters.

## ✨ features

- **Unified API**: Same asset code works in dev (local) for rapid prototyping but just as well on a massive HPC system via SLURM.
- **Job launch**: Currently we have one job per dagster asset (unless you use multi-asset jobs). We are exploring operator fusion here: https://github.com/ascii-supply-networks/dagster-slurm/issues/22 please share your thoughts with us
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


### 🏗️ Architecture

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