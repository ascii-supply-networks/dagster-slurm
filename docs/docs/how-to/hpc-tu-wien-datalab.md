---
sidebar_position: 99
title: HPC - TU Wien Datalab
---

## Sample configuration: TU Wien Datalab

```dotenv title=".env.datalab"
# SSH / cluster login node access
SLURM_EDGE_NODE_HOST=cluster.datalab.tuwien.ac.at
SLURM_EDGE_NODE_PORT=22
SLURM_EDGE_NODE_USER=user.name                  # replace with your TU Datalab username
SLURM_EDGE_NODE_KEY_PATH=/Users/you/.ssh/id_datalab

# Deployment settings
SLURM_DEPLOYMENT_BASE_PATH=/home/your_user/dagster-slurm
SLURM_PARTITION=GPU-l40s                           # default partition with 1x L40S GPU per node
# SLURM_RESERVATION=...                            # optional reservation if provided by admins
SLURM_SUPERCOMPUTER_SITE=datalab

# Dagster deployment selector
DAGSTER_DEPLOYMENT=production_supercomputer # or staging_supercomputer
```

The Datalab preset mirrors the `ssh datalab` experience: the default host, port, and user match the cluster settings, and the sample partition queues a single GPU (`srun -p GPU-l40s --gres=gpu:1 -N1 -n1 --time=00:10:00 --pty bash`). Swap `SLURM_PARTITION` if you target `GPU-a100`, `GPU-a40`, or CPU-only queues, and adjust `SLURM_DEPLOYMENT_BASE_PATH` to a writable location in your home directory. If your site enforces password auth, remove `SLURM_EDGE_NODE_KEY_PATH` and set `SLURM_EDGE_NODE_PASSWORD`; pseudo-TTY is requested automatically when needed.

With the variables defined, restart your Dagster code location. For a quick validation, run `sinfo` on the login node to confirm the partition name and `srun` with the parameters above to verify access before launching Dagster jobs.

For additional information about Datalab, see the [TU Wien Datalab documentation](https://www.it.tuwien.ac.at/services/netzwerk-und-server/datalab).
