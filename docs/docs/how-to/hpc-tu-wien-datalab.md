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
SLURM_PARTITION=GPU-l40s
# SLURM_RESERVATION=...                            # optional reservation if provided by admins
SLURM_SUPERCOMPUTER_SITE=datalab

# Dagster deployment selector
DAGSTER_DEPLOYMENT=production_supercomputer # or staging_supercomputer
```

With the variables defined, restart your Dagster code location. For a quick validation, run `sinfo` on the login node to confirm the partition name and `srun` with the parameters above to verify access before launching Dagster jobs.

For additional information about Datalab, see the [TU Wien Datalab documentation](https://www.it.tuwien.ac.at/services/netzwerk-und-server/datalab).
