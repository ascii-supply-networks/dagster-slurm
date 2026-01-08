---
sidebar_position: 99
title: HPC - MUSICA
---

Login on MUSICA is either via a link / qr-code on each login (not feasible for dagster-slurm) or with the help of the step client.
To obtain a time-limited ssh certificate that can be used with dagster-slurm, use the step client. Execute the following command and log in in your web browser:

```bash
step ssh certificate 'your.email.address@example.com' ~/.ssh/id_musica --no-password --insecure --force
```

## Sample configuration: MUSICA

```dotenv title=".env.musica"
# SSH / cluster login node access
SLURM_EDGE_NODE_HOST=musica.vie.asc.ac.at
SLURM_EDGE_NODE_PORT=22
SLURM_EDGE_NODE_USER=xy12345                  # replace with your MUSICA username
SLURM_EDGE_NODE_KEY_PATH=/Users/you/.ssh/id_musica  # Log in with the step client first to get a time-limited certificate

# Deployment settings
SLURM_DEPLOYMENT_BASE_PATH=/home/your_user/dagster-slurm
SLURM_PARTITION=zen4_0768_h100x4
SLURM_QOS=zen4_0768_h100x4
# SLURM_RESERVATION=...                       # optional reservation if provided by admins
SLURM_SUPERCOMPUTER_SITE=musica

# Dagster deployment selector
DAGSTER_DEPLOYMENT=production_supercomputer # or staging_supercomputer
```

With the variables defined, restart your Dagster code location. For a quick validation, run `sinfo` on the login node to confirm the partition name and `srun` with the parameters above to verify access before launching Dagster jobs.

For additional information about MUSICA, see the [MUSICA](https://docs.asc.ac.at/musica_test/).
