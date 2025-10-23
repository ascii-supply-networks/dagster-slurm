---
sidebar_position: 99
title: HPC - Leonardo
---

## Sample configuration: CINECA Leonardo

```dotenv title=".env.leonardo"
# SSH / cluster login node access
SLURM_EDGE_NODE_HOST=login01-ext.leonardo.cineca.it  # valid login nodes are login01-ext, login02-ext, login05-ext, login07-ext
SLURM_EDGE_NODE_PORT=22
SLURM_EDGE_NODE_USER=your_leonardo_username
# SLURM_EDGE_NODE_PASSWORD=password            # specify either password or ssh key path
SLURM_EDGE_NODE_KEY_PATH=/Users/you/.ssh/id_ed25519_vsc5

# Deployment settings
SLURM_DEPLOYMENT_BASE_PATH=/leonardo/home/userexternal/your_leonardo_username/dagster-slurm
SLURM_PARTITION=boost_usr_prod
# SLURM_QOS=boost_qos_bprod                   # Queue for large jobs (65 to 256 nodes). Leave out for normal queue (for up to 64 nodes).
# SLURM_QOS=boost_qos_dbg                     # Development queue with higher priority (max. 2 nodes, max. 30 minutes)
# SLURM_RESERVATION=reservation_name          # optional reservation (if active)
SLURM_SUPERCOMPUTER_SITE=leonardo

# Dagster deployment selector
DAGSTER_DEPLOYMENT=production_supercomputer
```

Leonardo requires you to have an active project allocation; the permitted partitions depend on your account type. Verify your entitlements on the cluster:

```bash
sacctmgr show assoc where user=$USER format=Cluster,Account%20,Partition,QOS%60
sinfo -s
```

If the output does not list `batch`, pick one of the partitions above that appears in both commands. GPU queues also need the matching QoS (e.g. `dcgpuqos`). Training accounts typically use `/leonardo/home/usertrain/<user>/…` for storage—adjust `SLURM_DEPLOYMENT_BASE_PATH` accordingly. If your site enforces OTP/Kerberos, configure a local `~/.ssh/config` entry or a bastion jump host—`dagster-slurm` will reuse that setup automatically.

With the variables defined, restart your Dagster code location. To dry-run against the real scheduler while still allowing on-the-fly environment packaging, point `DAGSTER_DEPLOYMENT=staging_supercomputer` and run:

```bash
pixi run start-staging-supercomputer
```

For production workloads you should publish the environment bundle ahead of time (e.g. via CI using `python scripts/deploy_environment.py`). Once you export the uploaded path as `CI_DEPLOYED_ENVIRONMENT_PATH`, switch to `DAGSTER_DEPLOYMENT=production_supercomputer` and start Dagster:

```bash
pixi run start-production-supercomputer
```

The production preset refuses to start if `CI_DEPLOYED_ENVIRONMENT_PATH` is missing, ensuring clusters never build environments during business-critical runs.

For additional information about Leonardo, see the [Leonardo documentation](https://docs.hpc.cineca.it/hpc/leonardo.html).
