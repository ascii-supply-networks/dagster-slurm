---
sidebar_position: 99
title: HPC - Leonardo
---

## Sample configuration: Leonardo (CINECA)

```dotenv title=".env.leonardo"
SLURM_EDGE_NODE_HOST=login01-ext.leonardo.cineca.it
SLURM_EDGE_NODE_PORT=22                     # Leonardo typically listens on 22; override if your project uses a custom port
SLURM_DEPLOYMENT_BASE_PATH=/leonardo/home/usertrain/a08trb02/dagster-slurm
SLURM_PARTITION=boost_usr_prod              # or boost_usr_dbg / dcgp_usr_prod depending on your entitlement
SLURM_QOS=boost_qos_bprod
SLURM_SUPERCOMPUTER_SITE=leonardo
DAGSTER_DEPLOYMENT=production_supercomputer
```

Leonardo requires you to have an active project allocation; the permitted partitions depend on your account type. Verify your entitlements on the cluster:

```bash
sacctmgr show assoc where user=$USER format=Cluster,Account,Partition,QOS%20
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