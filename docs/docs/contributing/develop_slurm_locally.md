---
sidebar_position: 4
title: Develop Slurm Locally
---

# Develop Slurm Locally

Switching to Slurm-backed execution locally only requires environment variables that describe the edge node. Create an `.env` file with:

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
