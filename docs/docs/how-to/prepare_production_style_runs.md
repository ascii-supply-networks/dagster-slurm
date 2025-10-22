---
sidebar_position: 3
title: Prepare production style runs
---


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