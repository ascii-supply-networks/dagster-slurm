---
sidebar_position: 1
---

# Tutorial

## prerequisites

- installation of pixi: https://pixi.sh/latest/installation/ `curl -fsSL https://pixi.sh/install.sh | sh`
- `pixi global install git`
- a container runtime like docker or podman; for now we assume `docker compose` is available to you. You could absolutely also use `nerdctl` or something similar.

## usage

Example

```bash
git clone https://github.com/ascii-supply-networks/dagster-slurm.git
docker compose up -d
cd dagster-slurm/examples
```

### local execution

Execute without slurm.
- small data
- rapid local prototyping

```bash
pixi run start
```

go to http://localhost:3000 and you should see the dagster webserver running.

### docker local execution

- test everything works on SLURM
- still small data
- mainly used for developing this integration

Ensure you have a `.env` file with the following content:

```
SLURM_EDGE_NODE_HOST=localhost
SLURM_EDGE_NODE_PORT=2223
SLURM_EDGE_NODE_USER=submitter
SLURM_EDGE_NODE_PASSWORD=submitter
SLURM_DEPLOYMENT_BASE_PATH=/home/submitter/pipelines/deployments
```

```bash
pixi run start-staging
```

go to http://localhost:3000 and you should see the dagster webserver running.

### prod docker local execution

- test everything works on SLURM
- still small data
- mainly used for developing this integration
- this target instead supports a faster startup of the job

Ensure you have a `.env` file with the following content:

```
SLURM_EDGE_NODE_HOST=localhost
SLURM_EDGE_NODE_PORT=2223
SLURM_EDGE_NODE_USER=submitter
SLURM_EDGE_NODE_PASSWORD=submitter
SLURM_DEPLOYMENT_BASE_PATH=/home/submitter/pipelines/deployments

# see the jq command below for dynamically setting this
# DAGSTER_PROD_ENV_PATH=/home/submitter/pipelines/deployments/<<<your deployment >>>
```

```bash
# we assume your CI-CD pipelines would out of band perform the deployment of the environment
# this allows your jobs to start up faster
pixi run deploy-prod-docker

cat deplyyment_metadata.json
export DAGSTER_PROD_ENV_PATH="$(jq -er '.deployment_path' foo.json)"

pixi run start-prod-docker
```

go to http://localhost:3000 and you should see the dagster webserver running.

### real HPC supercomputer execution

- large data
- you have to adapt the configuration to target your specific HPC deployment


## API examples

### controlplane

This is part of your [dagster](https://dagster.io/) deployment.
Possibly you might be using an instance of the [local-data-stack](https://github.com/l-mds/local-data-stack/).

```python
import dagster as dg
from dagster_slurm import ComputeResource


@dg.asset
def process_data(
    context: dg.AssetExecutionContext,
    compute: ComputeResource,
):
    """Some mini example of dagster-slurm"""
    script_path = dg.file_relative_path(
        __file__,
        "../../../../dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/shell/myfile.py",
    )
    completed_run = compute.run(
        context=context,
        payload_path=script_path,
        extra_env={
            "KEY_ENV": "value",
        },
        extras={
            "foo": "bar",
        },
    )
    yield from completed_run.get_results()
```

### userplane

```python
import os
from dagster_pipes import PipesContext, open_dagster_pipes


def main():
    context = PipesContext.get()
    context.log.info("Starting data processing...")
    context.log.debug(context.extras)
    key_env = os.environ.get("KEY_ENV")
    context.log.info(f"KEY_ENV: {key_env}")
    context.log.info(f"foo: {context.extras['foo']}")
    
    # Your processing logic here
    
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

### resource configuration

Take a look here to understand how to set up the resource configuration to interact with the local, docker-slurm or real HPC slurm runners https://github.com/ascii-supply-networks/dagster-slurm/blob/main/examples/projects/dagster-slurm-example/dagster_slurm_example/resources/__init__.py