# dagster-slurm

Integrating dagster to orchestrate slurm jobs for HPC systems and frameworks for scaling compute like ray for a better developer experience on supercomputers.

## Basic example

https://github.com/ascii-supply-networks/dagster-slurm/tree/main/examples


### prerequisites

- installation of pixi: https://pixi.sh/latest/installation/ `curl -fsSL https://pixi.sh/install.sh | sh`
- `pixi global install git`
- a container runtime like docker or podman; for now we assume `docker compose` is available to you. You could absolutely also use `nerdctl` or something similar.

### usage

Example

```bash
git clone https://github.com/ascii-supply-networks/dagster-slurm.git
docker compose up -d
cd dagster-slurm/examples
```

#### local execution

Execute without slurm.
- Small data
- Rapid local prototyping

```bash
pixi run start
```

go to http://localhost:3000 and you should see the dagster webserver running.

#### docker local execution

- Test everything works on SLURM
- Still small data
- Mainly used for developing this integration

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

#### prod docker local execution

- Test everything works on SLURM
- Still small data
- Mainly used for developing this integration
- This target instead supports a faster startup of the job

Ensure you have a `.env` file with the following content:

```
SLURM_EDGE_NODE_HOST=localhost
SLURM_EDGE_NODE_PORT=2223
SLURM_EDGE_NODE_USER=submitter
SLURM_EDGE_NODE_PASSWORD=submitter
SLURM_DEPLOYMENT_BASE_PATH=/home/submitter/pipelines/deployments

# see the JQ command below for dynamically setting this
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

#### real HPC supercomputer execution

- large data
- you have to adapt the configuration to target your specific HPC deployment


## contributing

See Details here: [docs](docs) for how to contribute!
Help building and maintaining this project is welcome.
