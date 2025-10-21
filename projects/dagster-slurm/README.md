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
docker compose up -d --build
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

- Targets clusters like VSC-5 (Vienna Scientific Cluster) and Leonardo (CINECA).
- Assets run against the real scheduler, so ensure the account has queue access and quotas.

Create a `.env` file with the edge-node credentials and select the site profile:

```dotenv
# example for VSC-5
SLURM_EDGE_NODE_HOST=vsc5.vsc.ac.at
SLURM_EDGE_NODE_PORT=22
SLURM_EDGE_NODE_USER=<<your_user>>
SLURM_EDGE_NODE_PASSWORD=<<your_password>>
SLURM_EDGE_NODE_JUMP_HOST=vmos.vsc.ac.at
SLURM_EDGE_NODE_JUMP_USER=<<your_user>>
SLURM_EDGE_NODE_JUMP_PASSWORD=<<your_password>>
SLURM_DEPLOYMENT_BASE_PATH=/home/<<your_user>>/pipelines/deployments
SLURM_PARTITION=zen3_0512
SLURM_QOS=zen3_0512_devel
SLURM_RESERVATION=dagster-slurm_21
SLURM_SUPERCOMPUTER_SITE=vsc5
DAGSTER_DEPLOYMENT=staging_supercomputer
```

If your account relies on passwords (or passwords + OTP), provide them for both the jump host and the final login node. The automation will answer the standard prompts; any time-based OTP still has to be supplied interactively once per validity window. When an extra prompt appears, Dagster writes `Enter ... for <host>:` to your terminal (via `/dev/tty`). Enter the code there to continue.

TTY allocation is handled automatically for password-based sessions, so you do not need to set `SLURM_EDGE_NODE_FORCE_TTY` unless your centre requires it explicitly.

With the variables in place, validate connectivity and job submission using the staging supercomputer profile:

```bash
pixi run start-staging-supercomputer
```

> Staging mode packages dependencies on demand. Expect the first asset run to upload a new environment bundle before dispatching the Slurm job.

For production you should pre-build and upload the execution environment via your CI/CD pipeline (see `examples/scripts/deploy_environment.py`). Capture the output path and expose it to Dagster as `CI_DEPLOYED_ENVIRONMENT_PATH`:

```bash
python scripts/deploy_environment.py --platform linux-64  # run from CI
# -> produces deployment_metadata.json with "deployment_path"

export CI_DEPLOYED_ENVIRONMENT_PATH=/home/submitter/pipelines/deployments/prod-env-20251018
export DAGSTER_DEPLOYMENT=production_supercomputer
pixi run start-production-supercomputer
```

If `CI_DEPLOYED_ENVIRONMENT_PATH` is missing, the production profile will refuse to start to prevent accidental live builds on the cluster.

To confirm a submission landed on the expected queue, run:

```bash
ssh -J <<your_user>>@vmos.vsc.ac.at <<your_user>>@vsc5.vsc.ac.at \
  "squeue -j <jobid> -o '%i %P %q %R %T'"
```

The `Partition`, `QOS`, and `Reservation` columns should match your `.env`.


## contributing

See Details here: [docs](docs) for how to contribute!
Help building and maintaining this project is welcome.
