# dagster-slurm

[![image](https://img.shields.io/pypi/v/dagster-slurm.svg)](https://pypi.python.org/pypi/dagster-slurm)
[![image](https://img.shields.io/pypi/l/dagster-slurm.svg)](https://pypi.python.org/pypi/dagster-slurm)
[![image](https://img.shields.io/pypi/pyversions/dagster-slurm.svg)](https://pypi.python.org/pypi/dagster-slurm)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

---

Integrating dagster to orchestrate slurm jobs for distributed systems like ray for a better developer experience on supercomputers.

As part of the hackathon (https://www.openhackathons.org/s/siteevent/a0CUP000013Tp8f2AC/se000375) we intend to work on this integration.
We are looking for more hands to join in - or review the task list so that we can make sure we are not missing anything.

- Tasks for implementation: https://github.com/orgs/ascii-supply-networks/projects/4
- Project lives here https://github.com/ascii-supply-networks/dagster-slurm

See the (draft) [documentation](https://ascii-supply-networks.github.io/dagster-slurm/)

> We are actively looking for contributions to bring this package to life together


## developing

```bash
docker compose up --build -d
ssh submitter@localhost -p 2222
# password: submitter
sinfo
```

## basic distribution

initial setup

```bash
curl -fsSL https://pixi.sh/install.sh | sh
bash
pixi global install pixi-unpack
```

environment setup

```bash
cd examples
pixi run pack
# if using an ARM host
# pixi run pack-aarch

scp -P 2222 environment.tar submitter@localhost:/home/submitter
ssh submitter@localhost -p 2222

# tar -xvf environment.tar
pixi exec pixi-unpack environment.tar
source ./activate.sh
```

as a result:

```bash
source /home/submitter/activate.sh
```

is now available on all the cluster nodes

## ray

```bash

```