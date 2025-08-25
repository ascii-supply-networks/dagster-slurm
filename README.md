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


## TODO:

1) see the tasks in the github project
2) refine documentation
3) clean up this readme

## developing

```bash
docker compose build
docker compose up -d

ssh submitter@localhost -p 2223
# password: submitter
sinfo
```

## basic distribution

initial setup.
Install `pixi-unpack` to unpack the environment on the HPC/supercomputer to make it available for execution.

```bash
curl -fsSL https://pixi.sh/install.sh | sh
pixi global install pixi-unpack
```

environment setup

```bash
cd examples
pixi run pack
# if using an ARM host
# pixi run pack-aarch

scp -P 2223 environment.tar submitter@localhost:/home/submitter
ssh submitter@localhost -p 2223

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

docs: https://docs.ray.io/en/latest/cluster/vms/user-guides/community/slurm.html#id7

- get `slurm-template.sh` but modified https://docs.ray.io/en/latest/cluster/vms/user-guides/community/slurm-template.html#slurm-template find our [version here](projects/dagster-slurm-ray/dagster_slurm_ray/scripts/slurm-template.sh)

- get `slurm-launch.py` inspired by (but modified) https://docs.ray.io/en/latest/cluster/vms/user-guides/community/slurm-launch.html#slurm-launch find our [version here](projects/dagster-slurm-ray/dagster_slurm_ray/slurm-launch.py)

- make a mini python file `my_mini_job.py` with contents of:

```python
import ray
ray.init()

@ray.remote
def f(x):
    return x * x

futures = [f.remote(i) for i in range(4)]
print(ray.get(futures)) # [0, 1, 4, 9]
```

Connect to the slurm master:

- by ssh connecting to your production supercomputer
- by `docker exec -ti <<container id >> bash` for the local slurm minicluster

And there follow along with

```bash
# test run locally (single node)
python my_mini_job.py

# submit to slurm (dry run)
python slurm-launch.py --exp-name test --command "python my_mini_job.py" --num-nodes 2 --activation-script /home/submitter/activate.sh --dry-run
# --- DRY RUN MODE ---
# Job script 'sbatch test_0823-0707.sh' has been generated but NOT submitted.
# You can inspect the script and submit it manually with:
#  sbatch sbatch test_0823-0707.sh

cat test_0823-0707.sh


# now really submit
python slurm-launch.py --exp-name test --command "python my_mini_job.py" --num-nodes 2 --activation-script /home/submitter/activate.sh
```

### slurm monitoring

```bash
# ongoing
squeue -u submitter

# status after completion by job id
sacct -j 1.1
# by user
sacct -u submitter

# all jobs
sacct

# logs (only whilst running, not after completion)
scontrol show job 3
cat $(scontrol show job 1 | grep -oP 'StdOut=\K\S+')
```

debugging

```bash
yum install procps

ps aux | grep ray

# cancel a stuck job
scancel 6
```