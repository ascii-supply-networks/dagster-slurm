Based on https://github.com/giovtorres/slurm-docker-cluster
Refinements:

- python environment handling
- SSH demo access
- Upgrade to current version of slurm and OS
- Streamlined/simplified setup

# Slurm Docker Cluster

To get up and running with Slurm in Docker, make sure you have the following tools installed:

- **[Docker](https://docs.docker.com/get-docker/)**
- **[Docker Compose](https://docs.docker.com/compose/install/)**

```bash
docker compose build
```

> find more details at https://github.com/giovtorres/slurm-docker-cluster

🚀 Starting the Cluster

Once the image is built, deploy the cluster with the default version of slurm
using Docker Compose:

```bash
docker compose up

ssh submitter@localhost -p 2223
```

The default is Slurm 25.11. To run Slurm 26.05 instead, set the image tag and
the Slurm release to build:

```bash
IMAGE_TAG=26-05-5-1 SLURM_TAG=slurm-26-05-5-1 docker compose up -d
```

Each image has its own SSH host keys. After switching versions or pulling a
rebuilt image, run `ssh-keygen -R "[localhost]:2223"` before connecting again.

## 🖥️ Accessing the Cluster

To interact with the Slurm controller, open a shell inside the `slurmctld` container:

```bash
docker exec -it slurmctld bash
```

Now you can run any Slurm command from inside the container:

```bash
[root@slurmctld /]# sinfo
PARTITION AVAIL  TIMELIMIT  NODES  STATE NODELIST
normal*      up 5-00:00:00      2   idle c[1-2]
```

## 🧑‍💻 Submitting Jobs

The cluster mounts the `slurm_jobdir` volume across all nodes, making job files accessible from the `/data` directory. To submit a job:

```bash
[root@slurmctld /]# cd /data/
[root@slurmctld data]# sbatch --wrap="hostname"
Submitted batch job 2
```

Check the output of the job:

```bash
[root@slurmctld data]# cat slurm-2.out
c1
```
