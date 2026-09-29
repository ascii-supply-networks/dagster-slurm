# Add worker allocations to a run's Ray cluster

A run-scoped session holds one allocation. It hosts the persistent Ray head and the payload drivers. Worker allocations add nodes from other Slurm jobs, on any partition, to that Ray cluster while the run continues, and release them again. A run can start on whatever GPUs are free now, add more as other GPUs free up, and shed nodes before their walltime, without restarting.

## Run a CPU head with GPU workers

By default, the run's first allocation hosts the Ray head. Set `ray_head_allocation` to run the head and the payload drivers in a separate, small CPU allocation, and all GPU work in worker allocations:

```python
from dagster_slurm import ComputeResource, RayLauncher, SlurmRunAllocationConfig

compute = ComputeResource(
    mode="slurm",
    slurm=slurm,
    allocation_scope="run",
    default_launcher=RayLauncher(num_gpus_per_node=3),
    ray_head_allocation=SlurmRunAllocationConfig(
        partition="cpu",
        cpus_per_task=4,
        mem="16G",
        time_limit="3-00:00:00",
    ),
    run_allocation=SlurmRunAllocationConfig(
        partition="GPU-rtx6000",
        gpus_per_node=3,
        time_limit="08:00:00",
        signal_before_timeout="TERM@600",
    ),
)
```

`ray_head_allocation` needs an explicit `partition` and `time_limit`: usually a CPU partition and a walltime that covers the whole run, such as the partition's maximum. The queue's defaults describe compute, so the head does not use them: it defaults to one node, 2 CPUs, 8 GB and no GPUs. Accounts, QoS and reservations still come from the queue. `run_allocation` becomes the run's first worker allocation: its nodes join the head as workers, and later worker allocations inherit its unset fields. The session submits it again whenever it has no live worker, for example when a retried run starts. On an explicitly configured `SlurmSessionResource`, set `ray_head_only=True`, `num_nodes=1` and `worker_allocation=SlurmWorkerAllocationConfig(...)` for the same layout.

The head's Ray node advertises no CPUs or GPUs, so Ray places no work that requests resources there. Tasks and actors that request no resources can still land on it, such as Ray Data's internal actors or `num_cpus=0` helpers. Size the head's memory for the GCS, the dashboard, the payload drivers and such helpers. Every worker node offers a `dagster_slurm_worker` resource, so helpers can stay off the head with `resources={"dagster_slurm_worker": 0.01}`.

## Add a worker allocation

Inside an asset, obtain the session with `compute.get_run_allocation_session(context)`. The same methods are available on an explicitly configured `SlurmSessionResource`:

```python
from dagster_slurm import SlurmWorkerAllocationConfig

session = compute.get_run_allocation_session(context)

# Returns after submission; it does not wait for the job to start.
a40 = session.add_worker_allocation(
    SlurmWorkerAllocationConfig(
        partition="GPU-a40",
        num_nodes=1,
        gpus_per_node=4,
        cpus_per_task=32,
        mem="200G",
        time_limit="08:00:00",
        signal_before_timeout="TERM@600",
        ray_resources={"accelerator:a40": 1, "vram_48gb": 1},
    )
)

# Optional: block until every node has joined, for example before sizing work.
node_ids = session.wait_for_worker_allocation(a40, timeout=3600)
```

When the job starts and the head's Ray cluster is ready, each node starts a Ray worker with the run's launcher settings, environment, port strategy and `redis_password`. Commands in `pre_start_commands`, such as exporting a Ray auth token, run on worker nodes too. If the job starts before the head's Ray cluster, it waits.

Unset Slurm fields inherit the session's first worker allocation or, without one, the head allocation. `nodelist` and `extra_sbatch_directives` apply to one submission only, and so do the Ray settings `ray_resources`, `ray_start_args` and `rejoin_timeout`: a label such as `accelerator:a40` must not follow a request to another partition. Setting `mem` or `mem_per_cpu` replaces the inherited memory setting of either form. Ray on these nodes reports `gpus_per_node` GPUs, and the CPUs Slurm granted on each node (`SLURM_CPUS_ON_NODE`) rather than every CPU of a shared node. `ray_resources` become Ray custom resources on every node of the allocation, so actors can target them with `resources={"accelerator:a40": 1}`. On NVIDIA nodes, Ray also adds its own `accelerator_type:<model>` resource. `ray_start_args` passes further `ray start` options to these nodes, for example `--num-cpus=8` or `--object-store-memory=...`. They take precedence over the launcher's values and the defaults above.

If the Ray worker on one node ends while its head is live, for example after a raylet crash, that node starts again after a delay while the other nodes keep working. The delay grows each time, and a node that ends three more times stays out until the allocation joins another head.

Payload code sees new nodes as extra Ray capacity. Ray Data actor pools with a `(min, max)` concurrency grow onto them. To launch work once enough capacity has joined, see `wait_for_stable_ray_resources` in `dagster_slurm.ray`. Choosing which allocations to add, and when, stays in your application. For example, you can probe start times with `sbatch --test-only`.

## Remove a worker allocation

```python
state = session.remove_worker_allocation(a40, drain_timeout=900)
```

Removal uses `request_drain()` on the worker allocation and drains its nodes through Ray. Ray stops placing tasks and actors on them, while running tasks and actors continue. Each node leaves the cluster once it is idle, and the job ends when all of its nodes have left. The call returns the final Slurm state, normally `COMPLETED`. Work still running at the `drain_timeout` deadline is stopped. Ray then retries the lost tasks and restarts lost actors according to their `max_retries` and `max_restarts` settings. A job that has not started yet is cancelled. With `wait=False`, the drain continues in the background.

Long-lived actors keep a node busy until the deadline. Payloads that commit per unit of work lose at most the unit in progress. To leave earlier, release actors placed on draining nodes.

With `signal_before_timeout`, a worker allocation drains itself before its walltime. The pre-walltime signal starts the same drain, which ends 15 seconds before the walltime so that Ray can stop cleanly. Without that signal, Slurm ends the job at its walltime and Ray treats the nodes as failed.

Removing a worker never signals the payload drivers, so it does not raise `SlurmStepDrained`. A partial drain is a Ray-level event, and the payload keeps running on the remaining nodes. Draining the head allocation itself still works as described in [Continue a run in a successor allocation](walltime-relay.md).

## Elect a new head

When the allocation that hosts the head goes away, the session elects a new head, and the Ray cluster continues on the remaining allocations. This covers the run's first allocation reaching its walltime as well as a node failure. Every step process of the run checks the head every 30 seconds once the session has a worker allocation or a separate head; a process that started before the first worker allocation was added begins within 2 minutes. It elects:

1. a running successor, if one is published;
2. otherwise the running, undrained worker allocation with the most time left. It hosts the head on its first node without waiting in the queue: a head-only control plane next to that node's Ray worker, so the node keeps offering its CPUs and GPUs. If the head fails to start there, the next election tries the other worker allocations first, and a worker allocation that failed twice is passed over;
3. otherwise a new allocation shaped like the session's first head, once it starts.

Worker allocations follow the new head: their nodes leave the old head and join the new one, and the Slurm jobs keep their nodes throughout. A worker allocation without a live head waits up to `rejoin_timeout` seconds, 600 by default, before it releases itself. Raise it when a replacement head can wait longer in the queue; lower it on billed clusters where idle GPUs are costly. Keep it above about 5 minutes, so a worker allocation stays long enough to be elected: when the process running an election dies, its claim blocks the next election for 2 minutes.

A head that phases out hands over before it ends. When it drains, after its pre-walltime signal or a `request_drain()` call, its payload drivers get the signal and stop at a checkpoint. The session then elects the next head, and the draining allocation stays until the head has moved. A head without a pre-walltime signal is drained 5 minutes before its walltime. A head whose job ended, or whose Ray process stopped while the job runs, is replaced at once.

When nodes leave, the cluster keeps running on the others: Ray retries lost tasks and restarts lost actors elsewhere. With `worker_allocation` or `ray_head_allocation` set, the session submits the worker allocation again whenever no worker is left.

A new head keeps the Slurm allocations, not the Ray state. It starts a new GCS, worker nodes register again with new node IDs, and all actors, objects and in-flight tasks are gone. Payloads restart from their own checkpoints, so commit finished work outside Ray. Ray's GCS fault tolerance cannot help here: it needs a stable head address and an external Redis, and a new head on Slurm has a new address.

Payload steps whose driver ran on the lost head fail with `SlurmAllocationEnded`, also when their `srun` reported an exit code first. A step that was about to start as a new head took over fails the same way, with `allocation_state` set to `REPLACED`. Get the elected head and launch the remaining work again:

```python
from dagster_slurm import SlurmAllocationEnded

while True:
    try:
        return compute.run(
            context=context, payload_path="process_documents.py"
        ).get_results()
    except SlurmAllocationEnded:
        # Waits for the election; committed work is skipped on relaunch.
        session.replace_head(timeout=3600)
```

`replace_head()` returns the healthy head, and elects one first if the session has none. Every step can call it at the same time, from any process: the first caller elects, the others wait, and all get the same head. It returns a healthy head unchanged; to move one, call `request_drain()` on it first. Sessions without worker allocations do not elect heads on their own, so call `replace_head()` there to fail over. A run that is retried after its supervisor died also elects a running worker allocation when it starts.

## Recovery and cleanup

The session's `allocation.json` lists each worker allocation and an election in progress, and the session directory holds a `ray_head` file with the current head's job ID, which worker nodes follow. A restarted supervisor finds live workers with `session.worker_allocations` and can drain or cancel them. While a worker allocation waits for a new head, the orphan sensor treats the run like one with a published successor. Worker allocations share the session's Slurm job name, so session teardown cancels them with the rest of the run.

## Requirements

- Worker nodes must reach the head's Ray ports, and the head must reach the workers' port blocks. This is usually true between the partitions of one cluster.
- Compute nodes need the Slurm client commands `srun` and `squeue`.
- The activated environment needs `python3` and the `ray drain-node` command. Draining and head election are tested with Ray 2.55 and 2.58.
- A worker allocation can host the head only with `port_strategy="random"` (the default) or `"hash_jobid"`, which give the head and the node's worker separate ports. With `"fixed"`, the session skips worker allocations and elects a new head allocation instead. Keep that allocation off the nodes of worker allocations, for example in its own partition.
