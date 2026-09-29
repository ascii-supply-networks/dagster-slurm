# Add worker allocations to a run's Ray cluster

A run-scoped session holds one allocation. It hosts the persistent Ray head and the payload drivers. Worker allocations add nodes from other Slurm jobs, on any partition, to that Ray cluster while the run continues, and release them again. A run can start on whatever GPUs are free now, add more as other GPUs free up, and shed nodes before their walltime, without restarting.

## Keep the head in its own allocation

By default, the run's first allocation hosts the Ray head, so ending it ends the Ray cluster. Set `ray_head_allocation` to run the head and the payload drivers in a separate, small allocation instead:

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

The head allocation defaults to one node and no GPUs. Its Ray head advertises no CPUs or GPUs, so no Ray task can take it down. `run_allocation` becomes the run's first worker allocation: its nodes join the head as workers, and later worker allocations inherit its unset fields. The session submits it again whenever it has no live worker, for example when a retried run starts. On an explicitly configured `SlurmSessionResource`, set `ray_head_only=True`, `num_nodes=1` and `worker_allocation=SlurmWorkerAllocationConfig(...)` for the same layout.

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

Unset fields inherit the session's first worker allocation or, without one, the head allocation. `nodelist` and `extra_sbatch_directives` apply to one submission only. Setting `mem` or `mem_per_cpu` replaces the inherited memory setting of either form. Ray on these nodes reports `gpus_per_node` GPUs. `ray_resources` become Ray custom resources on every node of the allocation, so actors can target them with `resources={"accelerator:a40": 1}`. On NVIDIA nodes, Ray also adds its own `accelerator_type:<model>` resource. `ray_start_args` passes further `ray start` options to these nodes, for example `--num-cpus=8` or `--object-store-memory=...`. They take precedence over the launcher's values.

Payload code sees new nodes as extra Ray capacity. Ray Data actor pools with a `(min, max)` concurrency grow onto them. To launch work once enough capacity has joined, see `wait_for_stable_ray_resources` in `dagster_slurm.ray`. Choosing which allocations to add, and when, stays in your application. For example, you can probe start times with `sbatch --test-only`.

## Remove a worker allocation

```python
state = session.remove_worker_allocation(a40, drain_timeout=900)
```

Removal uses `request_drain()` on the worker allocation and drains its nodes through Ray. Ray stops placing tasks and actors on them, while running tasks and actors continue. Each node leaves the cluster once it is idle, and the job ends when all of its nodes have left. The call returns the final Slurm state, normally `COMPLETED`. Work still running at the `drain_timeout` deadline is stopped. Ray then retries the lost tasks and restarts lost actors according to their `max_retries` and `max_restarts` settings. A job that has not started yet is cancelled. With `wait=False`, the drain continues in the background.

Long-lived actors keep a node busy until the deadline. Payloads that commit per unit of work lose at most the unit in progress. To leave earlier, release actors placed on draining nodes.

With `signal_before_timeout`, a worker allocation drains itself before its walltime. The pre-walltime signal starts the same drain, which ends 15 seconds before the walltime so that Ray can stop cleanly. Without that signal, Slurm ends the job at its walltime and Ray treats the nodes as failed.

Removing a worker never signals the payload drivers, so it does not raise `SlurmStepDrained`. A partial drain is a Ray-level event, and the payload keeps running on the remaining nodes. Draining the head allocation itself still works as described in [Continue a run in a successor allocation](walltime-relay.md).

## Fail over the head

Worker allocations follow the session's current head. When a successor is promoted, or the head is replaced, their nodes leave the old head and join the new one. The Slurm jobs keep their nodes, so a failover does not give up GPUs that took hours to get. Ray state on the old head, such as actors and objects, is lost, so payloads that ran there must be launched again.

When the head allocation ends unexpectedly, the payload step fails with `SlurmAllocationEnded`, also when its `srun` reported an exit code first. Replace the head and launch the remaining work again:

```python
from dagster_slurm import SlurmAllocationEnded

while True:
    try:
        return compute.run(
            context=context, payload_path="process_documents.py"
        ).get_results()
    except SlurmAllocationEnded:
        # Submit a new head, wait for it to run and promote it. Worker
        # allocations re-join it; committed work is skipped on relaunch.
        session.replace_head(timeout=3600)
```

`replace_head()` uses a published successor, or submits one with the head's configuration (or `config`), waits for it to run, starts Ray there with the recorded launcher and environment, and promotes it. It also moves a healthy head once its payloads have drained. A worker allocation without a live head waits up to `rejoin_timeout` seconds, 1800 by default, before it releases itself. A run that is retried after its supervisor died also adopts the live workers: its new head allocation publishes itself, and the workers join it.

## Recovery and cleanup

The session's `allocation.json` lists each worker allocation, and the session directory holds a `ray_head` file with the current head's job ID, which worker nodes follow. A restarted supervisor finds live workers with `session.worker_allocations` and can drain or cancel them. While a worker allocation waits for a new head, the orphan sensor treats the run like one with a published successor. Worker allocations share the session's Slurm job name, so session teardown cancels them with the rest of the run.

## Requirements

- Worker nodes must reach the head's Ray ports, and the head must reach the workers' port blocks. This is usually true between the partitions of one cluster.
- Compute nodes need the Slurm client commands `srun` and `squeue`.
- The activated environment needs `python3` and the `ray drain-node` command. Draining and failover are tested with Ray 2.55 and 2.58.
