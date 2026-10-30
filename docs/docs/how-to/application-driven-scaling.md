# Scale worker allocations from your application

[Worker allocations](elastic-worker-allocations.md) add and remove Slurm jobs around a run's Ray cluster. Deciding how many a run needs is workload policy: how much parallel work remains, how long a model takes to start, which hardware fits, and whether a short allocation is worth its startup cost. The session gives that policy three hooks and keeps the decisions out of the library:

| Hook                                         | Mechanism (library)                                                                  | Policy (your application)                            |
| -------------------------------------------- | ------------------------------------------------------------------------------------ | ---------------------------------------------------- |
| `session.inspect()`                          | One observation of every allocation: scheduler facts, Ray membership, drain state    | What the observation means for your work             |
| `session.estimate_worker_starts(candidates)` | Scheduler start estimates for candidate shapes, within a request budget              | Which candidates to try, and how to rank the answers |
| `session.reconcile_workers(intent)`          | Submit or release allocations until an intent's count holds, safely across processes | The count and shape of the intent                    |

Nothing runs in the background. Your code calls these hooks, for example once per batch of work or on a timer in the step that drives the workload. Slurm keeps scheduling and admission control: the hooks submit and release ordinary jobs.

## Observe the session

```python
from dagster_slurm import SessionSnapshot

session = compute.get_run_allocation_session(context)
snapshot: SessionSnapshot = session.inspect()

for worker in snapshot.workers():
    print(
        worker.slurm_job_id,
        worker.state,  # RUNNING, PENDING, ... or None if unknown
        worker.reason,  # e.g. "Resources" or "Priority" while pending
        worker.time_left_seconds,  # actual remaining walltime once running
        worker.granted_gpus,  # from the scheduler's grant, not the request
        worker.ready,  # every node joined the current Ray head
        worker.draining,
    )
```

`inspect()` takes two SSH round trips and one scheduler query, and does not wait for any allocation. Every allocation of a session shares its Slurm job name, so a single `squeue` call lists them all. The snapshot covers the head, a published successor and every worker allocation, with:

- `state`, `reason`: the scheduler's state and pending reason. A job that left the queue reports its final state from accounting, or `ENDED`.
- `time_left_seconds`: the scheduler's remaining walltime for a running job. It accounts for time limit changes, unlike the configured `time_limit_seconds`.
- `requested_nodes`, `requested_gpus_per_node` and `tres`: what was asked for, and the trackable resources the scheduler reports, which are the grant once the job runs.
- `nodes`, `ray_nodes`, `ready`: the allocation's Slurm nodes, the Ray node ID of each node that joined the current head, and whether all of them did. A `RUNNING` job is not yet a ready Ray worker.
- `draining`, `intent`, `drain_deadline`, `submitted_at`: drain status, and the bookkeeping of `reconcile_workers`. `submitted_at` is the login node's clock when `reconcile_workers` submitted the allocation, `None` for other allocations.

Values that could not be observed are `None`, never zero or `False`. When the scheduler does not answer, every `state` is `None` and `snapshot.certain` is `False`. `snapshot.observed_at` is the login node's clock at the query, so processes on different hosts compare the same time.

## Estimate start times

```python
from dagster_slurm import SlurmWorkerAllocationConfig

candidates = [
    SlurmWorkerAllocationConfig(partition="gpu-a", gpus_per_node=4, time_limit="02:00:00"),
    SlurmWorkerAllocationConfig(partition="gpu-a", gpus_per_node=2, time_limit="01:00:00"),
    SlurmWorkerAllocationConfig(partition="gpu-b", gpus_per_node=1, time_limit="00:30:00"),
]
for estimate in session.estimate_worker_starts(candidates):
    print(estimate.config.partition, estimate.start_in_seconds, estimate.error)
```

The default `SbatchTestOnlyEstimator` asks `sbatch --test-only` about every candidate in one SSH round trip. Each candidate is rendered with the options `add_worker_allocation` would submit, so inherited account, QoS, reservation and memory settings apply; `session.worker_sbatch_options(candidates)` returns those options. `start_in_seconds` compares the estimate with the login node's clock. A candidate the scheduler rejects has `error` set instead.

Each candidate costs one scheduler RPC. The estimator refuses more than `max_candidates` (8) per call and reuses answers for `cache_seconds` (60). Pass `estimator=SbatchTestOnlyEstimator(max_candidates=..., cache_seconds=...)` to change them, or any object with an `estimate(session, candidates)` method for site-specific behavior, such as a reservation calendar.

Estimates are advisory and reserve nothing. Other jobs can take the resources before your submission arrives, and a free-node snapshot alone cannot say whether a job would be scheduled there. Confirm an optimistic estimate with `pending_timeout` below.

## Reconcile an intent

```python
from dagster_slurm import WorkerIntent

result = session.reconcile_workers(
    WorkerIntent(
        key="gpu-helpers",
        count=3,
        config=SlurmWorkerAllocationConfig(partition="gpu-a", gpus_per_node=2, time_limit="01:00:00"),
        drain_timeout=600,
        pending_timeout=120,
    ),
    observed=snapshot,
)
print(result.submitted, result.released, result.live, result.held_back)
```

A `WorkerIntent` names a group of worker allocations and how many should exist. `reconcile_workers` submits allocations of `config` until `count` are pending or running, or releases the surplus. It guarantees:

- **No duplicates.** A call holds the session lock from its scheduler query to its last submission, so step processes that express the same intent at the same time submit it once. Pending allocations count toward the intent.
- **Recovery around submission.** Each submission is recorded with a token before it reaches the scheduler, and the job carries the token in its Slurm comment. When a call is interrupted before it records the job ID, for example when the SSH connection drops after `sbatch`, the next call finds the job by its token (`result.recovered`).
- **Revalidation.** The call queries the scheduler again before acting. It submits nothing when the scheduler does not answer, when `observed` is older than `max_observation_age` (300 seconds) or when the head changed since `observed` was taken. `result.held_back` says why. Releases and cleanup of allocations whose state is known continue.
- **Durable drain deadlines.** To scale in, the call releases pending allocations first, then the running ones with the least time left. Each released allocation gets a deadline (`drain_timeout`, or the worker's walltime drain window, plus a grace period), which is persisted before its drain starts. Later calls retry a failed drain without moving the deadline and cancel the job once it passes, also after a supervisor restart.
- **Expiry of queued allocations.** With `pending_timeout`, an allocation that is still pending after that many seconds is cancelled. The call does not replace it at once, so your policy can pick another shape for the next call.
- **Named releases.** `release=[job_id, ...]` releases those allocations of the intent in this call, whatever the scale-in order, for example a worker that failed your own health check. They get the same persisted deadline. `count` then applies to the other allocations: keep it to replace them, or lower it. Job IDs that are not live allocations of the intent, or that are already released, are ignored, so you can repeat a release until the snapshot shows its `drain_deadline`.
- **A submission budget.** `max_submissions` bounds the allocations one call submits. `count` is often relative to a snapshot, such as `len(active) + 1`. When an allocation ends after the snapshot, the call would replace it with `config`, or with the inherited shape if `config` is `None`. Pass `max_submissions=0` when your policy adds nothing, and the number it adds otherwise. `result.held_back` says when the budget stopped a submission.

Allocations added with `add_worker_allocation` belong to no intent and are never counted or released by `reconcile_workers`. When the session elects a worker allocation of an intent as its new head, that allocation keeps counting toward the intent until it drains, and scale-in never releases it. A worker that an intent released never becomes the head.

## Change an allocation's role

```python
session.assign_worker_intent(added.slurm_job_id, "gpu-helpers")  # counted and released from now on
session.assign_worker_intent(helper.slurm_job_id, None)  # kept: scale-in never releases it
```

`assign_worker_intent` moves a live worker allocation into an intent, or out of every intent. Use it when your application changes what an allocation is for: an allocation added with `add_worker_allocation` can join an intent, and an allocation your application now depends on can leave one. The move holds the session lock. An allocation that an intent is releasing keeps its intent until it ends. An allocation that joins an intent has no `submitted_at`, so `pending_timeout` never cancels it.

## One reconciliation step

```python
snapshot = session.inspect()  # library: observe
demand = workload.remaining_parallel_units()  # application: demand
candidates = workload.candidate_shapes(demand, snapshot)  # application: candidates
estimates = session.estimate_worker_starts(candidates)  # library: probe
intent = workload.choose(demand, snapshot, estimates)  # application: decision
added = max(0, intent.count - len(active(snapshot, intent.key)))
session.reconcile_workers(intent, observed=snapshot, max_submissions=added)  # library: revalidate and act
```

The two workloads below use the same hooks and decide differently.

Both count only allocations that are pending or running and not released:

```python
def active(snapshot, key):
    return [w for w in snapshot.workers(key) if w.committed and w.drain_deadline is None]
```

**Independent tasks with checkpoints and cheap startup.** Any allocation that starts soon is useful while enough tasks remain, even a short one: unfinished tasks run again after its workers leave. The intent grows by one allocation per call and shrinks as soon as the work runs out.

```python
def batch_intent(remaining_tasks, snapshot, estimates, tasks_per_allocation=4):
    live = active(snapshot, "batch")
    wanted = -(-remaining_tasks // tasks_per_allocation)  # ceiling division
    starts_now = [e for e in estimates if e.starts_within(120)]
    return WorkerIntent(
        key="batch",
        count=min(wanted, len(live) + (1 if starts_now else 0)),
        config=starts_now[0].config if starts_now else None,
        pending_timeout=120,  # a start that does not happen is cancelled
    )
```

**Work behind an expensive model initialization.** An allocation must stay long enough to amortize its startup, so short windows are declined even when GPUs are free. `time_limit_seconds` on an estimate is the walltime the candidate would request, including an inherited one.

```python
def model_intent(startup_seconds, work_left, snapshot, estimates):
    if not work_left:
        return WorkerIntent(key="model", count=0, drain_timeout=900)
    head_left = snapshot.head.time_left_seconds
    worth_it = [
        e
        for e in estimates
        if e.starts_within(300)
        and e.time_limit_seconds is not None
        and e.time_limit_seconds >= 4 * startup_seconds
        # Workers should not outlive the head they join.
        and (head_left is None or e.time_limit_seconds <= head_left - 600)
    ]
    return WorkerIntent(
        key="model",
        count=len(active(snapshot, "model")) + (1 if worth_it else 0),
        config=worth_it[0].config if worth_it else None,
        drain_timeout=900,  # let in-flight requests finish
    )
```

Given a 20-minute window that starts now, the batch workload adds an allocation and the model workload declines it; given a 2-hour window, both accept.

Further policy stays with your application: how much demand remains, how to size actor pools, batching and GPU memory needs, hardware preferences, fairness between runs, and when startup cost outweighs useful work. A per-run ceiling cannot establish account-wide headroom either: account and QoS limits and other jobs' commitments also decide admission. Probe sparingly, for example at most once a minute, and count pending allocations and successors as committed. To use free Ray capacity inside the current allocations, see `wait_for_stable_ray_resources` and `run_with_ray_reserve_topup` in `dagster_slurm.ray`; that capacity is a different question from adding Slurm allocations.

## Recovery and limits

The intent's bookkeeping lives in the session's `allocation.json`: worker records carry their intent, submission time and release deadline, and `worker_submissions` holds submissions that have not recorded their job ID yet. Both survive head elections and successor promotions. Worker allocations keep the session's job name, so session teardown cancels them with the rest of the run.

The session lock is a directory on the cluster's shared filesystem. A process killed while it holds the lock, for example by `SIGKILL`, leaves the lock behind, and later calls time out until it is removed.
