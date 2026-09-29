# Continue a run in a successor allocation

A run-scoped session can queue a successor allocation while its current allocation runs. The caller chooses when to submit, drains its payload at a checkpoint boundary, promotes the successor, and launches the remaining work. Finished work must be committed outside the allocation so the next payload can skip it. To use several allocations at the same time, see [Add worker allocations to a run's Ray cluster](elastic-worker-allocations.md).

Configure a drain signal and, optionally, a minimum backfill walltime:

```python
from dagster_slurm import ComputeResource, RayLauncher, SlurmRunAllocationConfig

compute = ComputeResource(
    mode="slurm",
    slurm=slurm,
    allocation_scope="run",
    default_launcher=RayLauncher(num_gpus_per_node=1),
    run_allocation=SlurmRunAllocationConfig(
        num_nodes=2,
        gpus_per_node=1,
        time_limit="20:00:00",
        time_min="04:00:00",
        signal_before_timeout="TERM@120",
    ),
)
```

`time_min` emits `--time-min`, allowing Slurm to shorten the requested walltime for backfill. It must be positive, no greater than `time_limit`, and longer than the signal's lead time. `session.allocation.end_time` queries `squeue` on every access and returns the scheduler's current end-time string in the cluster's timezone, or `None` if unavailable. Do not calculate the deadline from the originally requested `time_limit`.

Inside an asset, obtain the session with `compute.get_run_allocation_session(context)`. The same relay methods are available on an explicitly configured `SlurmSessionResource`:

```python
session = compute.get_run_allocation_session(context)
predecessor = session.allocation

# Returns after submission; it does not wait for the job to start.
successor = session.submit_successor(
    SlurmRunAllocationConfig(
        time_limit="12:00:00",
        extra_sbatch_directives=["--begin=now+1hour"],
    )
)

# When your scheduling policy decides to hand over:
predecessor.request_drain()

# Wait for your active payload calls to return their drained outcomes, and
# for your scheduler integration to report that successor is RUNNING.
session.promote_successor()

# Subsequent compute.run(...) calls use the successor allocation.
```

Unspecified successor fields inherit the current allocation's configuration, except `extra_sbatch_directives`: these apply to one submission and default to an empty list on every successor. Pass them explicitly each time they are needed; a delayed `--begin` or dependency from an earlier allocation will not repeat. Only one successor can be outstanding. `extra_sbatch_directives` accepts one long-form `--option[=value]` per list entry, for example `--dependency=afterany:123`. Whitespace, duplicate options, and overrides of library-managed directives are rejected. Use the first-class fields for allocation shape, walltime, and signalling. A dependency that prevents overlap requires the predecessor to finish before promotion.

`promote_successor()` requires a RUNNING successor and no active predecessor payloads, including payloads still waiting for `srun` to start. It starts the persistent Ray cluster using the recorded launcher and environment, publishes the successor, updates Dagster run tags, and releases the predecessor. Pass `launcher=...` and `activation_script=...` when the successor needs a different Ray configuration. Coordinate promotion with your payload submissions; a plan prepared against a retired allocation is rejected and must be prepared again.

Ray startup runs outside the session lifecycle locks. Before publishing, promotion checks both allocation identities, successor readiness, and predecessor payloads again. A payload admitted during startup makes promotion fail until that payload finishes. Session subclasses retain their public configuration, SSH pool, logger and Dagster context across submission and reattachment. Override `_make_allocation(...)` to supply an allocation subclass (including custom Ray startup), and `_clone_allocation_config(...)` when extra operational dependencies must be carried over.

Each job keeps its node markers, payload status files, and Ray directory under `jobs/<job_id>/`. The session's `allocation.json` and leases stay at a stable location. A restarted process can find a pending successor even after its predecessor ends. Repeating a completed promotion repairs tag publication and predecessor cleanup while preserving any active successor step's tags. The orphan sensor recognises a live published successor and still recovers supervisors with stale heartbeats. Session teardown cancels all allocations with the session job name, including pending successors.

The session maintains its supervisor heartbeat between payload calls, including while your caller waits for a queued successor. Teardown stops that heartbeat, including when allocations are preserved for reattachment.

## Handle drained work

The allocation shell traps the configured pre-walltime signal, records it, and forwards it only to registered payload steps. It stays alive, and persistent Ray head and worker steps remain available while the payload drains. `request_drain()` sends that same signal to the allocation shell; without a configured pre-walltime signal, on-demand drain uses `TERM`. Draining is permanent for that allocation: later payload starts return a drained outcome without starting work.

Payloads must handle the signal themselves and finish at a safe boundary. Slurm may deliver a pre-walltime signal up to 60 seconds early. KILL and STOP cannot provide a drain window and are rejected for sessions.

A drain received before workload launch skips the workload and records a drained exit zero. If it arrives while `setsid` is creating the workload process group, the supervisor retains the signal until the group exists. The workload must install its handler promptly; a signal received before its handler is installed can still terminate it.

The lower-level `SlurmAllocation.execute()` and `SlurmSessionResource.execute_in_session()` return `SlurmStepExecutionResult` with `drained`, `drain_signal`, and the payload's `exit_code`. A drained non-zero exit is returned as a typed result; an ordinary non-zero exit still raises an error with its log tails. `SlurmAllocationEnded` carries a `.result` with `allocation_state` when the allocation ends while a step runs, whether or not the step wrote an exit status first. A payload prepared for an allocation that was replaced before it started raises it too, with `allocation_state` set to `REPLACED`.

The Pipes/Compute API raises `SlurmStepDrained` for a drained invocation, including exit code zero, so partial work is not silently materialized. Both fresh invocations and reattachments finish reading Pipes messages and emit final logs before raising. The exception exposes `.result` and `.invocation`, a `PipesClientCompletedInvocation` containing all reported custom messages and materializations. Catch it inside your asset's relay loop and use your payload's completion report to decide whether work remains. Exit zero alone does not establish completion (a prelaunch drain also exits zero):

```python
from dagster_slurm import SlurmStepDrained

try:
    invocation = compute.run(
        context=context,
        payload_path="process_documents.py",
        defer_cleanup=True,
    )
except SlurmStepDrained as drained:
    context.log.info(
        f"Payload drained on {drained.result.drain_signal}; "
        f"exit code {drained.result.exit_code}"
    )
    reports = drained.invocation.get_custom_messages()
    if any(report.get("remaining") == 0 for report in reports):
        # Your payload explicitly reported that all work was committed.
        return drained.invocation.get_results(implicit_materializations=False)
    # Promote when the successor is ready, then launch remaining documents.
```

Fresh session invocations use separate subdirectories and Pipes message streams; reattachment reads the original invocation. Use `defer_cleanup=True` to retain invocation files throughout a relay loop, then call `compute.cleanup_deferred_run_dir(context)` when that loop is finished. Allocation selection, successor readiness, checkpointing, and relaunch policy remain in your application.
