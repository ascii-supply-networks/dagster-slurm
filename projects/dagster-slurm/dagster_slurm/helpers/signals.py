"""Shared signal forwarding for standalone jobs and session payload steps."""

import shlex
import signal


def build_pre_timeout_supervisor_script(
    workload_script_path: str,
    signal_before_timeout: str,
    marker_path: str,
    *,
    registration_script: str = "",
    drain_request_path: str | None = None,
) -> str | None:
    """Build a wrapper that forwards Slurm's pre-timeout signal to the workload.

    Slurm's ``B:`` signal targets only the batch shell, so the wrapper
    forwards it to an isolated workload process group and waits for signal
    handlers to flush checkpoints.

    The wrapper deliberately does *not* decide the outcome. "Exited 0 after
    the signal" covers both a job that finished all of its work with time to
    spare and one that flushed a partial checkpoint and gave up, and nothing
    at this level can tell them apart. Forcing a failure turned the first
    case into a false failure that discarded a complete, materialised run.
    The real exit code is propagated instead, and the signal is recorded in a
    marker file so the Dagster side can report it; whether the work actually
    finished is settled by the Pipes session.

    Untrappable signals already make the batch job fail without supervision.
    """
    signal_name = signal_before_timeout.split("@", maxsplit=1)[0]
    signal_token = signal_name.removeprefix("SIG")
    untrappable_numbers = {signal.SIGKILL.value, signal.SIGSTOP.value}
    if signal_token in {"KILL", "STOP"} or (
        signal_token.isdigit() and int(signal_token) in untrappable_numbers
    ):
        return None

    quoted_signal = shlex.quote(signal_name)
    quoted_workload_path = shlex.quote(workload_script_path)
    quoted_marker_path = shlex.quote(marker_path)
    drain_guard = ""
    if drain_request_path is not None:
        drain_guard = f"""
if [ -f {shlex.quote(drain_request_path)} ]; then
  printf '%s\\n' "$_dagster_slurm_signal" > {quoted_marker_path}
  exit 0
fi
"""
    return f"""#!/bin/bash
set -uo pipefail

_dagster_slurm_signal={quoted_signal}
_dagster_slurm_signal_received=0
_dagster_slurm_workload_pid=""
_dagster_slurm_isolated_group=0

_dagster_slurm_forward_signal() {{
  _dagster_slurm_signal_received=1
  # Recorded for the Dagster side; the exit code stays the workload's own.
  printf '%s\n' "$_dagster_slurm_signal" > {quoted_marker_path} 2>/dev/null || true
  echo "Dagster Slurm workload received pre-timeout signal $_dagster_slurm_signal; forwarding it and preserving the workload exit code." >&2
  if [[ -z "$_dagster_slurm_workload_pid" ]]; then
    return
  fi

  if [[ "$_dagster_slurm_isolated_group" -eq 1 ]]; then
    kill -s "$_dagster_slurm_signal" -- "-$_dagster_slurm_workload_pid" 2>/dev/null || true
  else
    kill -s "$_dagster_slurm_signal" "$_dagster_slurm_workload_pid" 2>/dev/null || true
  fi
}}

trap _dagster_slurm_forward_signal "$_dagster_slurm_signal"
{registration_script}
{drain_guard}

if command -v setsid >/dev/null 2>&1; then
  setsid bash {quoted_workload_path} &
  _dagster_slurm_isolated_group=1
else
  bash {quoted_workload_path} &
fi
_dagster_slurm_workload_pid=$!
if [[ "$_dagster_slurm_signal_received" -eq 1 ]]; then
  _dagster_slurm_forward_signal
fi

_dagster_slurm_workload_exit=0
while true; do
  wait "$_dagster_slurm_workload_pid"
  _dagster_slurm_wait_exit=$?
  if [[ "$_dagster_slurm_signal_received" -eq 1 ]]; then
    # The trap interrupts wait with 128+signal even when the workload's own
    # handler exits cleanly. Wait again to reap the workload's real status.
    _dagster_slurm_signal_received=0
    continue
  fi
  _dagster_slurm_workload_exit=$_dagster_slurm_wait_exit
  break
done

trap - "$_dagster_slurm_signal"

exit "$_dagster_slurm_workload_exit"
"""
