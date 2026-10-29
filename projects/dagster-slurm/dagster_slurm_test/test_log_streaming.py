"""Slurm stdout/stderr streaming while waiting for a job.

Each run used to hold two `tail -F` SSH sessions open for its whole lifetime.
sshd's MaxSessions caps the sessions on the shared ControlMaster connection
(10 by default), so a handful of concurrent runs exhausted it. The logs are
now read in the same command as the status poll.
"""

import os
import subprocess
from pathlib import Path

from dagster_slurm.pipes_clients.slurm_pipes_client import SlurmPipesClient


class _LocalShellPool:
    """Run pooled commands with the local bash, against real files."""

    def __init__(self, bin_dir: Path, state_file: Path):
        self.commands: list[str] = []
        self.env = {
            **os.environ,
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            "FAKE_SLURM_STATE": str(state_file),
        }

    def run(self, cmd: str, timeout=None) -> str:
        self.commands.append(cmd)
        return subprocess.run(
            ["bash", "--noprofile", "--norc", "-c", cmd],
            capture_output=True,
            text=True,
            env=self.env,
            check=True,
        ).stdout


def _fake_slurm(tmp_path: Path) -> tuple[Path, Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for tool in ("squeue", "sacct"):
        script = bin_dir / tool
        script.write_text('#!/bin/sh\ncat "$FAKE_SLURM_STATE"\n')
        script.chmod(0o755)
    state_file = tmp_path / "state"
    state_file.write_text("RUNNING\n")
    return bin_dir, state_file


def test_job_logs_are_read_with_the_status_poll(
    slurm_pipes_client: SlurmPipesClient, monkeypatch, tmp_path, capsys
):
    bin_dir, state_file = _fake_slurm(tmp_path)
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    out = run_dir / "slurm-7.out"
    err = run_dir / "slurm-7.err"
    out.write_text("line1\n\nline2\npart")  # job is mid-way through a line
    err.write_text("e1\n")

    def advance_job(seconds, job_id):
        # The first sleep precedes any poll; after the first poll, finish the job.
        advance_job.calls += 1
        if advance_job.calls == 2:
            with out.open("a") as handle:
                handle.write("ial\nline3\n")
            state_file.write_text("COMPLETED\n")

    advance_job.calls = 0
    monkeypatch.setattr(slurm_pipes_client, "_interruptible_sleep", advance_job)
    monkeypatch.setattr(
        slurm_pipes_client, "_store_supervisor_heartbeat", lambda *a, **k: None
    )
    monkeypatch.setattr(slurm_pipes_client, "_is_run_canceling", lambda *a, **k: False)

    spawned: list[list[str]] = []
    real_popen = subprocess.Popen

    def recording_popen(args, *a, **k):
        spawned.append(list(args))
        return real_popen(args, *a, **k)

    monkeypatch.setattr(subprocess, "Popen", recording_popen)

    pool = _LocalShellPool(bin_dir, state_file)
    reader = type("Reader", (), {"closed_message": None})()
    slurm_pipes_client._wait_for_job_with_streaming(
        7, pool, str(run_dir), message_reader=reader, poll_timeout=60
    )

    # No long-lived ssh session was opened for the logs.
    assert not [args for args in spawned if args[0] == "ssh"]
    # Every round trip was a status poll that also carried the log reads.
    assert pool.commands and all("squeue" in cmd for cmd in pool.commands)

    captured = capsys.readouterr()
    stdout_lines = [
        line for line in captured.out.splitlines() if line.startswith("[SLURM] ")
    ]
    assert stdout_lines == [
        "[SLURM] line1",
        "[SLURM] line2",
        "[SLURM] partial",
        "[SLURM] line3",
    ]
    stderr_lines = [
        line for line in captured.err.splitlines() if line.startswith("[SLURM ERR] ")
    ]
    assert stderr_lines == ["[SLURM ERR] e1"]
    assert reader._streamed_lines == {"stdout": 4, "stderr": 1}
