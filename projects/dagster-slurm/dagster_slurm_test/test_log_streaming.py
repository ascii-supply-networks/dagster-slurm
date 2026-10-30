"""Slurm stdout/stderr streaming while waiting for a job.

Each run used to follow its two log files with a long-lived ``tail -F`` SSH
session each, or, without a ControlMaster, with two polling loops that opened
a full SSH connection per poll. A few concurrent runs exhausted sshd's
MaxSessions or MaxStartups that way. The logs are now read by the status poll.
"""

import os
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from dagster_slurm.helpers.ssh_pool import SSHConnectionPool
from dagster_slurm.pipes_clients.slurm_pipes_client import (
    SlurmPipesClient,
    _SlurmLogFollower,
)
from dagster_slurm.resources.ssh import SSHConnectionResource


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


class _PasswordShellPool(_LocalShellPool):
    """Run pooled commands like password authentication does: under a pty.

    The pty turns every newline into CRLF, and ``_run_with_password`` answers
    anything in the output that looks like a password or OTP prompt.
    """

    def run(self, cmd: str, timeout=None) -> str:
        self.commands.append(cmd)
        pool = SSHConnectionPool(
            SSHConnectionResource(host="localhost", user="nobody", password="unused")
        )
        command = [
            "env",
            f"PATH={self.env['PATH']}",
            f"FAKE_SLURM_STATE={self.env['FAKE_SLURM_STATE']}",
            *["bash", "--noprofile", "--norc", "-c", cmd],
        ]
        result = pool._run_with_password(command, "unused", timeout=30)
        assert result.returncode == 0, result.stdout
        return result.stdout


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


def _wait_for_job(
    client: SlurmPipesClient,
    monkeypatch,
    tmp_path: Path,
    pool_class: type[_LocalShellPool],
    steps: list[dict[str, bytes | str]],
) -> tuple[_LocalShellPool, SimpleNamespace]:
    """Run the wait loop; before poll N, apply ``steps[N]`` to the fake job.

    A step appends bytes to ``out`` or ``err`` and may set the Slurm ``state``.
    """
    bin_dir, state_file = _fake_slurm(tmp_path)
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    files = {"out": run_dir / "slurm-7.out", "err": run_dir / "slurm-7.err"}
    sleeps = 0

    def advance_job(seconds, job_id):
        nonlocal sleeps
        if sleeps < len(steps):
            for name, value in steps[sleeps].items():
                if name == "state":
                    state_file.write_text(f"{value}\n")
                else:
                    with files[name].open("ab") as handle:
                        handle.write(cast(bytes, value))
        sleeps += 1

    monkeypatch.setattr(client, "_interruptible_sleep", advance_job)
    monkeypatch.setattr(client, "_store_supervisor_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(client, "_is_run_canceling", lambda *a, **k: False)
    monkeypatch.setattr(client, "_read_pre_timeout_signal", lambda *a, **k: None)

    pool = pool_class(bin_dir, state_file)
    reader = SimpleNamespace(closed_message=None)
    client._wait_for_job_with_streaming(
        7,
        cast(SSHConnectionPool, pool),
        str(run_dir),
        message_reader=reader,
        poll_timeout=60,
    )
    return pool, reader


def _forwarded(captured: str, prefix: str) -> list[str]:
    return [line for line in captured.splitlines() if line.startswith(prefix)]


@pytest.mark.parametrize("pool_class", [_LocalShellPool, _PasswordShellPool])
def test_job_logs_are_read_with_the_status_poll(
    slurm_pipes_client: SlurmPipesClient,
    monkeypatch,
    tmp_path,
    capsys,
    pool_class: type[_LocalShellPool],
):
    spawned: list[list[str]] = []
    real_popen = subprocess.Popen

    def recording_popen(args, *a, **k):
        spawned.append(list(args))
        return real_popen(args, *a, **k)

    monkeypatch.setattr(subprocess, "Popen", recording_popen)

    pool, reader = _wait_for_job(
        slurm_pipes_client,
        monkeypatch,
        tmp_path,
        pool_class,
        [
            # The job is mid-way through a line; "OTP" would trip a naive
            # password-prompt matcher.
            {"out": b"line1\n\nOTP line2\npart", "err": b"e1\n"},
            {"out": b"ial\nline3\n", "state": "COMPLETED"},
        ],
    )

    # No long-lived ssh session was opened for the logs.
    assert not [args for args in spawned if args[0] == "ssh"]
    # Every round trip was a status poll that also carried the log reads.
    assert pool.commands and all("squeue" in cmd for cmd in pool.commands)

    captured = capsys.readouterr()
    assert _forwarded(captured.out, "[SLURM] ") == [
        "[SLURM] line1",
        "[SLURM] OTP line2",
        "[SLURM] partial",
        "[SLURM] line3",
    ]
    assert _forwarded(captured.err, "[SLURM ERR] ") == ["[SLURM ERR] e1"]
    # Blank lines count too, so the fallback after the job lines up with them.
    assert reader._streamed_lines == {"stdout": 5, "stderr": 1}


def test_each_poll_reads_only_the_new_bytes(
    slurm_pipes_client: SlurmPipesClient, monkeypatch, tmp_path, capsys
):
    pool, _ = _wait_for_job(
        slurm_pipes_client,
        monkeypatch,
        tmp_path,
        _LocalShellPool,
        [
            {"out": b"first\nsec"},
            {"out": b"ond\n"},
            {"state": "COMPLETED"},
        ],
    )

    # The partial "sec" is read again; complete lines are not.
    offsets = [cmd.split("tail -c +")[1].split()[0] for cmd in pool.commands]
    assert offsets == ["1", "7", "14"]
    assert _forwarded(capsys.readouterr().out, "[SLURM] ") == [
        "[SLURM] first",
        "[SLURM] second",
    ]


def test_the_last_line_is_forwarded_when_a_job_fails(
    slurm_pipes_client: SlurmPipesClient, monkeypatch, tmp_path, capsys
):
    with pytest.raises(RuntimeError, match="did not complete successfully"):
        _wait_for_job(
            slurm_pipes_client,
            monkeypatch,
            tmp_path,
            _LocalShellPool,
            [{"err": b"Traceback\nValueError: no newline", "state": "FAILED"}],
        )

    assert _forwarded(capsys.readouterr().err, "[SLURM ERR] ") == [
        "[SLURM ERR] Traceback",
        "[SLURM ERR] ValueError: no newline",
    ]


def test_undecodable_output_and_long_lines_are_forwarded(
    slurm_pipes_client: SlurmPipesClient, monkeypatch, tmp_path, capsys
):
    monkeypatch.setattr(_SlurmLogFollower, "MAX_BYTES_PER_POLL", 8)

    _wait_for_job(
        slurm_pipes_client,
        monkeypatch,
        tmp_path,
        _LocalShellPool,
        [{"out": b"bad \xff\nlonger-than-a-read\n"}, {}, {}, {"state": "COMPLETED"}],
    )

    assert _forwarded(capsys.readouterr().out, "[SLURM] ") == [
        "[SLURM] bad �",
        "[SLURM] longer-t",
        "[SLURM] han-a-re",
        "[SLURM] ad",
    ]


def test_a_failing_log_write_does_not_stop_job_supervision(
    slurm_pipes_client: SlurmPipesClient, monkeypatch, tmp_path
):
    class BrokenStdout:
        def write(self, _text):
            raise OSError("stdout is gone")

        def flush(self):
            pass

    monkeypatch.setattr("sys.stdout", BrokenStdout())

    _wait_for_job(
        slurm_pipes_client,
        monkeypatch,
        tmp_path,
        _LocalShellPool,
        [{"out": b"lost\n"}, {"state": "COMPLETED"}],
    )


def test_status_output_without_log_sections_is_returned_unchanged():
    follower = _SlurmLogFollower(
        [("stdout", "/remote/slurm-1.out", None, "[SLURM] ")],
        cast(Any, None),
        cast(Any, None),
    )

    assert follower.consume("RUNNING\n") == "RUNNING\n"
