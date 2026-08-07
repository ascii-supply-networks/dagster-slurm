"""``defer_cleanup``/``cleanup_deferred_run_dir`` regression coverage.

See issue #205: ``_schedule_async_cleanup`` fires a fire-and-forget
``nohup rm -rf <run_dir> &`` after every ``run()`` call, including in
session mode where a caller (e.g. a per-wave loop) intentionally invokes
``run()`` repeatedly against the same deterministic ``run_dir``. The next
call's ``mkdir -p``/upload into that same path can race the still-in-flight
background deletion from the previous call. ``defer_cleanup=True`` lets such
a caller skip that per-call cleanup and remove the directory itself, once,
via ``cleanup_deferred_run_dir``.
"""

from pathlib import Path
from typing import Any, cast

from dagster_slurm import BashLauncher
from dagster_slurm.pipes_clients.slurm_pipes_client import SlurmPipesClient

from .test_env_caching import FakePool, configure_client_for_local_run, make_context


def _make_client(pool: FakePool) -> SlurmPipesClient:
    from types import SimpleNamespace

    return SlurmPipesClient(
        slurm_resource=cast(
            Any,
            SimpleNamespace(
                ssh="dummy",
                queue=SimpleNamespace(num_nodes=1),
                remote_base="/remote/base",
            ),
        ),
        launcher=BashLauncher(),
    )


def _rm_commands(pool: FakePool) -> list[str]:
    return [cmd for cmd in pool.commands if "rm -rf" in cmd]


def test_default_behavior_still_cleans_up_every_call(monkeypatch, tmp_path: Path):
    """Regression guard: every existing caller (defer_cleanup unset) is unaffected."""
    pool = FakePool()
    client = _make_client(pool)
    configure_client_for_local_run(client, monkeypatch, pool)

    payload = tmp_path / "payload.py"
    payload.write_text("print('hello')")

    client.run(context=make_context(), payload_path=str(payload))

    assert _rm_commands(pool), (
        "run() without defer_cleanup must still trigger cleanup, exactly as "
        "before this change"
    )


def test_defer_cleanup_skips_the_async_cleanup_this_call_would_trigger(
    monkeypatch, tmp_path: Path
):
    pool = FakePool()
    client = _make_client(pool)
    configure_client_for_local_run(client, monkeypatch, pool)

    payload = tmp_path / "payload.py"
    payload.write_text("print('hello')")

    client.run(context=make_context(), payload_path=str(payload), defer_cleanup=True)

    assert not _rm_commands(pool), (
        "defer_cleanup=True must suppress this call's own async cleanup so a "
        "later call reusing the same run_dir cannot race an in-flight delete"
    )


def test_cleanup_deferred_run_dir_removes_the_same_run_dir_run_used(
    monkeypatch, tmp_path: Path
):
    """The caller's later, explicit cleanup targets exactly the run_dir run() used.

    ``run_dir`` is deterministic (remote_base + run_id + step/partition/
    mapping identity), so ``cleanup_deferred_run_dir`` must recompute the
    identical path without any state threaded through from the deferred
    ``run()`` call.
    """
    pool = FakePool()
    client = _make_client(pool)
    configure_client_for_local_run(client, monkeypatch, pool)

    payload = tmp_path / "payload.py"
    payload.write_text("print('hello')")
    context = make_context()

    client.run(context=context, payload_path=str(payload), defer_cleanup=True)
    assert not _rm_commands(pool)

    client.cleanup_deferred_run_dir(context=context)

    rm_commands = _rm_commands(pool)
    assert len(rm_commands) == 1
    assert "/remote/base/runs/run123/step" in rm_commands[0]


def test_cleanup_deferred_run_dir_is_a_noop_in_debug_mode(monkeypatch, tmp_path: Path):
    pool = FakePool()
    client = _make_client(pool)
    client.debug_mode = True
    configure_client_for_local_run(client, monkeypatch, pool)

    client.cleanup_deferred_run_dir(context=make_context())

    assert not _rm_commands(pool)


def test_defer_cleanup_also_suppresses_failure_path_cleanup(
    monkeypatch, tmp_path: Path
):
    """A failed wave with defer_cleanup=True must not delete run_dir either.

    The wave loop this exists for may retry (reusing the same run_dir) after
    a failure, so per-call failure cleanup must defer exactly like success
    cleanup does.
    """
    pool = FakePool()
    client = _make_client(pool)
    configure_client_for_local_run(client, monkeypatch, pool)
    client.cleanup_on_failure = True

    def _boom(**_kwargs):
        raise RuntimeError("submission failed")

    monkeypatch.setattr(client, "_execute_standalone", _boom)

    payload = tmp_path / "payload.py"
    payload.write_text("print('hello')")

    try:
        client.run(
            context=make_context(), payload_path=str(payload), defer_cleanup=True
        )
    except RuntimeError:
        pass

    assert not _rm_commands(pool)
