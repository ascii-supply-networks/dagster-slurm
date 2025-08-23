import os
import re
import shlex
import textwrap
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from threading import Event, Thread
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    PipesFileContextInjector,
    PipesFileMessageReader,
    asset,
    open_pipes_session,
    PipesContextInjector,
    PipesMessageReader
)
import json
from dagster_pipes import PipesDefaultMessageWriter, PipesDefaultContextLoader, PipesDefaultMessageWriter

from .helpers import (
    _detect_partition,
    _dexec,
    _find_controller,
    _put_file,
    _submit_python,
    _tail_logs,
    _wait_done,
    upload_file,
)

CONTAINER = "slurmctld"
CONTAINER_DATA_DIR = "/data"
HOST_SHARED = os.path.abspath("./shared")
PIPES_BASE = os.path.join(HOST_SHARED, "pipes")

LOCAL_PAYLOAD = os.environ.get(
    "LOCAL_EXTERNAL_FILE",
    str(
        (
            Path(__file__).resolve().parents[3]
            / "dagster_slurm_example"
            / "defs"
            / "shell"
            / "external_file.py"
        ).resolve()
    ),
)

PYTHON_IN_NODE = os.environ.get("PYTHON_IN_NODE", "python3")

PYTHON_IN_NODE = "/usr/bin/python3"
DEFAULT_LOCAL_SCRIPT = os.path.join(os.path.dirname(__file__), "test.py")
TERMINAL = {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "PREEMPTED", "NODE_FAIL"}


def job_state(jid):
    o, _, _ = _dexec(CONTAINER, f"squeue -h -j {jid} -o '%T' || true")
    s = (o or "").strip()
    if s:
        return s
    o, _, _ = _dexec(CONTAINER, f"sacct -X -n -j {jid} -o State || true")
    s = (o or "").strip()
    return s.split()[0] if s else ""


@asset(name="slurm_submit_pipes")
def slurm_submit_remote_pipes(context: AssetExecutionContext):  # noqa: C901
    os.makedirs(PIPES_BASE, exist_ok=True)
    _dexec(
        CONTAINER,
        "mkdir -p /data/{logs,results,pipes} && chmod 1777 /data/{logs,results,pipes}",
    )

    run_id = context.run_id or uuid.uuid4().hex
    host_run_dir = os.path.join(PIPES_BASE, run_id)
    os.makedirs(host_run_dir, exist_ok=True)
    container_run_dir = f"{CONTAINER_DATA_DIR}/pipes/{run_id}"

    cont_context = f"{container_run_dir}/context.json"
    cont_messages = f"{container_run_dir}/messages.jsonl"

    injector = DockerFileContextInjector(CONTAINER, cont_context)
    reader   = DockerFileMessageReader(CONTAINER, cont_messages, include_stdio_in_messages=True)

    remote_payload_uri = (
        f"docker://{CONTAINER}{container_run_dir}/external_file_pipes.py"
    )
    upload_file(LOCAL_PAYLOAD, remote_payload_uri)

    with open_pipes_session(
        context=context,
        context_injector=injector,
        message_reader=reader,
        extras={"via": "slurm-docker"},
    ) as session:
        env = session.get_bootstrap_env_vars()
        exports = "\n".join(f"export {k}={shlex.quote(v)}" for k, v in env.items())
        _dexec(CONTAINER, f"mkdir -p {shlex.quote(container_run_dir)} && cat > {container_run_dir}/pipes.env <<'EOF'\n{exports}\nEOF")

        bootstrap = textwrap.dedent(f"""\
            set -euxo pipefail
            source {container_run_dir}/pipes.env
            echo "CTX=$DAGSTER_PIPES_CONTEXT_PATH"
            echo "MSG=$DAGSTER_PIPES_MESSAGES_PATH"
            mkdir -p "$(dirname "$DAGSTER_PIPES_MESSAGES_PATH")"

            export MAMBA_ROOT_PREFIX=/data/micromamba
            if [ ! -x "$MAMBA_ROOT_PREFIX/bin/micromamba" ]; then
            mkdir -p "$MAMBA_ROOT_PREFIX/bin"
            curl -Ls https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xvj bin/micromamba -O > "$MAMBA_ROOT_PREFIX/bin/micromamba"
            chmod +x "$MAMBA_ROOT_PREFIX/bin/micromamba"
            fi

            # build a py>=3.9 env and install dagster-pipes (no output suppression so we can debug)
            "$MAMBA_ROOT_PREFIX/bin/micromamba" create -y -p /data/envs/pipes python=3.11 pip
            /data/envs/pipes/bin/pip install --no-cache-dir dagster-pipes

            # sanity: show version & env that the external will see
            /data/envs/pipes/bin/python - <<'PY'
                import dagster_pipes, os
                print("PIPES_VERSION", dagster_pipes.__version__)
                print("CTX", os.environ.get("DAGSTER_PIPES_CONTEXT_PATH"))
                print("MSG", os.environ.get("DAGSTER_PIPES_MESSAGES_PATH"))
                PY

            /data/envs/pipes/bin/python {container_run_dir}/external_file_pipes.py
        """)
        opts = [
            "-J",
            "pipes_ext",
            "-D",
            container_run_dir,
            "-o",
            f"{container_run_dir}/slurm-%j.out",
            "-e",
            f"{container_run_dir}/slurm-%j.err",
            "-t",
            "00:05:00",
            "-c",
            "1",
            "--mem=512M",
        ]
        part = _detect_partition()
        if part:
            opts += ["-p", part]

        _dexec(
            CONTAINER,
            f"cat > {container_run_dir}/job.sbatch <<'SB'\n#!/bin/bash\n{bootstrap}\nSB\nchmod +x {container_run_dir}/job.sbatch",
        )
        out, _, _ = _dexec(
            CONTAINER,
            "sbatch "
            + " ".join(shlex.quote(x) for x in opts)
            + f" {container_run_dir}/job.sbatch",
        )

        m = re.search(r"Submitted batch job (\d+)", out)
        if not m:
            raise RuntimeError(f"Could not parse job id.\nstdout:\n{out}")
        job_id = int(m.group(1))
        context.log.info(f"Submitted job {job_id}")

        while True:
            st = job_state(job_id)
            if st in TERMINAL:
                break
            time.sleep(0.5)

        for ev in session.get_results():
            yield ev
    #    yield Output(None)

    return MaterializeResult(
        metadata={
            "job_id": job_id,
            "run_dir_host": host_run_dir,
            "run_dir_container": container_run_dir,
            "payload_local": LOCAL_PAYLOAD,
        }
    )


@asset(name="slurm_submit_external")
def slurm_submit_external(context: AssetExecutionContext) -> MaterializeResult:
    """
    Upload a local script (default: external_file.py) to /data, submit to Slurm in Docker,
    wait for completion, and materialize what happened.
    """
    controller = _find_controller()
    partition = _detect_partition(controller)

    local_script = os.environ.get("LOCAL_EXTERNAL_FILE", DEFAULT_LOCAL_SCRIPT)
    context.log.info(f"Using local script: {local_script}")

    remote_script = f"{CONTAINER_DATA_DIR}/external_file.py"
    _put_file(controller, local_script, remote_script)

    submit = _submit_python(
        controller,
        remote_script,
        job_name="external",
        minutes=2,
        cpus=1,
        mem_mb=256,
        args=["--message", "hello from dagster"],
        partition=partition,
    )
    context.log.info(
        f"Submitted job id={submit.job_id} on node={submit.node or 'unknown'}"
    )

    final_state = _wait_done(controller, submit.job_id)
    context.log.info(f"Job {submit.job_id} finished: {final_state}")

    out_txt, err_txt = _tail_logs(controller, "external", submit.job_id, n=80)

    md = {
        "job_id": submit.job_id,
        "node": submit.node or "unknown",
        "state": final_state,
        "stdout_tail": out_txt,
        "stderr_tail": err_txt,
    }
    return MaterializeResult(metadata=md)


class DockerFileContextInjector(PipesContextInjector):
    """
    Write the context JSON *inside* the container and pass the container path to the external.
    """
    def __init__(self, container: str, container_path: str):
        self.container = container
        self.container_path = container_path

    @contextmanager
    def inject_context(self, context_data):
        # ensure dir exists in container
        parent = shlex.quote(os.path.dirname(self.container_path))
        _dexec(self.container, f"mkdir -p {parent}")
        # write context via docker exec heredoc (avoids temp + docker cp)
        payload = json.dumps(context_data)
        _dexec(self.container, f"cat > {shlex.quote(self.container_path)} <<'JSON'\n{payload}\nJSON")
        try:
            yield {PipesDefaultContextLoader.FILE_PATH_KEY: self.container_path}
        finally:
            # optional: leave it in place; remove if you prefer:
            # _dexec(self.container, f"rm -f {shlex.quote(self.container_path)}")
            pass

    def no_messages_debug_text(self) -> str:
        return f"DockerFileContextInjector wrote context to {self.container}:{self.container_path}"

class DockerFileMessageReader(PipesMessageReader):
    """
    Poll a messages file *inside* the container via `docker exec`, while telling the external
    to write to that container path.
    """
    def __init__(self, container: str, container_path: str, include_stdio_in_messages: bool = True, interval: float = 0.5):
        self.container = container
        self.container_path = container_path
        self.include_stdio_in_messages = include_stdio_in_messages
        self.interval = interval

    @contextmanager
    def read_messages(self, handler):
        stop = Event()
        t = Thread(target=self._reader, args=(handler, stop), daemon=True)
        t.start()
        try:
            # Tell the external process to write to the *container* path
            yield {
                PipesDefaultMessageWriter.FILE_PATH_KEY: self.container_path,
                PipesDefaultMessageWriter.INCLUDE_STDIO_IN_MESSAGES_KEY: self.include_stdio_in_messages,
            }
        finally:
            stop.set()
            t.join(timeout=60)

    def _reader(self, handler, stop: Event):
        pos = 0  # byte offset in file
        path_q = shlex.quote(self.container_path)
        while not stop.is_set():
            # size of file (or 0 if not there yet)
            out, _, _ = _dexec(self.container, f"test -f {path_q} && wc -c < {path_q} || echo 0")
            try:
                size = int(out.strip() or "0")
            except ValueError:
                size = 0

            if size > pos:
                # fetch appended bytes starting from pos+1 (tail -c +N is 1-based)
                start = pos + 1
                out, _, _ = _dexec(self.container, f"tail -c +{start} {path_q} || true")
                # move cursor to current end; tolerate slight race
                pos = size
                # deliver complete JSON lines
                for line in out.splitlines():
                    try:
                        msg = json.loads(line)
                        handler.handle_message(msg)
                    except Exception:
                        # ignore partial/invalid lines
                        pass
            time.sleep(self.interval)

    def no_messages_debug_text(self) -> str:
        return f"DockerFileMessageReader expected messages at {self.container}:{self.container_path}"
    
    
class DualPathContextInjector(PipesFileContextInjector):
    """
    Host<->container bridge for FILE-based Pipes.

    Writes the context JSON to 'host_path' (Dagster side), and injects
    'DAGSTER_PIPES_CONTEXT_PATH' pointing at 'container_path' (Slurm side).
    """

    def __init__(self, host_path: str, container_path: str):
        super().__init__(path=host_path)
        self._container_path = container_path
        self.host_path = host_path

    def inject_context(self, context_data):
        inner_cm = super().inject_context(context_data)

        @contextmanager
        def _cm():
            env = inner_cm.__enter__()
            try:
                env = dict(env)
                env["DAGSTER_PIPES_CONTEXT_PATH"] = self._container_path
                yield env
            finally:
                inner_cm.__exit__(None, None, None)

        return _cm()

    def no_messages_debug_text(self) -> str:
        return (
            "DualPathContextInjector wrote context to host "
            f"{self.host_path} and injected container path {self._container_path}."
        )


class DualPathFileMessageReader(PipesFileMessageReader):
    """
    Host<->container bridge for FILE-based Pipes messages.

    Reads messages on 'host_path' (Dagster side) by delegating to an inner
    PipesFileMessageReader, but injects 'DAGSTER_PIPES_MESSAGES_PATH' for
    the external process to the 'container_path'.
    """
    def __init__(self, host_path: str, container_path: str,
                 include_stdio_in_messages: bool = True, cleanup_file: bool = False):
        super().__init__(path=host_path,
                         include_stdio_in_messages=include_stdio_in_messages,
                         cleanup_file=cleanup_file)
        self._container_path = container_path
        self.host_path = host_path

    @contextmanager
    def read_messages(self, handler):
        # Let parent create/tail the HOST file
        with super().read_messages(handler) as params:
            # Rewrite the parameter the job will read from env
            forwarded = dict(params)
            forwarded[PipesDefaultMessageWriter.FILE_PATH_KEY] = self._container_path
            yield forwarded

    def no_messages_debug_text(self) -> str:
        base = super().no_messages_debug_text()
        return f"{base}\nDualPathFileMessageReader redirected external writes to {self._container_path}"