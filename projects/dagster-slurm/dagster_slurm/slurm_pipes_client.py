import os
import shlex
import uuid
from pathlib import Path
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Mapping, Any
from string import Template

from .local_runner import LocalRunner
from .docker_runner import DockerRunner

from dagster import (
    AssetExecutionContext,
    PipesClient,
    PipesEnvContextInjector,
    get_dagster_logger,
    open_pipes_session,
    PipesMessageReader,
)

from dagster_pipes import encode_param
from .ssh_helpers import (
    ssh_check,
    scp_put,
    pack_env
)

from .ssh_message_reader import SshExecTailMessageReader
from .local_message_reader import LocalExecTailMessageReader

PipesMessage = Mapping[str, Any]

_ALLOWED_KEYS = {
    "PARTITION_OPTION",
    "JOB_NAME",
    "GIVEN_NODE",
    "NUM_NODES",
    "NUM_GPUS_PER_NODE",
    "LOAD_ENV",
    "COMMAND_PLACEHOLDER",
}

REMOTE_BASE = os.environ.get("SLURM_REMOTE_BASE", "/home/submitter").rstrip("/")
ACTIVATE_SH_DEFAULT = os.environ.get("SLURM_ACTIVATE_SH", "/home/submitter/activate.sh")
REMOTE_PY = os.environ.get("SLURM_PYTHON", "python")

def get_runner(logger):
    # Use an env var to decide which runner to use. Default to local.
    env = os.environ.get("DAGSTER_ENV", "dev").lower()
    if env == "staging":
        logger.info("Using SlurmRunner for staging environment.")
        return DockerRunner(logger)
    logger.info("Using LocalRunner for dev environment.")
    return LocalRunner(logger)

def get_backend() -> str:
    return os.environ.get("COMPUTE_BACKEND", "python").lower()  # python|ray|spark

def _find_environment_sh() -> Optional[Path]:
    """
    Look for examples/environment.sh from a variety of roots so this works
    regardless of current working directory.
    """
    here = Path(__file__).resolve()
    candidates = [
        Path("examples/environment.sh"),
        here.parent.parent.parent.parent / "examples" / "environment.sh",  # repo_root/examples/environment.sh
        here.parents[3] / "examples" / "environment.sh",
        here.parents[4] / "examples" / "environment.sh" if len(here.parents) >= 5 else None,
    ]
    for p in candidates:
        if p and p.exists():
            return p
    return None


def _maybe_examples_pythonpath() -> str:
    """
    Build a PYTHONPATH segment with your three examples projects if present.
    """
    here = Path(__file__).resolve()
    repo_root = here.parents[3]  # projects/dagster-slurm/dagster_slurm/ -> projects -> repo_root
    examples = repo_root / "examples" / "projects"
    parts = []
    for rel in [
        "dagster-slurm-example",
        "dagster-slurm-example-shared",
        "dagster-slurm-example-hpc-workload",
    ]:
        p = examples / rel
        if p.exists():
            parts.append(str(p))
    return ":".join(parts)

class _PipesBaseSlurmClient(PipesClient):
    """
    Minimal, generic Slurm+SSH Pipes client.

    Responsibilities:
      - create per-run remote dirs
      - upload payload
      - write `pipes.env`
      - write/submit `sbatch` script
      - stream Pipes messages while polling Slurm
    """

    def __init__(
        self,
        *,
        # Remote & env bootstrap
        remote_base: Optional[str] = None,
        activate_sh: Optional[str] = None,
        remote_python: Optional[str] = None,
        # sbatch defaults (can be overridden per-run)
        default_partition: Optional[str] = "",
        default_time_limit: Optional[str] = "00:10:00",
        default_cpus: Optional[str] = "1",
        default_mem: Optional[str] = "256M",
        default_mem_per_cpu: Optional[str] = "",  # leave empty to prefer --mem
        # Pipes bits
        poll_interval_seconds: float = 1.0,
        context_injector: Optional[PipesEnvContextInjector] = None,
        message_reader_factory: Optional[Callable[[str], PipesMessageReader]] = None,
    ):
        super().__init__()

        # Logger & runner first (so factory can see runner type)
        self.logger = get_dagster_logger()
        self.runner = get_runner(self.logger)

        # Resolve defaults from env
        self.remote_base = (remote_base or os.environ.get("SLURM_REMOTE_BASE", "/home/submitter")).rstrip("/")
        # NOTE: This is only used as a *fallback* if we don't generate a per-run activate.sh
        self.activate_sh = activate_sh or ACTIVATE_SH_DEFAULT
        self.remote_python = remote_python or os.environ.get("SLURM_PYTHON", "python3")

        self.default_partition = default_partition or os.environ.get("SLURM_PARTITION", "")
        self.default_time_limit = os.environ.get("SLURM_TIME", default_time_limit)
        self.default_cpus = os.environ.get("SLURM_CPUS", default_cpus)
        self.default_mem = os.environ.get("SLURM_MEM", default_mem)
        self.default_mem_per_cpu = os.environ.get("SLURM_MEM_PER_CPU", default_mem_per_cpu)

        self.poll_interval_seconds = poll_interval_seconds
        self.context_injector = context_injector or PipesEnvContextInjector()

        if message_reader_factory:
            self._message_reader_factory = message_reader_factory
        else:
            def _default_reader(path: str) -> PipesMessageReader:
                # Decide based on runner type at call time
                if isinstance(self.runner, LocalRunner):
                    return LocalExecTailMessageReader(path, include_stdio_in_messages=True)
                return SshExecTailMessageReader(path, include_stdio_in_messages=True)
            self._message_reader_factory = _default_reader

        # surfaces for the caller to read after run()
        self.last_job_id: Optional[int] = None
        self.last_remote_run_dir: Optional[str] = None

    # -------------------- helpers --------------------

    def _dev_examples_pythonpath(self) -> str:
        """
        Return colon-separated paths to examples projects so imports work in dev:
        dagster-slurm-example, dagster-slurm-example-shared, dagster-slurm-example-hpc-workload.
        Returns "" if not found.
        """
        here = Path(__file__).resolve()
        root = here
        for _ in range(8):  # walk up to find repo root
            candidate = root / "examples" / "projects"
            if candidate.is_dir():
                base = candidate
                break
            if root.parent == root:
                return ""
            root = root.parent
        else:
            return ""

        wanted = [
            "dagster-slurm-example",
            "dagster-slurm-example-shared",
            "dagster-slurm-example-hpc-workload",
        ]
        parts = [str((base / name)) for name in wanted if (base / name).is_dir()]
        return ":".join(parts)
    def _make_message_reader(self, path: str, is_staging: bool) -> PipesMessageReader:
        if is_staging:
            return self._message_reader_factory(path)  # remote tail via SSH
        # dev ⇒ read locally
        return LocalExecTailMessageReader(path, include_stdio_in_messages=True)

    def _render_template_keep_others(self, template_text: str, params: Dict[str, str]) -> str:
        """(Kept from earlier versions; not used in the current flow)"""
        mapping = {k: str(v) for k, v in (params or {}).items() if k in _ALLOWED_KEYS}
        return Template(template_text).safe_substitute(mapping)

    def _discover_projects_pythonpath(self, local_payload: str) -> str:
        """
        For dev only: find .../examples/projects and add each immediate subdir
        (so imports like dagster_slurm_example[_shared] resolve without installs).
        """
        p = Path(local_payload).resolve()
        projects_dir: Optional[Path] = None
        for anc in p.parents:
            if anc.name == "projects":
                projects_dir = anc
                break
        if not projects_dir or not projects_dir.is_dir():
            return ""
        parts: List[str] = []
        for d in sorted(projects_dir.iterdir()):
            if d.is_dir():
                parts.append(str(d))
        return ":".join(parts)

    def _bootstrap_for_backend(
        self,
        *,
        backend: str,
        exec_run_dir: str,
        remote_payload_path: str,
        remote_python: str,
        session_env: Dict[str, str],
        extra_env: Optional[Dict[str, str]],
    ) -> List[str]:
        messages_path = f"{exec_run_dir}/messages.jsonl"

        lines: List[str] = [
            "#!/bin/bash",
            "set -euo pipefail",
            f': > "{messages_path}" || true',
            f'export DAGSTER_PIPES_MESSAGES="{messages_path}"',
            'export DAGSTER_PIPES_INCLUDE_STDIO="1"',
            'echo "[$(date -Is)] Exec run dir: ' + exec_run_dir + '"',
            'echo "[$(date -Is)] DAGSTER_PIPES_MESSAGES=${DAGSTER_PIPES_MESSAGES:-unset}"',
            # Pipes context (DAGSTER_PIPES_CONTEXT etc.)
            *[f'export {k}={shlex.quote(v)}' for k, v in session_env.items()],
        ]

        if extra_env:
            lines += [f'export {k}={shlex.quote(str(v))}' for k, v in extra_env.items()]

        # If present, install env inside allocation (creates per-run activate.sh)
        lines += [
            (
                f'if [ -f {shlex.quote(exec_run_dir)}/environment.sh ]; then '
                f'  echo "--- installing environment from environment.sh ---"; '
                f'  chmod +x {shlex.quote(exec_run_dir)}/environment.sh && '
                f'  cd {shlex.quote(exec_run_dir)} && ./environment.sh; '
                f'  echo "--- installation complete ---"; '
                f'fi'
            )
        ]

        # Prefer per-run activate.sh (produced by environment.sh). Fallback to global.
        lines += [
            f'if [ -f {shlex.quote(exec_run_dir)}/activate.sh ]; then',
            '  echo "--- sourcing per-run activate.sh ---";',
            f'  source {shlex.quote(exec_run_dir)}/activate.sh;',
            'elif [ -f ' + shlex.quote(self.activate_sh) + ' ]; then',
            '  echo "--- sourcing global activate.sh ---";',
            f'  source {shlex.quote(self.activate_sh)};',
            'else',
            '  echo "WARN: no activate.sh found; continuing without environment activation" >&2;',
            'fi',
        ]

        # Dev-only: add examples to PYTHONPATH so example pkgs import without being packed
        if isinstance(self.runner, LocalRunner):
            pp = self._dev_examples_pythonpath()
            lines += [
                f'export PYTHONPATH="{pp}:${{PYTHONPATH:-}}"' if pp else 'export PYTHONPATH="${PYTHONPATH:-}"',
                'echo "[$(date -Is)] PYTHONPATH=${PYTHONPATH:-<empty>}"',
            ]
        else:
            lines += ['echo "[$(date -Is)] PYTHONPATH=${PYTHONPATH:-<empty>}"']

        # Backend launcher
        if backend == "python":
            lines += [
                'echo "--- running python payload ---"',
                f'exec {shlex.quote(remote_python)} {shlex.quote(remote_payload_path)}',
            ]
            return lines

        if backend == "ray":
            lines += [
                'export RAY_ADDRESS="${RAY_ADDRESS:-auto}"',
                'echo "--- running ray-enabled python payload ---"',
                f'exec {shlex.quote(remote_python)} {shlex.quote(remote_payload_path)}',
            ]
            return lines

        if backend == "spark":
            lines += [
                'echo "--- (spark backend placeholder) running python payload ---"',
                f'exec {shlex.quote(remote_python)} {shlex.quote(remote_payload_path)}',
            ]
            return lines

        raise ValueError(f"Unknown COMPUTE_BACKEND={backend!r}")

    # -------------------- main --------------------

    def run(
        self,
        context: AssetExecutionContext,
        *,
        local_payload: str,
        job_name: str = "pipes_ext",
        results_dir_name: str = "results",
        logs_dir_name: str = "logs",
        time_limit: Optional[str] = None,
        cpus: Optional[str] = None,
        mem: Optional[str] = None,
        mem_per_cpu: Optional[str] = None,
        partition: Optional[str] = None,
        extra_sbatch_args: Optional[Iterable[str]] = None,
        extra_env: Optional[Dict[str, str]] = None,
        slurm_template_path: Optional[str] = None,
        template_params: Optional[Dict[str, str]] = None,
        package_src_dir: str = "../projects/dagster-slurm",
        package_name: str = "dagster-slurm",
        remote_base: Optional[str] = None,
        remote_python: Optional[str] = None,
        extras: Optional[Dict[str, Any]] = None,
    ) -> Iterator:
        """
        Yields Dagster events (logs/materializations) from the remote Pipes process.
        On success, sets `self.last_job_id` and `self.last_remote_run_dir`.
        """
        
        run_id = context.run_id or uuid.uuid4().hex
        backend = get_backend()

        is_staging = isinstance(self.runner, DockerRunner)
        env_sh = Path("examples/environment.sh")
        lock = Path("examples/pixi.lock")
        # if (not env_sh.exists()) or (lock.exists() and env_sh.stat().st_mtime < lock.stat().st_mtime):
        pack_env(log=self.logger)
        # Optional local fallback activate (no longer required)
        local_activate = os.environ.get("LOCAL_ACTIVATE_SH", "")
        if local_activate:
            self.activate_sh = os.path.expanduser(local_activate)

        # Resolve locations
        exec_base_dir = (remote_base or self.remote_base) if is_staging else "/tmp/dagster_local_runs"
        remote_python = remote_python or self.remote_python
        temp_dir_base = "/tmp" if is_staging else exec_base_dir

        exec_run_dir = f"{exec_base_dir}/pipes/{run_id}"
        temp_run_dir = f"{temp_dir_base}/pipes/{run_id}"
        Path(temp_run_dir).mkdir(parents=True, exist_ok=True)

        # The messages file lives where the driver runs.
        message_reader_path = f"{exec_run_dir}/messages.jsonl"
        reader = self._make_message_reader(message_reader_path, is_staging)

        # Ensure execution directory exists (locally or remotely)
        self.runner.run_command(f"mkdir -p {shlex.quote(exec_run_dir)}")
        # Pre-create messages file early to avoid 'not created yet' warning
        self.runner.run_command(f': > {shlex.quote(message_reader_path)} || true')

        with open_pipes_session(
            context=context,
            context_injector=self.context_injector,
            message_reader=reader,
            extras=extras,
        ) as session:
            # Always ship the environment installer if it exists in examples/
            examples_env = Path("examples/environment.sh")
            if examples_env.exists():
                self.runner.put_file(str(examples_env), f"{exec_run_dir}/environment.sh")
            # Ship payload
            payload_filename = Path(local_payload).name
            remote_payload_path = f"{exec_run_dir}/{payload_filename}"
            self.runner.put_file(local_payload, remote_payload_path)

            # Build env exported to the job — force legacy file-writer compatibility
            pipes_env = session.get_bootstrap_env_vars()

            # Force the external writer to use a file path, in the format expected by your
            # dagster_pipes version (encoded params in DAGSTER_PIPES_MESSAGES).
            #pipes_env["DAGSTER_PIPES_MESSAGES"] = encode_param(
            #    {"path": message_reader_path, "stdio": True}
            #)

            # Ensure no legacy/alternate params vars sneak in (avoid double-specifying).
            for k in ("DAGSTER_PIPES_MESSAGE_PARAMS", "DAGSTER_PIPES_MESSAGES_PARAMS"):
                pipes_env.pop(k, None)
            job_script_lines = self._bootstrap_for_backend(
                backend=backend,
                exec_run_dir=exec_run_dir,
                remote_payload_path=remote_payload_path,
                remote_python=remote_python,
                session_env=pipes_env,
                extra_env=extra_env,
            )

            # Write job.sh locally
            local_job_script = Path(temp_run_dir) / "job.sh"
            local_job_script.write_text("\n".join(job_script_lines) + "\n")

            # Compute the remote script path
            remote_job_script = f"{exec_run_dir}/job.sh"

            # In dev, exec_dir == temp_dir_base, so paths can be identical.
            # Avoid copying a file onto itself.
            if os.path.abspath(str(local_job_script)) != os.path.abspath(remote_job_script):
                self.runner.put_file(str(local_job_script), remote_job_script)

            # Make sure it's executable (works for both local/remote runners)
            self.runner.run_command(f"chmod +x {shlex.quote(remote_job_script)}")

            # submit: LocalRunner runs synchronously; DockerRunner submits via Slurm.
            job_id = self.runner.submit_job(remote_job_script)
            context.log.info(f"Submitted job {job_id} via {type(self.runner).__name__}")
            self.runner.wait_for_job(job_id)

            self.last_job_id = self.runner.last_job_id
            self.last_remote_run_dir = exec_run_dir

            for ev in session.get_results():
                yield ev
