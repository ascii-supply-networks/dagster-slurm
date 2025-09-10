import subprocess
import shlex
from pathlib import Path
import os

class LocalRunner:
    """Executes commands and manages files on the local machine for dev."""

    def __init__(self, logger):
        self.logger = logger
        self.last_job_id = None
        self.last_remote_run_dir = None

    def run_command(self, cmd: str, is_check: bool = True) -> str:
        self.logger.info(f"Running local command: {cmd}")
        process = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            check=is_check,
        )
        if process.stdout:
            self.logger.debug(f"Local stdout:\n{process.stdout}")
        if process.stderr:
            self.logger.warning(f"Local stderr:\n{process.stderr}")
        return process.stdout

    def put_file(self, local_path: str, remote_path: str) -> None:
        self.logger.info(f"Copying '{local_path}' to '{remote_path}'")
        Path(remote_path).parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(["cp", local_path, remote_path], check=True)

    def submit_job(self, job_script_path: str) -> int:
        self.logger.info(f"Submitting local job by running script: {job_script_path}")
        self.run_command(f"bash {shlex.quote(job_script_path)}")
        self.last_job_id = os.getpid()
        return self.last_job_id

    def wait_for_job(self, job_id: int):
        self.logger.info(f"Local job {job_id} finished synchronously.")