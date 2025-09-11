import subprocess
import shlex
from pathlib import Path
import os
class LocalRunner:
    def __init__(self, logger):
        self.logger = logger
        self.last_job_id = None
        self.last_remote_run_dir = None

    def run_command(self, cmd: str, is_check: bool = True) -> str:
        self.logger.info(f"Running local command: {cmd}")
        try:
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
        except subprocess.CalledProcessError as e:
            # log everything before re-raising so Dagster shows the cause
            if e.stdout:
                self.logger.warning(f"Local stdout (on error):\n{e.stdout}")
            if e.stderr:
                self.logger.error(f"Local stderr (on error):\n{e.stderr}")
            raise

    def put_file(self, local_path: str, remote_path: str) -> None:
        self.logger.info(f"Copying '{local_path}' to '{remote_path}'")
        Path(remote_path).parent.mkdir(parents=True, exist_ok=True)
        if os.path.abspath(local_path) == os.path.abspath(remote_path):
            self.logger.debug("Source and destination are the same; skipping copy.")
            return
        subprocess.run(["cp", local_path, remote_path], check=True)

    def submit_job(self, job_script_path: str) -> int:
        self.logger.info(f"Submitting local job by running script: {job_script_path}")
        # Let run_command raise if the script fails; logs will include stderr.
        self.run_command(f"bash {shlex.quote(job_script_path)}")
        self.last_job_id = os.getpid()
        return self.last_job_id

    def wait_for_job(self, job_id: int):
        self.logger.info(f"Local job {job_id} finished synchronously.")
