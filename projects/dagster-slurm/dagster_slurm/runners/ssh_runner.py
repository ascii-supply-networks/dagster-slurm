# dagster_slurm/runners/ssh_runner.py
"""SSH/Slurm execution runner."""

import re
import shlex
import time
from typing import List

from dagster import get_dagster_logger

from ..helpers.ssh_helpers import (
    TERMINAL_STATES,
    scp_put,
    ssh_check,
    ssh_job_state,
    ssh_mkdir,
    ssh_write_file,
)
from ..resources.ssh import SSHConnectionResource
from .base import Runner


class SSHRunner(Runner):
    """Executes scripts on remote Slurm cluster via SSH.
    Used for staging/prod modes.
    """

    def __init__(self, ssh_resource: "SSHConnectionResource"):
        self.ssh_resource = ssh_resource
        self.logger = get_dagster_logger()
        self._last_job_id: int = 0

    def execute_script(
        self,
        script_lines: List[str],
        working_dir: str,
        wait: bool = True,
    ) -> int:
        """Execute script on remote host via sbatch."""
        # Ensure remote working dir exists
        self.create_directory(working_dir)
        # Write script to remote
        script_path = f"{working_dir}/launch.sh"
        script_content = "\n".join(script_lines)
        ssh_write_file(script_content, script_path, self.ssh_resource)
        # Make executable
        ssh_check(f"chmod +x {shlex.quote(script_path)}", self.ssh_resource)
        self.logger.info(f"Submitting to Slurm: {script_path}")
        # Submit via sbatch
        submit_cmd = f"sbatch {shlex.quote(script_path)}"
        output = ssh_check(submit_cmd, self.ssh_resource)
        # Parse job ID
        match = re.search(r"Submitted batch job (\d+)", output)
        if not match:
            raise RuntimeError(f"Could not parse job ID from sbatch output:\n{output}")
        job_id = int(match.group(1))
        self._last_job_id = job_id
        self.logger.info(f"Slurm job submitted: {job_id}")
        if wait:
            self.wait_for_completion(job_id)
        return job_id

    def upload_file(self, local_path: str, remote_path: str) -> None:
        """Upload file via SCP."""
        # Ensure remote parent dir exists
        remote_dir = "/".join(remote_path.rsplit("/", 1)[:-1])
        if remote_dir:
            self.create_directory(remote_dir)
        scp_put(local_path, remote_path, self.ssh_resource)
        self.logger.debug(f"Uploaded: {local_path} -> {remote_path}")

    def create_directory(self, path: str) -> None:
        """Create remote directory."""
        ssh_mkdir(path, self.ssh_resource)

    def wait_for_completion(self, job_id: int) -> None:
        """Poll Slurm until job completes."""
        self.logger.info(f"Waiting for job {job_id} to complete...")
        while True:
            state = ssh_job_state(job_id, self.ssh_resource)
            if not state:
                self.logger.warning(f"Job {job_id} state unknown, assuming completed")
                break
            if state in TERMINAL_STATES:
                self.logger.info(f"Job {job_id} finished with state: {state}")
                if state != "COMPLETED":
                    raise RuntimeError(f"Slurm job {job_id} failed with state: {state}")
                break
            self.logger.debug(f"Job {job_id} state: {state}")
            time.sleep(5)


# dagster_slurm/runners/__init__.py
"""Execution runners."""
from .base import Runner
from .local_runner import LocalRunner
from .ssh_runner import SSHRunner

__all__ = ["Runner", "LocalRunner", "SSHRunner"]
