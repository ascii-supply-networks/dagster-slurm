import re
import shlex
import time
from . import ssh_helpers

class DockerRunner:
    """Executes commands and manages files on a remote Slurm cluster via SSH."""

    def __init__(self, logger):
        self.logger = logger
        self.last_job_id = None
        self.last_remote_run_dir = None

    def run_command(self, cmd: str, is_check: bool = True) -> str:
        return ssh_helpers.ssh_check(cmd) if is_check else ssh_helpers.ssh_run(cmd)[0]

    def put_file(self, local_path: str, remote_path: str) -> None:
        ssh_helpers.scp_put(local_path, remote_path)

    def submit_job(self, job_script_path: str) -> int:
        self.logger.info(f"Submitting remote Slurm job: {job_script_path}")
        sbatch_cmd = f"sbatch {shlex.quote(job_script_path)}"
        out = self.run_command(sbatch_cmd)
        
        m = re.search(r"Submitted batch job (\d+)", out)
        if not m:
            raise RuntimeError(f"Could not parse job id from sbatch output:\n{out}")
        
        self.last_job_id = int(m.group(1))
        return self.last_job_id

    def wait_for_job(self, job_id: int):
        self.logger.info(f"Polling Slurm job {job_id} for completion...")
        while True:
            state = ssh_helpers.ssh_job_state(job_id)
            if not state:
                self.logger.warning("Slurm job state not found, assuming completion.")
                break
            if state in ssh_helpers.TERMINAL_STATES:
                self.logger.info(f"Job {job_id} finished with state: {state}")
                if state != "COMPLETED":
                     raise RuntimeError(f"Slurm job {job_id} failed with state: {state}")
                break
            time.sleep(5)