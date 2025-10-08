"""Local execution runner."""

import os
import shutil
import subprocess
from pathlib import Path
from typing import List

from dagster import get_dagster_logger

from .base import Runner


class LocalRunner(Runner):
    """Executes scripts locally via subprocess.
    Used for dev mode - no SSH, no Slurm.
    """

    def __init__(self):
        self.logger = get_dagster_logger()
        self._last_job_id: int = os.getpid()

    def execute_script(
        self,
        script_lines: List[str],
        working_dir: str,
        wait: bool = True,
    ) -> int:
        """Execute shell script locally.

        Args:
            script_lines: Bash script lines (including shebang)
            working_dir: Directory to execute in
            wait: Block until completion

        Returns:
            Process ID

        """
        # Ensure working dir exists
        Path(working_dir).mkdir(parents=True, exist_ok=True)

        # Write script
        script_path = Path(working_dir) / "launch.sh"
        script_path.write_text("\n".join(script_lines))
        script_path.chmod(0o755)

        self.logger.info(f"Executing local script: {script_path}")

        if wait:
            # Run synchronously
            try:
                result = subprocess.run(
                    ["bash", str(script_path)],
                    cwd=working_dir,
                    capture_output=True,
                    text=True,
                    check=True,
                )
                if result.stdout:
                    self.logger.debug(f"stdout:\n{result.stdout}")
                if result.stderr:
                    self.logger.debug(f"stderr:\n{result.stderr}")
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Script failed (exit {e.returncode})")
                if e.stdout:
                    self.logger.error(f"stdout:\n{e.stdout}")
                if e.stderr:
                    self.logger.error(f"stderr:\n{e.stderr}")
                raise
        else:
            # Run asynchronously
            subprocess.Popen(
                ["bash", str(script_path)],
                cwd=working_dir,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

        self._last_job_id = os.getpid()
        return self._last_job_id

    def upload_file(self, local_path: str, remote_path: str) -> None:
        """Copy file locally."""
        remote_path_obj = Path(remote_path)
        remote_path_obj.parent.mkdir(parents=True, exist_ok=True)

        # Skip if same file
        if os.path.abspath(local_path) == os.path.abspath(remote_path):
            self.logger.debug(f"Source and dest identical, skipping: {local_path}")
            return

        shutil.copy2(local_path, remote_path)
        self.logger.debug(f"Copied: {local_path} -> {remote_path}")

    def create_directory(self, path: str) -> None:
        """Create local directory."""
        Path(path).mkdir(parents=True, exist_ok=True)
        self.logger.debug(f"Created directory: {path}")

    def wait_for_completion(self, job_id: int) -> None:
        """No-op for local runner (execute_script already blocks)."""
        pass
