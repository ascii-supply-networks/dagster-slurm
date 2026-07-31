"""Local execution runner."""

import os
import shutil
import subprocess
import sys
import threading
from collections.abc import Callable
from pathlib import Path
from typing import List, Optional, TextIO

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
        line_callback: Optional[Callable[[str], object]] = None,
    ) -> int:
        """Execute shell script locally.

        Args:
            script_lines: Bash script lines (including shebang)
            working_dir: Directory to execute in
            wait: Block until completion
            line_callback: Called as each stdout or stderr line is observed

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
            process = subprocess.Popen(
                ["bash", str(script_path)],
                cwd=working_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
            stdout_lines: list[str] = []
            stderr_lines: list[str] = []

            def forward_lines(
                source: TextIO,
                destination: TextIO,
                captured: list[str],
            ) -> None:
                for line in source:
                    captured.append(line)
                    if line_callback is not None:
                        line_callback(line.rstrip("\n"))
                    destination.write(line)
                    destination.flush()

            threads = [
                threading.Thread(
                    target=forward_lines,
                    args=(process.stdout, sys.stdout, stdout_lines),
                    daemon=True,
                ),
                threading.Thread(
                    target=forward_lines,
                    args=(process.stderr, sys.stderr, stderr_lines),
                    daemon=True,
                ),
            ]
            for thread in threads:
                thread.start()
            return_code = process.wait()
            for thread in threads:
                thread.join()
            if return_code:
                error = subprocess.CalledProcessError(
                    return_code,
                    ["bash", str(script_path)],
                    output="".join(stdout_lines),
                    stderr="".join(stderr_lines),
                )
                self.logger.error(f"Script failed (exit {error.returncode})")
                raise error
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
