"""Abstract runner interface."""

from abc import ABC, abstractmethod
from typing import List


class Runner(ABC):
    """Abstract base for execution runners.

    Runners handle the "how" of execution:
    - LocalRunner: subprocess on local machine
    - SSHRunner: SSH to remote machine (not needed with SSHConnectionPool)

    Key principle: Runners handle all I/O, launchers are pure.
    """

    @abstractmethod
    def execute_script(
        self,
        script_lines: List[str],
        working_dir: str,
        wait: bool = True,
    ) -> int:
        """Execute a shell script.

        Args:
            script_lines: Bash script lines (including shebang)
            working_dir: Directory to execute in
            wait: If True, block until completion

        Returns:
            Job/process ID

        """
        pass

    @abstractmethod
    def upload_file(self, local_path: str, remote_path: str) -> None:
        """Upload file to execution environment.

        For LocalRunner: copy within filesystem
        For remote: handled by SSHConnectionPool
        """
        pass

    @abstractmethod
    def create_directory(self, path: str) -> None:
        """Create directory (with parents) in execution environment."""
        pass

    @abstractmethod
    def wait_for_completion(self, job_id: int) -> None:
        """Block until job completes."""
        pass
