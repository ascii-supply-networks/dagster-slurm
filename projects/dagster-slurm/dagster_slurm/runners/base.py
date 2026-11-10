"""Abstract runner interface."""

from abc import ABC, abstractmethod
from collections.abc import Callable


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
        script_lines: list[str],
        working_dir: str,
        wait: bool = True,
        line_callback: Callable[[str], object] | None = None,
    ) -> int:
        """Execute a shell script.

        Args:
            script_lines: Bash script lines (including shebang)
            working_dir: Directory to execute in
            wait: If True, block until completion
            line_callback: Called as each stdout or stderr line is observed

        Returns:
            Job/process ID

        """

    @abstractmethod
    def upload_file(self, local_path: str, remote_path: str) -> None:
        """Upload file to execution environment.

        For LocalRunner: copy within filesystem
        For remote: handled by SSHConnectionPool
        """

    @abstractmethod
    def create_directory(self, path: str) -> None:
        """Create directory (with parents) in execution environment."""

    @abstractmethod
    def wait_for_completion(self, job_id: int) -> None:
        """Block until job completes."""
