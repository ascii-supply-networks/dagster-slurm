"""SSH utility functions."""

TERMINAL_STATES = {
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "TIMEOUT",
    "PREEMPTED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "BOOT_FAIL",
    "DEADLINE",
    "REVOKED",
}


def normalize_slurm_state(state: str) -> str:
    """Normalize Slurm states across squeue/sacct variants and truncation."""
    normalized = state.strip().upper()
    if not normalized:
        return ""

    if normalized.startswith("COMPLET"):
        return "COMPLETED"
    if normalized.startswith("FAIL"):
        return "FAILED"
    if normalized.startswith("CANCELLED"):
        return "CANCELLED"
    if normalized.startswith("TIMEOUT"):
        return "TIMEOUT"
    if normalized.startswith("PREEMPT"):
        return "PREEMPTED"
    if normalized.startswith("NODE_FAIL"):
        return "NODE_FAIL"
    if normalized.startswith("OUT_OF_ME"):
        return "OUT_OF_MEMORY"
    if normalized.startswith("BOOT_FAIL"):
        return "BOOT_FAIL"
    if normalized.startswith("DEADLINE"):
        return "DEADLINE"
    if normalized.startswith("REVOKED"):
        return "REVOKED"

    return normalized


def ssh_run(cmd: str, ssh_resource) -> tuple[str, str, int]:
    """Run SSH command via connection pool, return (stdout, stderr, returncode).

    Note: This is a legacy function for compatibility.
    New code should use SSHConnectionPool directly.
    """
    import shlex
    import subprocess

    remote_cmd = f"bash --noprofile --norc -c {shlex.quote(cmd)}"

    ssh_cmd = [
        "ssh",
        "-p",
        str(ssh_resource.port),
        "-i",
        ssh_resource.key_path,
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "BatchMode=yes",
        "-o",
        "LogLevel=ERROR",
        f"{ssh_resource.user}@{ssh_resource.host}",
        remote_cmd,
    ]

    result = subprocess.run(ssh_cmd, capture_output=True, text=True)
    return result.stdout, result.stderr, result.returncode


def ssh_check(cmd: str, ssh_resource) -> str:
    """Run SSH command, raise on failure.

    Note: This is a legacy function for compatibility.
    New code should use SSHConnectionPool directly.
    """
    stdout, stderr, returncode = ssh_run(cmd, ssh_resource)

    if returncode != 0:
        raise RuntimeError(
            f"SSH command failed (exit {returncode}): {cmd}\n"
            f"stdout: {stdout}\n"
            f"stderr: {stderr}"
        )

    return stdout
