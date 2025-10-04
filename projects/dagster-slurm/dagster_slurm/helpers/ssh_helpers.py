"""SSH utility functions."""

TERMINAL_STATES = {
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "CANCELLED+",
    "TIMEOUT",
    "PREEMPTED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "BOOT_FAIL",
    "DEADLINE",
    "REVOKED",
}


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
