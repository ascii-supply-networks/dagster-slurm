"""Join-and-drain monitor for Ray nodes contributed by a Slurm worker allocation.

The session uploads this file next to its persistent ``ray_worker.sh``. On each
node of a worker allocation, the worker script runs it beside
``ray start --block``. The monitor publishes the node's Ray ID, then waits for
the allocation's drain request. On a request it drains the node through the
GCS: Ray stops placing new work there, lets running work finish, and the
raylet exits once the node is idle. The monitor returns when the raylet has
exited or the drain deadline has passed; the worker script then stops Ray.

The monitor also returns, without a drain, when the session's head pointer
names another head allocation, so the worker allocation can join it.

The remote environment might not have ``dagster_slurm`` installed, so this
module uses only the standard library and a lazily imported ``ray``.
"""

from __future__ import annotations

import glob
from importlib import import_module
import os
import socket
import subprocess
import sys
import time
from typing import Callable, Optional

DRAIN_REASON = "DRAIN_NODE_REASON_PREEMPTION"
POLL_SECONDS = 2.0
MAX_CONNECT_DELAY_SECONDS = 30.0
REGISTRATION_TIMEOUT_SECONDS = 300.0


def read_head_job(path: str) -> Optional[str]:
    """Return the job ID in the session's head pointer, if it names one."""
    try:
        with open(path, encoding="utf-8") as handle:
            value = handle.read().strip()
    except OSError:
        return None
    return value if value.isdigit() else None


def read_drain_seconds(path: str) -> Optional[int]:
    """Return the requested drain window, or None while no drain is requested.

    The allocation pre-creates the file empty and writes the window in place,
    so polling reopens an existing file and sees updates promptly on NFS.
    """
    try:
        with open(path, encoding="utf-8") as handle:
            value = handle.read().strip()
    except OSError:
        return None
    return int(value) if value.isdigit() else None


def child_pids(parent_pid: int, name: str) -> list[int]:
    """Return the PIDs of ``parent_pid``'s children whose command name is ``name``."""
    pids = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat", encoding="utf-8") as handle:
                stat = handle.read()
        except OSError:
            continue
        # The command name is parenthesized and can contain spaces.
        command = stat[stat.find("(") + 1 : stat.rfind(")")]
        fields = stat[stat.rfind(")") + 2 :].split()
        if command == name and len(fields) > 1 and int(fields[1]) == parent_pid:
            pids.append(int(entry))
    return pids


def process_alive(pid: int) -> bool:
    """Whether ``pid`` runs; an exited child its parent has not reaped does not."""
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as handle:
            stat = handle.read()
    except OSError:
        return False
    return stat[stat.rfind(")") + 2 :].split()[:1] != ["Z"]


def wait_unless(stop: Callable[[], bool], seconds: float) -> None:
    """Sleep for ``seconds``, returning early once ``stop()`` is true."""
    deadline = time.monotonic() + seconds
    while not stop() and (remaining := deadline - time.monotonic()) > 0:
        time.sleep(min(0.5, remaining))


def matching_node_ids(
    nodes: list[dict], temp_dir: str, node_ip: str, host: str
) -> list[str]:
    """IDs of live Ray nodes whose raylet runs from ``temp_dir`` on this machine.

    Nodes of one allocation can use the same temp dir path on different
    machines, so the socket path alone does not identify this node.
    """
    prefix = temp_dir.rstrip("/") + "/"
    matches = []
    for node in nodes:
        socket_name = str(node.get("RayletSocketName") or "")
        if not node.get("Alive") or not socket_name.startswith(prefix):
            continue
        if node_ip:
            if node.get("NodeManagerAddress") != node_ip:
                continue
        elif node.get("NodeManagerHostname") not in (None, host):
            continue
        matches.append(str(node["NodeID"]))
    return matches


def resolve_node_id(
    address: str,
    temp_dir: str,
    node_ip: str,
    ray_start_pid: int,
    *,
    stop: Callable[[], bool] = lambda: False,
    timeout: float = REGISTRATION_TIMEOUT_SECONDS,
) -> Optional[str]:
    """Look up this node's Ray ID in the GCS node table.

    A short-lived driver connects only once the local raylet has created its
    socket, and waits longer after each attempt, so a join normally costs one
    connection.
    """
    ray = import_module("ray")
    host = socket.gethostname()
    raylet_sockets = os.path.join(temp_dir, "session_*", "sockets", "raylet")
    kwargs = {"address": address, "logging_level": "ERROR", "log_to_driver": False}
    if node_ip:
        kwargs["_node_ip_address"] = node_ip
    deadline = time.monotonic() + timeout
    delay = POLL_SECONDS
    while time.monotonic() < deadline and process_alive(ray_start_pid) and not stop():
        if not glob.glob(raylet_sockets):
            wait_unless(stop, POLL_SECONDS)
            continue
        try:
            ray.init(**kwargs)
            try:
                matches = matching_node_ids(ray.nodes(), temp_dir, node_ip, host)
            finally:
                ray.shutdown()
        except Exception as exc:  # noqa: BLE001 - registration races are expected
            print(f"Waiting for the local Ray node to register: {exc}", flush=True)
            matches = []
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            print(
                f"WARNING: {len(matches)} Ray nodes match this node; not publishing an ID",
                flush=True,
            )
            return None
        # A drain request or a head move must not wait out the backoff.
        wait_unless(stop, delay)
        delay = min(delay * 2, MAX_CONNECT_DELAY_SECONDS)
    return None


def request_node_drain(address: str, node_id: str, seconds: int) -> None:
    """Ask the GCS to stop scheduling on this node and to expect it to leave."""
    command = [
        "ray",
        "drain-node",
        f"--address={address}",
        f"--node-id={node_id}",
        f"--reason={DRAIN_REASON}",
        "--reason-message=Slurm worker allocation "
        f"{os.environ.get('SLURM_JOB_ID', '')} is draining",
        f"--deadline-remaining-seconds={seconds}",
    ]
    result = subprocess.run(
        command, capture_output=True, text=True, timeout=120, check=False
    )
    if result.returncode != 0:
        print(
            f"WARNING: ray drain-node failed ({result.returncode}): "
            f"{result.stderr.strip() or result.stdout.strip()}",
            flush=True,
        )


def monitor(
    *,
    address: str,
    temp_dir: str,
    node_dir: str,
    ray_start_pid: int,
    node_ip: str = "",
    head_pointer: str = "",
    head_job: str = "",
    resolve: Callable[..., Optional[str]] = resolve_node_id,
    drain: Callable[[str, str, int], None] = request_node_drain,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Publish the node ID, then drain or leave when the allocation asks."""
    drain_path = os.path.join(node_dir, "drain")
    # Match the names in the allocation's nodes.txt.
    host = os.environ.get("SLURMD_NODENAME") or socket.gethostname()
    node_file = os.path.join(node_dir, f"{host}.id")

    def head_moved() -> bool:
        current = read_head_job(head_pointer) if head_pointer else None
        return bool(head_job and current and current != head_job)

    node_id = resolve(
        address,
        temp_dir,
        node_ip,
        ray_start_pid,
        stop=lambda: read_drain_seconds(drain_path) is not None or head_moved(),
    )
    if node_id:
        tmp_file = f"{node_file}.tmp.{os.getpid()}"
        with open(tmp_file, "w", encoding="utf-8") as handle:
            handle.write(f"{node_id} {head_job}\n")
        os.replace(tmp_file, node_file)
        print(f"Ray node {node_id} joined the run's cluster", flush=True)
    elif process_alive(ray_start_pid):
        print("WARNING: could not resolve this node's Ray ID", flush=True)

    try:
        while process_alive(ray_start_pid):
            if head_moved():
                print("The run's Ray head moved; leaving to join it", flush=True)
                return 0
            seconds = read_drain_seconds(drain_path)
            if seconds is None:
                sleep(POLL_SECONDS)
                continue
            raylets = child_pids(ray_start_pid, "raylet")
            print(f"Draining Ray node for up to {seconds}s", flush=True)
            if node_id and raylets:
                drain(address, node_id, seconds)
                deadline = time.monotonic() + seconds
                while time.monotonic() < deadline and any(map(process_alive, raylets)):
                    sleep(1.0)
            if any(map(process_alive, raylets)):
                print("Drain deadline reached; stopping Ray with work left", flush=True)
            else:
                print("Ray node drained", flush=True)
            return 0
        return 0
    finally:
        # This node no longer serves the head it published.
        if node_id:
            try:
                os.remove(node_file)
            except OSError:
                pass


def main(argv: list[str]) -> int:
    address, temp_dir, node_dir, ray_start_pid, node_ip = argv
    return monitor(
        address=address,
        temp_dir=temp_dir,
        node_dir=node_dir,
        ray_start_pid=int(ray_start_pid),
        node_ip=node_ip,
        head_pointer=os.environ.get("DAGSTER_SLURM_RAY_HEAD_POINTER", ""),
        head_job=os.environ.get("DAGSTER_SLURM_RAY_HEAD_JOB", ""),
    )


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
