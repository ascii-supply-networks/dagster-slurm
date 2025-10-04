#!/usr/bin/env python3
import json
import os
import socket
import time
from datetime import datetime
from pathlib import Path

OUT_DIR = Path(os.environ.get("JOB_OUTPUT_DIR", "/data/results"))
LOG_DIR = Path(os.environ.get("JOB_LOG_DIR", "/data/logs"))
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

job_id     = os.environ.get("SLURM_JOB_ID", "nojob")
job_name   = os.environ.get("SLURM_JOB_NAME", "hello_py")
procid     = os.environ.get("SLURM_PROCID", "0")
nodelist   = os.environ.get("SLURM_NODELIST", socket.gethostname())

payload = {
    "timestamp": datetime.now().isoformat(timespec="seconds"),
    "host": socket.gethostname(),
    "cwd": os.getcwd(),
    "job_id": job_id,
    "job_name": job_name,
    "procid": procid,
    "nodelist": nodelist,
    "env_excerpt": {k: os.environ[k] for k in (
        "SLURM_JOB_ID", "SLURM_JOB_NAME", "SLURM_NODELIST",
        "SLURM_CPUS_ON_NODE", "SLURM_NTASKS", "SLURM_CPUS_PER_TASK"
    ) if k in os.environ}
}

json_path = OUT_DIR / f"{job_name}_{job_id}_{procid}.json"
txt_path  = OUT_DIR / f"{job_name}_{job_id}_{procid}.txt"
json_path.write_text(json.dumps(payload, indent=2))
txt_path.write_text(f"{payload['timestamp']} hello from {payload['host']} "
                    f"(job {job_id}, task {procid})\n")

print(f"Wrote: {json_path}")
print(f"Wrote: {txt_path}")

time.sleep(2)

try:
    listing = sorted(os.listdir("/data"))[:20]
    print("Listing of /data:", ", ".join(listing))
except Exception as e:
    print("Could not list /data:", e)