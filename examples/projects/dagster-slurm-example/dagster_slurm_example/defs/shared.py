from pathlib import Path
import os

foo = "bar"
example_defs_prefix = "dse"
REMOTE_BASE         = os.environ.get("SLURM_REMOTE_BASE", "/home/submitter").rstrip("/")
ACTIVATE_SH         = os.environ.get("SLURM_ACTIVATE_SH", "/home/submitter/activate.sh")
REMOTE_PY           = os.environ.get("SLURM_PYTHON", "python")
PARTITION           = os.environ.get("SLURM_PARTITION", "")
TIME_LIMIT          = os.environ.get("SLURM_TIME", "00:10:00")
CPUS                = os.environ.get("SLURM_CPUS", "1")
MEM                 = os.environ.get("SLURM_MEM", "256M")
MEM_PER_CPU   = os.environ.get("SLURM_MEM_PER_CPU", "256M")

LOCAL_PAYLOAD = os.environ.get(
    "LOCAL_EXTERNAL_FILE",
    str(
        (Path(__file__).resolve().parents[3]
         / "dagster_slurm_example" / "defs" / "shell" / "external_file.py"
        ).resolve()
    ),
)
