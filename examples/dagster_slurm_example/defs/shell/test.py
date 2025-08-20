import os, json, socket
from pathlib import Path
from datetime import datetime

def main():
    out_dir = Path(os.environ.get("JOB_OUTPUT_DIR", "/data/results"))
    out_dir.mkdir(parents=True, exist_ok=True)
    base = f"hello_{os.environ.get('SLURM_JOB_ID','nojob')}_0"
    (out_dir / f"{base}.json").write_text(json.dumps({
        "ts": datetime.now().isoformat(timespec="seconds"),
        "host": socket.gethostname(),
    }, indent=2))
    (out_dir / f"{base}.txt").write_text("ok\n")

if __name__ == "__main__":
    main() 