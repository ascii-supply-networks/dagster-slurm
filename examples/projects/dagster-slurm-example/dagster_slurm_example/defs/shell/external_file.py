import os
import random
import socket
import time
from pathlib import Path

from dagster_pipes import open_dagster_pipes

def main():
    with open_dagster_pipes() as pipes:
        pipes.log.info("hello from Slurm via Dagster Pipes")
        pipes.log.info(f"node={socket.gethostname()}")

        out_dir = Path(os.environ.get("JOB_OUTPUT_DIR", "/data/results"))
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "proof.txt").write_text("materialized via pipes\n")

        pipes.report_asset_materialization(
            metadata={"rows": {"raw_value": random.randint(0, 100), "type": "int"}}
        )

        for i in range(5):
            pipes.log.info(f"work step {i + 1}/5")
            time.sleep(0.3)
if __name__ == "__main__":
    main()
