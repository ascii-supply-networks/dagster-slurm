# runs inside the Slurm allocation
import os
import pathlib
import tempfile
import textwrap
import time

from dagster_pipes import PipesDefaultMessageWriter
from ray.job_submission import JobStatus, JobSubmissionClient


# TODO: fix this example normally a pipes session should look like
# https://github.com/ascii-supply-networks/dagster-slurm/blob/main/examples/projects/dagster-slurm-example-hpc-workload/dagster_slurm_example_hpc_workload/ray/ray_external.py
# and also we would not want to generate the file on the fly but want to use a job from VCS
def main():  # noqa: C901
    with PipesDefaultMessageWriter.from_env() as writer:
        addr = os.environ.get("RAY_DASHBOARD_ADDR", "http://127.0.0.1:8265")
        writer.report_log(f"[slurm→ray] Dashboard: {addr}")
        client = JobSubmissionClient(address=addr)

        # write a tiny Ray entrypoint into a temp working_dir so Ray can upload it
        work = pathlib.Path(tempfile.mkdtemp(prefix="ray_job_"))
        entry = work / "ray_payload.py"
        entry.write_text(
            textwrap.dedent("""
import ray

ray.init()

@ray.remote
def my_function(x):
    return x * 2

futures = [my_function.remote(i) for i in range(4)]
print(ray.get(futures))
        """).strip()
            + "\n"
        )

        job_id = client.submit_job(
            entrypoint=f"python {entry.name}",
            runtime_env={"working_dir": str(work)},
        )
        writer.report_log(f"[slurm→ray] Submitted Ray job: {job_id}")

        last = None
        while True:
            st = client.get_job_status(job_id)
            if st != last:
                writer.report_log(f"[slurm→ray] Ray job state: {st}")
                last = st
            if st in {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.STOPPED}:
                break
            # (optional) you can fetch full logs and tail them; here we just sleep a bit
            time.sleep(1.5)

        logs = client.get_job_logs(job_id) or ""
        for line in logs.splitlines()[-200:]:
            writer.report_log(f"[ray logs] {line}")

        if st != JobStatus.SUCCEEDED:
            raise SystemExit(1)

        writer.report_log("[slurm→ray] Ray job succeeded")


if __name__ == "__main__":
    main()
