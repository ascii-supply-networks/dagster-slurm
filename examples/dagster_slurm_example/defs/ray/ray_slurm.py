# runs inside the Slurm allocation
import os, time, tempfile, textwrap, pathlib
from dagster_pipes import PipesDefaultMessageWriter
from ray.job_submission import JobSubmissionClient, JobStatus

def main():
    with PipesDefaultMessageWriter.from_env() as writer:
        addr = os.environ.get("RAY_DASHBOARD_ADDR", "http://127.0.0.1:8265")
        writer.report_log(f"[slurm→ray] Dashboard: {addr}")
        client = JobSubmissionClient(address=addr)

        # write a tiny Ray entrypoint into a temp working_dir so Ray can upload it
        work = pathlib.Path(tempfile.mkdtemp(prefix="ray_job_"))
        entry = work / "ray_payload.py"
        entry.write_text(textwrap.dedent("""
            import ray, time
            ray.init(address="auto", namespace="dagster")
            @ray.remote
            def divide(x):
                if x == 0:
                    raise ValueError("division by zero")
                return 10 / x

            futs = [divide.remote(i) for i in [1,2,0,3]]
            ok = bad = 0
            while futs:
                ready, futs = ray.wait(futs, num_returns=1)
                for f in ready:
                    try:
                        r = ray.get(f)
                        print(f"[ray payload] result={r}", flush=True)
                        ok += 1
                    except Exception as e:
                        print(f"[ray payload] error: {e}", flush=True)
                        bad += 1
            print(f"[ray payload] done; ok={ok} bad={bad}", flush=True)
        """).strip() + "\n")

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
