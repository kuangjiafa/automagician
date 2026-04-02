import time
import os
import shutil
from src.automagician.database import Database
from src.automagician.classes import WavJob, JobStatus, Machine, OptJob

def benchmark():
    if os.path.exists("test_db.sqlite"):
        os.remove("test_db.sqlite")

    db = Database("test_db.sqlite")

    # Create 1000 opt jobs
    opt_jobs = {}
    wav_jobs = {}

    print("Setting up data...")
    for i in range(1000):
        dir_name = f"/tmp/opt_{i}"
        opt_jobs[dir_name] = OptJob(JobStatus.CONVERGED, Machine.FRI, Machine.FRI)

        wav_dir = f"/tmp/opt_{i}/wav"
        wav_jobs[wav_dir] = WavJob(opt_id=-1, wav_status=JobStatus.CONVERGED, wav_last_on=Machine.FRI)

    db.write_job_statuses(opt_jobs, {}, {})

    print("Benchmarking wav_jobs writes...")
    start = time.time()
    db.write_job_statuses({}, {}, wav_jobs)
    end = time.time()

    print(f"Time taken: {end - start:.4f} seconds")

if __name__ == "__main__":
    benchmark()
