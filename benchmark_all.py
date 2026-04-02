import time
import os
from src.automagician.database import Database
from src.automagician.classes import DosJob, WavJob, OptJob, JobStatus, Machine

def benchmark():
    if os.path.exists("test_db_all.sqlite"):
        os.remove("test_db_all.sqlite")

    db = Database("test_db_all.sqlite")

    opt_jobs = {}
    dos_jobs = {}
    wav_jobs = {}

    N = 10000
    print(f"Setting up data for {N} jobs...")
    for i in range(N):
        dir_name = f"/tmp/opt_{i}"
        opt_jobs[dir_name] = OptJob(JobStatus.CONVERGED, Machine.FRI, Machine.FRI)

        dos_dir = f"/tmp/opt_{i}/dos"
        dos_jobs[dos_dir] = DosJob(opt_id=-1, sc_status=JobStatus.CONVERGED, dos_status=JobStatus.CONVERGED, sc_last_on=Machine.FRI, dos_last_on=Machine.FRI)

        wav_dir = f"/tmp/opt_{i}/wav"
        wav_jobs[wav_dir] = WavJob(opt_id=-1, wav_status=JobStatus.CONVERGED, wav_last_on=Machine.FRI)

    start = time.time()
    db.write_job_statuses(opt_jobs, dos_jobs, wav_jobs)
    end = time.time()

    print(f"Time taken (baseline): {end - start:.4f} seconds")

if __name__ == "__main__":
    benchmark()
