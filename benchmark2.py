import time
import os
from automagician.database import Database
from automagician.classes import OptJob, DosJob, WavJob, Machine, JobStatus

def run_benchmark():
    db_path = "test_benchmark2.db"
    if os.path.exists(db_path):
        os.remove(db_path)

    db = Database(db_path)

    # Generate 5000 opt jobs
    opt_jobs = {}
    dos_jobs = {}
    wav_jobs = {}

    for i in range(5000):
        dir_name = f"/test/job_{i}"
        opt_jobs[dir_name] = OptJob(status=JobStatus.CONVERGED, home_machine=Machine.FRI, last_on=Machine.FRI)
        dos_jobs[f"{dir_name}/dos"] = DosJob(opt_id=-1, sc_status=JobStatus.CONVERGED, dos_status=JobStatus.CONVERGED, sc_last_on=Machine.FRI, dos_last_on=Machine.FRI)
        wav_jobs[f"{dir_name}/wav"] = WavJob(opt_id=-1, wav_status=JobStatus.CONVERGED, wav_last_on=Machine.FRI)

    start_time = time.time()
    db.write_job_statuses(opt_jobs, dos_jobs, wav_jobs)
    end_time = time.time()

    print(f"Initial insert time for 5000: {end_time - start_time:.4f} seconds")

    if os.path.exists(db_path):
        os.remove(db_path)

if __name__ == "__main__":
    run_benchmark()
