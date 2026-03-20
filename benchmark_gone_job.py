import os
import time
import sqlite3
import logging
from typing import Dict, List
from automagician.database import Database
from automagician.classes import OptJob, GoneJob, JobStatus, Machine
from automagician.process_job import gone_job_check

# Setup logging to avoid clutter
logging.basicConfig(level=logging.ERROR)

def setup_benchmark_db(db_path: str, num_jobs: int):
    if os.path.exists(db_path):
        os.remove(db_path)
    db = Database(db_path)

    # Add many jobs that "don't exist" on disk
    for i in range(num_jobs):
        job_dir = f"/tmp/non_existent_job_{i}"
        job = OptJob(status=JobStatus.INCOMPLETE, home_machine=Machine.FRI, last_on=Machine.FRI)
        db.add_opt_job_to_db(job, job_dir, commit=False)
    db.db.connection.commit()
    return db

def run_benchmark():
    db_path = "benchmark.db"
    num_jobs = 1000
    db = setup_benchmark_db(db_path, num_jobs)
    opt_jobs = db.get_opt_jobs()

    start_time = time.time()
    gone_job_check(db, opt_jobs)
    end_time = time.time()

    print(f"Time taken for {num_jobs} gone jobs: {end_time - start_time:.4f} seconds")

    if os.path.exists(db_path):
        os.remove(db_path)

if __name__ == "__main__":
    run_benchmark()
    # Run it a few times to get an average?
    # Actually once is enough to see the difference usually if it's N+1
