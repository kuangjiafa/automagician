import sqlite3
import time
import os
from src.automagician.database import Database
from src.automagician.classes import WavJob, JobStatus, Machine, OptJob

def optimize_benchmark():
    if os.path.exists("test_db2.sqlite"):
        os.remove("test_db2.sqlite")

    db = Database("test_db2.sqlite")

    # Create 10000 opt jobs
    opt_jobs = {}
    wav_jobs = {}

    N = 10000
    print(f"Setting up data for {N} jobs...")
    for i in range(N):
        dir_name = f"/tmp/opt_{i}"
        opt_jobs[dir_name] = OptJob(JobStatus.CONVERGED, Machine.FRI, Machine.FRI)

        wav_dir = f"/tmp/opt_{i}/wav"
        wav_jobs[wav_dir] = WavJob(opt_id=-1, wav_status=JobStatus.CONVERGED, wav_last_on=Machine.FRI)

    db.write_job_statuses(opt_jobs, {}, {})

    print("Benchmarking bulk wav_jobs writes...")
    start = time.time()

    # Let's write the optimized version here directly for testing
    import src.automagician.small_functions as small_functions
    import logging
    logger = logging.getLogger()

    # Pre-fetch all opt_jobs to avoid querying DB for each one
    opt_dirs = [small_functions.get_opt_dir(job_dir) for job_dir in wav_jobs]

    # Use IN clause to fetch multiple opt ids at once
    # Chunking might be necessary for very large sets, SQLite limit is 999
    chunk_size = 900
    opt_dir_to_id = {}

    for i in range(0, len(opt_dirs), chunk_size):
        chunk = set(opt_dirs[i:i+chunk_size])
        placeholders = ','.join(['?'] * len(chunk))
        query = f"SELECT dir, rowid FROM opt_jobs WHERE dir IN ({placeholders})"
        cursor = db.db.execute(query, list(chunk))
        for row in cursor:
            opt_dir_to_id[row[0]] = row[1]

    to_insert = []
    to_update = []

    # Fetch existing wav_jobs
    existing_wav_ids = set()
    cursor = db.db.execute("SELECT opt_id FROM wav_jobs")
    for row in cursor:
        existing_wav_ids.add(row[0])

    for job_dir, job in wav_jobs.items():
        opt_dir = small_functions.get_opt_dir(job_dir)
        opt_id = opt_dir_to_id.get(opt_dir)

        if opt_id is None:
            logger.warning(f"no opt job at directory {opt_dir}. Expected as was adding a wav_job")
            continue

        job.opt_id = opt_id

        if opt_id in existing_wav_ids:
            to_update.append((job.wav_status.value, job.wav_last_on.value, opt_id))
        else:
            to_insert.append((opt_id, job.wav_status.value, job.wav_last_on.value))

    if to_insert:
        db.db.executemany("INSERT INTO wav_jobs VALUES (?, ?, ?)", to_insert)
    if to_update:
        db.db.executemany("UPDATE wav_jobs SET wav_status = ?, wav_last_on = ? WHERE opt_id = ?", to_update)

    db.db.connection.commit()

    end = time.time()

    print(f"Time taken: {end - start:.4f} seconds")

if __name__ == "__main__":
    optimize_benchmark()
