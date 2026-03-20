import os
import pytest
from automagician.classes import GoneJob, JobStatus, Machine
from automagician.database import Database

def test_add_gone_jobs_to_db_bulk(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)

    jobs = [
        GoneJob("/tmp/job1", JobStatus.INCOMPLETE, Machine.FRI, Machine.FRI),
        GoneJob("/tmp/job2", JobStatus.ERROR, Machine.HALIFAX, Machine.HALIFAX),
    ]

    database.add_gone_jobs_to_db(jobs)

    gone_jobs = database.get_gone_jobs()
    assert len(gone_jobs) == 2
    assert gone_jobs["/tmp/job1"].status == JobStatus.INCOMPLETE
    assert gone_jobs["/tmp/job2"].status == JobStatus.ERROR

def test_add_gone_jobs_to_db_update_bulk(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)

    # Initial add
    database.add_gone_job_to_db(GoneJob("/tmp/job1", JobStatus.INCOMPLETE, Machine.FRI, Machine.FRI))

    # Bulk update
    jobs = [
        GoneJob("/tmp/job1", JobStatus.CONVERGED, Machine.FRI, Machine.FRI),
        GoneJob("/tmp/job2", JobStatus.ERROR, Machine.HALIFAX, Machine.HALIFAX),
    ]
    database.add_gone_jobs_to_db(jobs)

    gone_jobs = database.get_gone_jobs()
    assert len(gone_jobs) == 2
    assert gone_jobs["/tmp/job1"].status == JobStatus.CONVERGED
    assert gone_jobs["/tmp/job2"].status == JobStatus.ERROR
