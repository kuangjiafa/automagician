import os
import shutil
import pytest
from automagician.update_job import log_error

def test_log_error_no_prev_errors_1_error(tmp_path):
    job_path = os.path.join(tmp_path, "job_path")
    home_path = os.path.join(tmp_path, "fake_home")
    os.mkdir(job_path)
    os.mkdir(home_path)
    shutil.copy("test/test_files/failed_u_run/ll_out", job_path)
    log_error(job_path, home_path)
    assert os.path.exists(os.path.join(home_path, "error_log.dat"))
    with open(os.path.join(home_path, "error_log.dat")) as error_log:
        log_file = error_log.read()
        assert f"{job_path}" in log_file
        assert "ZBRENT: fatal error in bracketing" in log_file

def test_log_error_prev_errors(tmp_path):
    job_path = os.path.join(tmp_path, "job_path")
    home_path = os.path.join(tmp_path, "fake_home")
    os.mkdir(job_path)
    os.mkdir(home_path)
    shutil.copy("test/test_files/failed_u_run/ll_out", job_path)
    log_error(job_path, home_path)
    log_error(job_path, home_path)
    assert os.path.exists(os.path.join(home_path, "error_log.dat"))
    with open(os.path.join(home_path, "error_log.dat")) as error_log:
        log_file = error_log.readline()
        assert f"{job_path}" in log_file
        assert "ZBRENT: fatal error in bracketing" in log_file
        log_file = error_log.readline()
        assert f"{job_path}" in log_file
        assert "ZBRENT: fatal error in bracketing" in log_file

def test_log_error_no_errors(tmp_path):
    job_path = os.path.join(tmp_path, "job_path")
    home_path = os.path.join(tmp_path, "fake_home")
    os.mkdir(job_path)
    os.mkdir(home_path)
    # Using h2/ll_out which has no errors
    shutil.copy("test/test_files/h2/ll_out", job_path)

    log_error(job_path, home_path)

    # error_log.dat should be created (because of open(..., "a+")) but empty
    assert os.path.exists(os.path.join(home_path, "error_log.dat"))
    with open(os.path.join(home_path, "error_log.dat")) as error_log:
        log_file = error_log.read()
        assert log_file == ""
