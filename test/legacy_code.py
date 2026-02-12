# pylint: disable=duplicate-code
import logging
import os
import re
import shutil
import subprocess
import time
import traceback
from typing import Dict, List, Literal, TextIO, Tuple

from automagician.classes import (
    DosJob,
    GoneJob,
    JobStatus,
    Machine,
    OptJob,
    SSHConfig,
    WavJob,
)
from automagician.database import Database

# Placeholder for legacy functions extracted from tests
import os
import pathlib
import shutil
import time

import pytest

from automagician.classes import DosJob, GoneJob, JobStatus, Machine, OptJob, SSHConfig
from automagician.database import Database
from automagician.machine import get_subfile
from automagician.process_job import (
    check_error,
    check_has_opt,
    classify_job_dir,
    determine_box_convergence,
    determine_convergence,
    gone_job_check,
    grep_ll_out_convergence,
    is_isif3,
    process_converged,
    process_dos,
    process_opt,
    process_unconverged,
)


def test_check_has_opt_no_files(tmp_path):
    subfile = get_subfile(0)
    has_opt_files = check_has_opt(tmp_path, subfile)
    assert has_opt_files is False


def test_check_has_opt_all_reqiured_files(tmp_path):
    subfile_name = get_subfile(0)
    poscar = open(os.path.join(tmp_path, "POSCAR"), "w")
    potcar = open(os.path.join(tmp_path, "POTCAR"), "w")
    incar = open(os.path.join(tmp_path, "INCAR"), "w")
    kpoints = open(os.path.join(tmp_path, "KPOINTS"), "w")
    subfile_file = open(os.path.join(tmp_path, "fri.sub"), "w")
    poscar.close()
    potcar.close()
    incar.close()
    kpoints.close()
    subfile_file.close()
    has_opt_files = check_has_opt(tmp_path, subfile_name)
    assert has_opt_files is True


def test_check_has_opt_wrong_subfile(tmp_path):
    subfile_name = get_subfile(1)
    poscar = open(os.path.join(tmp_path, "POSCAR"), "w")
    potcar = open(os.path.join(tmp_path, "POTCAR"), "w")
    incar = open(os.path.join(tmp_path, "INCAR"), "w")
    kpoints = open(os.path.join(tmp_path, "KPOINTS"), "w")
    subfile_file = open(os.path.join(tmp_path, "fri.sub"), "w")
    poscar.close()
    potcar.close()
    incar.close()
    kpoints.close()
    subfile_file.close()
    has_opt_files = check_has_opt(tmp_path, subfile_name)
    assert has_opt_files is False


def test_check_error_has_error(tmp_path):
    shutil.copy("test/test_files/failed_u_run/ll_out", os.path.join(tmp_path))
    error = check_error(tmp_path)
    assert error is True


def test_check_error_has_no_error(tmp_path):
    shutil.copy("test/test_files/h2/ll_out", tmp_path)
    error = check_error(tmp_path)
    assert error is False


def test_grep_ll_out_convergence_converged(tmp_path):
    shutil.copy("test/test_files/h2/ll_out", tmp_path)
    cwd = os.getcwd()
    grep_return_code = grep_ll_out_convergence(os.path.join(tmp_path, "ll_out"))
    assert cwd == os.getcwd()
    assert grep_return_code is True


def test_grep_ll_out_convergence_error(tmp_path):
    shutil.copy("test/test_files/failed_u_run/ll_out", tmp_path)
    cwd = os.getcwd()
    grep_return_code = grep_ll_out_convergence(os.path.join(tmp_path, "ll_out"))
    assert cwd == os.getcwd()
    assert grep_return_code is False


def test_is_isif3_non_isif3(tmp_path):
    shutil.copy("test/test_files/failed_u_run/INCAR", tmp_path)
    isif3_ret_val = is_isif3(tmp_path)
    assert isif3_ret_val is False


def test_is_isif3_isif3(tmp_path):
    shutil.copy("test/test_files/failed_u_run/INCAR", tmp_path)
    with open(os.path.join(tmp_path, "INCAR"), "a+") as f:
        f.write("ISIF = 3")
    isif3_ret_val = is_isif3(tmp_path)
    assert isif3_ret_val is True


def test_is_isif3_isif3_white_space(tmp_path):
    shutil.copy("test/test_files/failed_u_run/INCAR", tmp_path)
    with open(os.path.join(tmp_path, "INCAR"), "a+") as f:
        f.write("ISIF      =        3")
    isif3_ret_val = is_isif3(tmp_path)
    assert isif3_ret_val is True


def test_determine_box_convergence_empty_fe_dat(tmp_path):
    with open(os.path.join(tmp_path, "fe.dat"), "w+"):
        pass
    cwd = os.getcwd()
    converged = determine_box_convergence(tmp_path)
    assert cwd == os.getcwd()
    assert converged is False


def test_determine_box_convergence_1_line(tmp_path):
    with open(os.path.join(tmp_path, "fe.dat"), "w+") as f:
        f.write("A signular line, so this should be converged")
    cwd = os.getcwd()
    converged = determine_box_convergence(tmp_path)
    assert cwd == os.getcwd()
    assert converged is True


def test_determine_box_convergence_2_lines(tmp_path):
    with open(os.path.join(tmp_path, "fe.dat"), "w+") as f:
        f.write("A signular line, so this should be converged\n")
        f.write("A second line, so this should mark as not converged \n")
    cwd = os.getcwd()
    converged = determine_box_convergence(tmp_path)
    assert cwd == os.getcwd()
    assert converged is False


def test_process_converged(tmp_path):
    opt_jobs = {}
    mock_job = OptJob(JobStatus.INCOMPLETE, 0, 0)
    opt_jobs[tmp_path] = mock_job
    expected_opt_jobs = {tmp_path: OptJob(JobStatus.CONVERGED, 0, 0)}
    process_converged(tmp_path, opt_jobs)
    assert opt_jobs == expected_opt_jobs
    assert os.path.exists(os.path.join(tmp_path, "convergence_certificate"))


def test_process_unconverged_no_files(tmp_path):
    shutil.copy("test/test_files/h2_completed_run/fri.sub", tmp_path)
    opt_jobs = {}
    mock_job = OptJob(JobStatus.INCOMPLETE, 0, 0)
    opt_jobs[tmp_path] = mock_job
    expected_opt_jobs = {tmp_path: OptJob(JobStatus.INCOMPLETE, 0, 0)}
    sub_quene = []
    with open(os.path.join(tmp_path, "prelminary_results.txt"), "w") as f:
        process_unconverged(tmp_path, opt_jobs, False, 50, sub_quene, 0, False, f)
    assert opt_jobs == expected_opt_jobs
    assert not os.path.exists(os.path.join(tmp_path, "convergence_certificate"))
    assert sub_quene == [tmp_path]
    with open(os.path.join(tmp_path, "prelminary_results.txt"), "r") as f:
        file = f.read()
        assert file == ""
    assert not os.path.exists(os.path.join(tmp_path, "run0"))


def test_process_unconverged_NO_OUTCAR(tmp_path):
    job_path = os.path.join(tmp_path, "job_path")
    shutil.copytree("test/test_files/h2_completed_run", job_path)
    os.remove(os.path.join(job_path, "OUTCAR"))
    opt_jobs = {}
    mock_job = OptJob(JobStatus.INCOMPLETE, 0, 0)
    opt_jobs[job_path] = mock_job
    expected_opt_jobs = {job_path: OptJob(JobStatus.INCOMPLETE, 0, 0)}
    sub_quene = []
    with open(os.path.join(job_path, "prelminary_results.txt"), "w") as f:
        process_unconverged(job_path, opt_jobs, False, 50, sub_quene, 0, False, f)
    assert opt_jobs == expected_opt_jobs
    assert not os.path.exists(os.path.join(job_path, "convergence_certificate"))
    assert sub_quene == [job_path]
    with open(os.path.join(job_path, "prelminary_results.txt"), "r") as f:
        file = f.read()
        assert file == ""
    assert not os.path.exists(os.path.join(job_path, "run0"))


def test_process_unconverged_NO_CONTCAR(tmp_path):
    job_path = os.path.join(tmp_path, "job_path")
    shutil.copytree("test/test_files/h2_completed_run", job_path)
    os.remove(os.path.join(job_path, "CONTCAR"))
    opt_jobs = {}
    mock_job = OptJob(JobStatus.INCOMPLETE, 0, 0)
    opt_jobs[job_path] = mock_job
    expected_opt_jobs = {job_path: OptJob(JobStatus.INCOMPLETE, 0, 0)}
    sub_quene = []
    with open(os.path.join(job_path, "prelminary_results.txt"), "w") as f:
        process_unconverged(job_path, opt_jobs, False, 50, sub_quene, 0, False, f)
    assert opt_jobs == expected_opt_jobs
    assert not os.path.exists(os.path.join(job_path, "convergence_certificate"))
    assert sub_quene == [job_path]
    with open(os.path.join(job_path, "prelminary_results.txt"), "r") as f:
        file = f.read()
        assert file == ""
    assert not os.path.exists(os.path.join(job_path, "run0"))


def test_process_unconverged_files_present(tmp_path):
    job_path = os.path.join(tmp_path, "job_path")
    shutil.copytree("test/test_files/h2_completed_run", job_path)
    opt_jobs = {}
    mock_job = OptJob(JobStatus.INCOMPLETE, 0, 0)
    opt_jobs[job_path] = mock_job
    expected_opt_jobs = {job_path: OptJob(JobStatus.INCOMPLETE, 0, 0)}
    sub_quene = []
    with open(os.path.join(job_path, "prelminary_results.txt"), "w") as f:
        process_unconverged(job_path, opt_jobs, False, 50, sub_quene, 0, False, f)
    assert opt_jobs == expected_opt_jobs
    assert not os.path.exists(os.path.join(job_path, "convergence_certificate"))
    assert sub_quene == [job_path]
    with open(os.path.join(job_path, "prelminary_results.txt"), "r") as f:
        file = f.read()
        assert file == f"{job_path}\n     0     0.0     0.0\n"
    assert os.path.isdir(os.path.join(job_path, "run0"))


def test_process_opt_no_files(tmp_path):
    job_dir = os.path.join(tmp_path, "job_dir")
    home_dir = os.path.join(tmp_path, "home_dir")
    os.mkdir(job_dir)
    os.mkdir(home_dir)
    opt_jobs = {}
    config = SSHConfig("NoSSH")
    sub_quene = []
    with open(os.path.join(tmp_path, "prelminary_results.txt"), "w") as f:
        process_opt(
            job_directory=job_dir,
            machine=0,
            opt_jobs=opt_jobs,
            clear_certificate=False,
            home_dir=home_dir,
            ssh_config=config,
            preliminary_results=f,
            continue_past_limit=False,
            limit=50,
            sub_queue=sub_quene,
            hit_limit=False,
        )
    assert sub_quene == []
    assert opt_jobs == {}
    with open(os.path.join(tmp_path, "prelminary_results.txt"), "r") as f:
        assert f.read() == ""


def test_process_opt_completed_h2_running(tmp_path):
    job_dir = os.path.join(tmp_path, "job_dir")
    home_dir = os.path.join(tmp_path, "home_dir")
    shutil.copytree("test/test_files/h2_completed_run", job_dir)
    os.mkdir(home_dir)
    test_job = OptJob(JobStatus.RUNNING, 0, 0)
    opt_jobs = {job_dir: test_job}
    config = SSHConfig("NoSSH")
    sub_quene = []
    with open(os.path.join(tmp_path, "prelminary_results.txt"), "w") as f:
        process_opt(
            job_directory=job_dir,
            machine=0,
            opt_jobs=opt_jobs,
            clear_certificate=False,
            home_dir=home_dir,
            ssh_config=config,
            preliminary_results=f,
            continue_past_limit=False,
            limit=50,
            sub_queue=sub_quene,
            hit_limit=False,
        )
    assert sub_quene == []
    assert opt_jobs == {job_dir: OptJob(JobStatus.RUNNING, 0, 0)}
    with open(os.path.join(tmp_path, "prelminary_results.txt"), "r") as f:
        file_as_str = f.read()
        assert file_as_str == f"{job_dir}\n     0     0.0     0.0\n"


def test_process_opt_completed_h2_incomplete(tmp_path):
    job_dir = os.path.join(tmp_path, "job_dir")
    home_dir = os.path.join(tmp_path, "home_dir")
    shutil.copytree("test/test_files/h2_completed_run", job_dir)
    os.mkdir(home_dir)
    test_job = OptJob(JobStatus.INCOMPLETE, 0, 0)
    opt_jobs = {job_dir: test_job}
    config = SSHConfig("NoSSH")
    sub_quene = []
    with open(os.path.join(tmp_path, "prelminary_results.txt"), "w") as f:
        process_opt(
            job_directory=job_dir,
            machine=0,
            opt_jobs=opt_jobs,
            clear_certificate=False,
            home_dir=home_dir,
            ssh_config=config,
            preliminary_results=f,
            continue_past_limit=False,
            limit=50,
            sub_queue=sub_quene,
            hit_limit=False,
        )
    assert sub_quene == []
    completed_job = OptJob(JobStatus.CONVERGED, 0, 0)
    assert opt_jobs == {job_dir: completed_job}
    assert os.path.exists(os.path.join(job_dir, "convergence_certificate"))
    with open(os.path.join(tmp_path, "prelminary_results.txt"), "r") as f:
        file_as_str = f.read()
        assert file_as_str == ""


def test_process_opt_completed_h2_incomplete_remove_certificate(tmp_path):
    job_dir = os.path.join(tmp_path, "job_dir")
    home_dir = os.path.join(tmp_path, "home_dir")
    shutil.copytree("test/test_files/h2", job_dir)
    with open(os.path.join(job_dir, "convergence_certificate"), "w") as f:
        pass
    os.mkdir(home_dir)
    test_job = OptJob(JobStatus.INCOMPLETE, 0, 0)
    opt_jobs = {job_dir: test_job}
    config = SSHConfig("NoSSH")
    sub_quene = []
    with open(os.path.join(tmp_path, "prelminary_results.txt"), "w") as f:
        process_opt(
            job_directory=job_dir,
            machine=0,
            opt_jobs=opt_jobs,
            clear_certificate=True,
            home_dir=home_dir,
            ssh_config=config,
            preliminary_results=f,
            continue_past_limit=False,
            limit=50,
            sub_queue=sub_quene,
            hit_limit=False,
        )
    assert sub_quene == [job_dir]
    assert opt_jobs == {job_dir: OptJob(JobStatus.INCOMPLETE, 0, 0)}
    assert not os.path.exists(os.path.join(job_dir, "convergence_certificate"))
    with open(os.path.join(tmp_path, "prelminary_results.txt"), "r") as f:
        file_as_str = f.read()
        assert file_as_str == ""


def test_process_opt_completed_h2_incomplete_missing_entry(tmp_path):
    job_dir = os.path.join(tmp_path, "job_dir")
    home_dir = os.path.join(tmp_path, "home_dir")
    shutil.copytree("test/test_files/h2", job_dir)
    os.mkdir(home_dir)
    opt_jobs = {}
    config = SSHConfig("NoSSH")
    sub_quene = []
    with open(os.path.join(tmp_path, "prelminary_results.txt"), "w") as f:
        process_opt(
            job_directory=job_dir,
            machine=0,
            opt_jobs=opt_jobs,
            clear_certificate=False,
            home_dir=home_dir,
            ssh_config=config,
            preliminary_results=f,
            continue_past_limit=False,
            limit=50,
            sub_queue=sub_quene,
            hit_limit=False,
        )
    assert sub_quene == []
    assert opt_jobs == {}
    assert not os.path.exists(os.path.join(job_dir, "convergence_certificate"))
    with open(os.path.join(tmp_path, "prelminary_results.txt"), "r") as f:
        file_as_str = f.read()
        assert file_as_str == ""


def test_process_opt_completed_h2_incomplete_missing_ll_out(tmp_path):
    job_dir = os.path.join(tmp_path, "job_dir")
    home_dir = os.path.join(tmp_path, "home_dir")
    shutil.copytree("test/test_files/h2", job_dir)
    os.remove(os.path.join(job_dir, "ll_out"))
    os.mkdir(home_dir)
    test_job = OptJob(JobStatus.INCOMPLETE, 0, 0)
    opt_jobs = {job_dir: test_job}
    config = SSHConfig("NoSSH")
    sub_quene = []
    with open(os.path.join(tmp_path, "prelminary_results.txt"), "w") as f:
        process_opt(
            job_directory=job_dir,
            machine=0,
            opt_jobs=opt_jobs,
            clear_certificate=False,
            home_dir=home_dir,
            ssh_config=config,
            preliminary_results=f,
            continue_past_limit=False,
            limit=50,
            sub_queue=sub_quene,
            hit_limit=False,
        )
    assert sub_quene == [job_dir]
    assert opt_jobs == {job_dir: OptJob(JobStatus.INCOMPLETE, 0, 0)}
    assert not os.path.exists(os.path.join(job_dir, "convergence_certificate"))
    with open(os.path.join(tmp_path, "prelminary_results.txt"), "r") as f:
        file_as_str = f.read()
        assert file_as_str == ""


def test_process_opt_failed_u_job(tmp_path):
    job_dir = os.path.join(tmp_path, "job_dir")
    home_dir = os.path.join(tmp_path, "home_dir")
    shutil.copytree("test/test_files/failed_u_run", job_dir)
    os.mkdir(home_dir)
    test_job = OptJob(JobStatus.INCOMPLETE, 0, 0)
    opt_jobs = {job_dir: test_job}
    config = SSHConfig("NoSSH")
    sub_quene = []
    with open(os.path.join(tmp_path, "prelminary_results.txt"), "w") as f:
        process_opt(
            job_directory=job_dir,
            machine=0,
            opt_jobs=opt_jobs,
            clear_certificate=False,
            home_dir=home_dir,
            ssh_config=config,
            preliminary_results=f,
            continue_past_limit=False,
            limit=50,
            sub_queue=sub_quene,
            hit_limit=False,
        )
    assert sub_quene == [job_dir]
    assert opt_jobs == {job_dir: OptJob(JobStatus.INCOMPLETE, 0, 0)}
    assert not os.path.exists(os.path.join(job_dir, "convergence_certificate"))
    with open(os.path.join(tmp_path, "prelminary_results.txt"), "r") as f:
        file_as_str = f.read()
        assert file_as_str == ""


def test_determine_convergence_converged_h2(tmp_path):
    job_dir = os.path.join(tmp_path, "job_dir")
    shutil.copytree("test/test_files/h2_completed_run", job_dir)
    converged = determine_convergence(job_dir)
    assert converged is True


def test_determine_convergence_uconverged_convergence_certificate(tmp_path):
    job_dir = os.path.join(tmp_path, "job_dir")
    shutil.copytree("test/test_files/h2", job_dir)
    with open(os.path.join(job_dir, "convergence_certificate"), "w"):
        pass
    converged = determine_convergence(job_dir)
    assert converged is True


def test_determine_convergence_uconverged_no_contcar(tmp_path):
    job_dir = os.path.join(tmp_path, "job_dir")
    shutil.copytree("test/test_files/h2", job_dir)
    converged = determine_convergence(job_dir)
    assert converged is False


def test_determine_convergence_uconverged_contcar(tmp_path):
    job_dir = os.path.join(tmp_path, "job_dir")
    shutil.copytree("test/test_files/h2", job_dir)
    with open(os.path.join(job_dir, "ll_out"), "w"):
        pass
    with open(os.path.join(job_dir, "CONTCAR"), "w"):
        pass
    converged = determine_convergence(job_dir)
    assert converged is False


def test_determine_convergence_converged_h2_isif_3(tmp_path):
    job_dir = os.path.join(tmp_path, "job_dir")
    shutil.copytree("test/test_files/h2_completed_run", job_dir)
    with open(os.path.join(job_dir, "INCAR"), "a+") as f:
        f.write("ISIF = 3")
    converged = determine_convergence(job_dir)
    assert converged is False


def test_classify_job_dir_dos_job():
    ret_val = classify_job_dir("/home/jw53959/lab2/dos")
    assert ret_val == "dos"


def test_classify_job_dir_wav_job():
    ret_val = classify_job_dir("/home/jw53959/lab2/wav")
    assert ret_val == "wav"


def test_classify_job_dir_sc_job():
    ret_val = classify_job_dir("/home/jw53959/lab2/sc")
    assert ret_val == "sc"


def test_classify_job_dir_opt_job():
    ret_val = classify_job_dir("/home/jw53959/lab2")
    assert ret_val == "opt"


def test_classify_job_dir_dos_job_trailing_slash():
    ret_val = classify_job_dir("/home/jw53959/lab2/dos")
    assert ret_val == "dos"


def test_classify_job_dir_dos_job_pathlib_path():
    ret_val = classify_job_dir(pathlib.PosixPath("/home/jw53959/lab2/dos"))
    assert ret_val == "dos"


def test_classify_job_dir_opt_job_dos_in_path():
    ret_val = classify_job_dir("/home/jw53959/lab2/dos/hi")
    assert ret_val == "opt"


def test_gone_job_check(tmp_path):
    database = Database(os.path.join(tmp_path, "test_db"))
    os.mkdir(os.path.join(tmp_path, "opt_job_1"))
    database.add_opt_job_to_db(
        OptJob(JobStatus.INCOMPLETE, Machine.FRI, Machine.FRI),
        os.path.join(tmp_path, "opt_job_1"),
    )
    database.add_opt_job_to_db(
        OptJob(JobStatus.CONVERGED, Machine.FRI, Machine.FRI),
        os.path.join(tmp_path, "opt_job_2"),
    )
    database.add_opt_job_to_db(
        OptJob(JobStatus.ERROR, Machine.FRI, Machine.FRI),
        os.path.join(tmp_path, "opt_job_3"),
    )
    database.add_opt_job_to_db(
        OptJob(JobStatus.NOT_FOUND, Machine.FRI, Machine.FRI),
        os.path.join(tmp_path, "opt_job_4"),
    )
    database.add_opt_job_to_db(
        OptJob(JobStatus.RUNNING, Machine.FRI, Machine.FRI),
        os.path.join(tmp_path, "opt_job_5"),
    )
    database.add_opt_job_to_db(
        OptJob(JobStatus.INCOMPLETE, Machine.FRI, Machine.FRI),
        os.path.join(tmp_path, "opt_job_6"),
    )
    opt_jobs = database.get_opt_jobs()
    gone_jobs = gone_job_check(database, opt_jobs)
    assert opt_jobs == {
        os.path.join(tmp_path, "opt_job_1"): OptJob(
            JobStatus.INCOMPLETE, Machine.FRI, Machine.FRI
        ),
        os.path.join(tmp_path, "opt_job_2"): OptJob(
            JobStatus.CONVERGED, Machine.FRI, Machine.FRI
        ),
        os.path.join(tmp_path, "opt_job_3"): OptJob(
            JobStatus.ERROR, Machine.FRI, Machine.FRI
        ),
        os.path.join(tmp_path, "opt_job_4"): OptJob(
            JobStatus.NOT_FOUND, Machine.FRI, Machine.FRI
        ),
        os.path.join(tmp_path, "opt_job_5"): OptJob(
            JobStatus.RUNNING, Machine.FRI, Machine.FRI
        ),
    }
    assert gone_jobs == {
        os.path.join(tmp_path, "opt_job_6"): GoneJob(
            os.path.join(tmp_path, "opt_job_6"),
            JobStatus.INCOMPLETE,
            Machine.FRI,
            Machine.FRI,
        )
    }

    assert database.get_opt_jobs() == opt_jobs
    assert database.get_gone_jobs() == gone_jobs


def test_process_dos_no_files(tmp_path):
    job_dir = os.path.join(tmp_path, "job_dir")
    home_dir = os.path.join(tmp_path, "home_dir")
    os.mkdir(job_dir)
    os.mkdir(home_dir)
    opt_jobs = {}
    dos_jobs = {}
    sub_quene = []
    process_dos(
        job_directory=job_dir,
        opt_jobs=opt_jobs,
        dos_jobs=dos_jobs,
        continue_past_limit=False,
        limit=50,
        sub_queue=sub_quene,
        machine=0,
        hit_limit=False,
    )
    assert sub_quene == []
    assert dos_jobs == {}
    assert opt_jobs == {}


@pytest.mark.skip(reason="h2_dos does not exist")
def test_process_dos_no_dos(tmp_path):
    job_dir = os.path.join(tmp_path, "job_dir")
    home_dir = os.path.join(tmp_path, "home_dir")
    shutil.copytree("test/test_files/h2_dos", job_dir)
    os.mkdir(home_dir)
    test_job = OptJob(JobStatus.CONVERGED, 0, 0)
    opt_jobs = {job_dir: test_job}
    dos_jobs = {}
    sub_quene = []
    dt_epoch = time.time()
    CHGCAR_path = os.path.join(job_dir, "CHGCAR")
    os.utime(CHGCAR_path, (dt_epoch - 120, dt_epoch - 120))
    DOSCAR_path = os.path.join(job_dir, "dos", "DOSCAR")
    os.utime(DOSCAR_path, (dt_epoch - 120, dt_epoch - 120))
    process_dos(
        job_directory=job_dir,
        opt_jobs=opt_jobs,
        dos_jobs=dos_jobs,
        continue_past_limit=False,
        limit=50,
        sub_queue=sub_quene,
        machine=0,
        hit_limit=False,
    )
    assert sub_quene != []
    assert dos_jobs == {
        job_dir: DosJob(-1, JobStatus.CONVERGED, JobStatus.RUNNING, 0, 0)
    }
    assert opt_jobs == {job_dir: OptJob(JobStatus.CONVERGED, 0, 0)}


def test_process_dos_completed_h2_running(tmp_path):
    job_dir = os.path.join(tmp_path, "job_dir")
    home_dir = os.path.join(tmp_path, "home_dir")
    shutil.copytree("test/test_files/h2_sc", job_dir)
    os.mkdir(home_dir)
    test_OptJob = OptJob(JobStatus.RUNNING, 0, 0)
    test_DosJob = DosJob(-1, JobStatus.RUNNING, JobStatus.RUNNING, 0, 0)
    opt_jobs = {job_dir: test_OptJob}
    dos_jobs = {job_dir: test_DosJob}
    sub_quene = []
    process_dos(
        job_directory=job_dir,
        opt_jobs=opt_jobs,
        dos_jobs=dos_jobs,
        continue_past_limit=False,
        limit=50,
        sub_queue=sub_quene,
        machine=0,
        hit_limit=False,
    )

    assert sub_quene == []
    assert opt_jobs == {job_dir: OptJob(JobStatus.RUNNING, 0, 0)}
    assert dos_jobs == {job_dir: DosJob(-1, JobStatus.RUNNING, JobStatus.RUNNING, 0, 0)}


def test_process_dos_completed_h2_incomplete(tmp_path):
    job_dir = os.path.join(tmp_path, "job_dir")
    home_dir = os.path.join(tmp_path, "home_dir")
    shutil.copytree("test/test_files/h2_sc", job_dir)
    os.mkdir(home_dir)
    test_OptJob = OptJob(JobStatus.CONVERGED, 0, 0)
    test_DosJob = DosJob(-1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 0, 0)
    opt_jobs = {job_dir: test_OptJob}
    dos_jobs = {job_dir: test_DosJob}
    sub_quene = []
    process_dos(
        job_directory=job_dir,
        opt_jobs=opt_jobs,
        dos_jobs=dos_jobs,
        continue_past_limit=False,
        limit=50,
        sub_queue=sub_quene,
        machine=0,
        hit_limit=False,
    )

    assert sub_quene != []
    assert opt_jobs == {job_dir: OptJob(JobStatus.CONVERGED, 0, 0)}
    assert dos_jobs == {
        job_dir: DosJob(-1, JobStatus.CONVERGED, JobStatus.RUNNING, 0, 0)
    }


def test_process_dos_failed_run(tmp_path):
    job_dir = os.path.join(tmp_path, "job_dir")
    home_dir = os.path.join(tmp_path, "home_dir")
    shutil.copytree("test/test_files/failed_u_run", job_dir)
    os.mkdir(home_dir)
    test_OptJob = OptJob(JobStatus.ERROR, 0, 0)
    test_DosJob = DosJob(-1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 0, 0)
    opt_jobs = {job_dir: test_OptJob}
    dos_jobs = {job_dir: test_DosJob}
    sub_quene = []
    process_dos(
        job_directory=job_dir,
        opt_jobs=opt_jobs,
        dos_jobs=dos_jobs,
        continue_past_limit=False,
        limit=50,
        sub_queue=sub_quene,
        machine=0,
        hit_limit=False,
    )

    assert sub_quene == []
    assert opt_jobs == {job_dir: OptJob(JobStatus.ERROR, 0, 0)}
    assert dos_jobs == {
        job_dir: DosJob(-1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 0, 0)
    }


@pytest.mark.skip(reason="h2_sc_with_dos does not exist")
def test_process_dos_completed_h2(tmp_path):
    job_dir = os.path.join(tmp_path, "job_dir")
    home_dir = os.path.join(tmp_path, "home_dir")
    shutil.copytree("test/test_files/h2_sc_with_dos", job_dir)
    os.mkdir(home_dir)
    test_OptJob = OptJob(JobStatus.CONVERGED, 0, 0)
    test_DosJob = DosJob(-1, JobStatus.CONVERGED, JobStatus.CONVERGED, 0, 0)
    opt_jobs = {job_dir: test_OptJob}
    dos_jobs = {job_dir: test_DosJob}
    sub_quene = []
    dt_epoch = time.time()
    CHGCAR_path = os.path.join(job_dir, "CHGCAR")
    os.utime(CHGCAR_path, (dt_epoch - 120, dt_epoch - 120))
    process_dos(
        job_directory=job_dir,
        opt_jobs=opt_jobs,
        dos_jobs=dos_jobs,
        continue_past_limit=False,
        limit=50,
        sub_queue=sub_quene,
        machine=0,
        hit_limit=False,
    )
    assert sub_quene == []
    assert opt_jobs == {job_dir: OptJob(JobStatus.CONVERGED, 0, 0)}
    assert dos_jobs == {
        job_dir: DosJob(-1, JobStatus.CONVERGED, JobStatus.CONVERGED, 0, 0)
    }
import filecmp
import os
import shutil
from pathlib import PosixPath

from automagician.classes import DosJob, JobStatus, OptJob, WavJob
from automagician.update_job import (
    add_preliminary_results,
    fix_error,
    get_error_message,
    get_opt_dir,
    log_error,
    set_status_for_newly_submitted_job,
    update_job_name,
)


def test_add_preliminary_results(tmp_path):
    with open(
        os.path.join(tmp_path, "prelminary_results.txt"), "w"
    ) as preliminary_results:
        add_preliminary_results(tmp_path, 5, 5.5, 3.0, preliminary_results)
    with open(
        os.path.join(tmp_path, "prelminary_results.txt"), "r"
    ) as preliminary_results:
        file = preliminary_results.read()
        assert file == f"{str(tmp_path)}\n     5     5.5     3.0\n"


def test_get_error_message_one_message(tmp_path):
    shutil.copy("test/test_files/failed_u_run/ll_out", tmp_path)

    error_messages = get_error_message(tmp_path)
    assert error_messages == ["ZBRENT: fatal error in bracketing"]


def test_get_error_message_no_messages(tmp_path):
    shutil.copy("test/test_files/h2/ll_out", tmp_path)

    error_messages = get_error_message(tmp_path)
    assert error_messages == []


def test_get_error_message_two_message(tmp_path):
    shutil.copy("test/test_files/failed_u_run/ll_out", tmp_path)
    ll_out_path = os.path.join(tmp_path, "ll_out")
    ll_out = open(ll_out_path, "a+")
    ll_out.write("error A test error")
    ll_out.close()
    error_messages = get_error_message(tmp_path)
    assert error_messages == ["ZBRENT: fatal error in bracketing", "error A test error"]


def test_log_error_no_prev_errors_1_error(tmp_path):
    job_path = os.path.join(tmp_path, "job_path")
    home_path = os.path.join(tmp_path, "fake_home")
    os.mkdir(job_path)
    os.mkdir(home_path)
    shutil.copy("test/test_files/failed_u_run/ll_out", job_path)
    log_error(job_path, home_path)
    assert os.path.exists(os.path.join(home_path, "error_log.dat"))
    error_log = open(os.path.join(home_path, "error_log.dat"))
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
    error_log = open(os.path.join(home_path, "error_log.dat"))
    log_file = error_log.readline()
    assert f"{job_path}" in log_file
    assert "ZBRENT: fatal error in bracketing" in log_file
    log_file = error_log.readline()
    assert f"{job_path}" in log_file
    assert "ZBRENT: fatal error in bracketing" in log_file


def test_fix_error_ZBRENT_no_CONTCAR(tmp_path):
    job_path = os.path.join(tmp_path, "job_path")
    os.mkdir(job_path)
    shutil.copy("test/test_files/failed_u_run/ll_out", job_path)
    shutil.copy("test/test_files/failed_u_run/fri.sub", job_path)
    error_was_fixed = fix_error(job_path)
    assert error_was_fixed is False


def test_fix_error_ZBRENT_CONTCAR_present(tmp_path):
    job_path = os.path.join(tmp_path, "job_path")
    shutil.copytree("test/test_files/failed_u_run", job_path)
    error_was_fixed = fix_error(job_path)
    assert error_was_fixed is True
    assert os.path.isdir(os.path.join(job_path, "run0"))


def test_fix_error_bad_POTCAR(tmp_path):
    job_path = os.path.join(tmp_path, "job_path")
    shutil.copytree("test/test_files/bad_potcar", job_path)
    error_was_fixed = fix_error(job_path)
    assert error_was_fixed is True
    assert filecmp.cmp(
        os.path.join(job_path, "POTCAR"),
        "test/test_files/bad_potcar/POTCAR_fixed",
        False,
    )


def test_fix_error_no_error(tmp_path):
    job_path = os.path.join(tmp_path, "job_path")
    shutil.copytree("test/test_files/h2", job_path)
    error_was_fixed = fix_error(job_path)
    assert error_was_fixed is False


def test_get_opt_dir_dos():
    opt_dir = get_opt_dir("/home/jw53939/hi/dos")
    assert opt_dir == "/home/jw53939/hi"


def test_get_opt_dir_wav():
    opt_dir = get_opt_dir("/home/jw53939/hi/wav")
    assert opt_dir == "/home/jw53939/hi"


def test_get_opt_dir_sc():
    opt_dir = get_opt_dir("/home/jw53939/hi/sc")
    assert opt_dir == "/home/jw53939/hi"


def test_get_opt_dir_opt():
    opt_dir = get_opt_dir("/home/jw53939/hi")
    assert opt_dir == "/home/jw53939/hi"


def test_get_opt_dir_posix_path():
    opt_dir = get_opt_dir(PosixPath("/home/jw53939/hi"))
    assert opt_dir == "/home/jw53939/hi"


def test_get_opt_dir_posix_path_dos():
    opt_dir = get_opt_dir(PosixPath("/home/jw53939/hi/dos"))
    assert opt_dir == "/home/jw53939/hi"


def test_set_status_for_newly_submitted_job_opt_job():
    dos_jobs = {}
    wav_jobs = {}
    job = OptJob(JobStatus.INCOMPLETE, 0, 0)
    job_dir = "/home/jw53959/lab2"
    job_2 = OptJob(JobStatus.INCOMPLETE, 3, 0)
    job_dir_2 = "/home/jw53959/lab2/hi"
    opt_jobs = {job_dir: job, job_dir_2: job_2}
    set_status_for_newly_submitted_job(job_dir, 0, dos_jobs, wav_jobs, opt_jobs, False)
    new_job = OptJob(JobStatus.RUNNING, 0, 0)
    assert opt_jobs == {
        job_dir: new_job,
        job_dir_2: OptJob(JobStatus.INCOMPLETE, 3, 0),
    }


def test_set_status_for_newly_submitted_job_dos_job():
    job_dir = "/home/jw53959/lab3/dos"
    job = DosJob(-1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 5, 4)
    dos_job_dir_2 = "/home/jw53959/lab5/"
    dos_job_2 = DosJob(-1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 6, 3)
    dos_jobs = {"/home/jw53959/lab3": job, dos_job_dir_2: dos_job_2}
    wav_jobs = {}
    job_opt = OptJob(JobStatus.INCOMPLETE, 0, 0)
    job_dir_opt = "/home/jw53959/lab2"
    opt_job_2 = OptJob(JobStatus.INCOMPLETE, 3, 0)
    opt_job_dir_2 = "/home/jw53959/lab2/hi"
    opt_jobs = {job_dir_opt: job_opt, opt_job_dir_2: opt_job_2}
    set_status_for_newly_submitted_job(job_dir, 0, dos_jobs, wav_jobs, opt_jobs, False)
    assert opt_jobs == {
        job_dir_opt: OptJob(JobStatus.INCOMPLETE, 0, 0),
        opt_job_dir_2: OptJob(JobStatus.INCOMPLETE, 3, 0),
    }
    assert dos_jobs == {
        "/home/jw53959/lab3": DosJob(-1, JobStatus.INCOMPLETE, JobStatus.RUNNING, 5, 0),
        dos_job_dir_2: DosJob(-1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 6, 3),
    }


def test_set_status_for_newly_submitted_job_sc_job():
    job_dir = "/home/jw53959/lab3/sc"
    job = DosJob(-1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 5, 4)
    dos_job_dir_2 = "/home/jw53959/lab5/"
    dos_job_2 = DosJob(-1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 6, 3)
    dos_jobs = {"/home/jw53959/lab3": job, dos_job_dir_2: dos_job_2}
    wav_jobs = {}
    job_opt = OptJob(JobStatus.INCOMPLETE, 0, 0)
    job_dir_opt = "/home/jw53959/lab2"
    opt_job_2 = OptJob(JobStatus.INCOMPLETE, 3, 0)
    opt_job_dir_2 = "/home/jw53959/lab2/hi"
    opt_jobs = {job_dir_opt: job_opt, opt_job_dir_2: opt_job_2}
    set_status_for_newly_submitted_job(job_dir, 0, dos_jobs, wav_jobs, opt_jobs, False)
    assert opt_jobs == {
        job_dir_opt: OptJob(JobStatus.INCOMPLETE, 0, 0),
        opt_job_dir_2: OptJob(JobStatus.INCOMPLETE, 3, 0),
    }
    assert dos_jobs == {
        "/home/jw53959/lab3": DosJob(-1, JobStatus.RUNNING, JobStatus.INCOMPLETE, 0, 4),
        dos_job_dir_2: DosJob(-1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 6, 3),
    }


def test_set_status_for_newly_submitted_job_wav_job():
    dos_job_dir = "/home/jw53959/lab3"
    job = DosJob(-1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 5, 4)
    dos_job_dir_2 = "/home/jw53959/lab5"
    dos_job_2 = DosJob(-1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 6, 3)
    dos_jobs = {"/home/jw53959/lab3": job, dos_job_dir_2: dos_job_2}
    job_dir = "/home/jw53959/wav"
    job = WavJob(-1, JobStatus.INCOMPLETE, 8)
    wav_job_dir_2 = "/home/jw53959/lab6"
    wav_job_2 = WavJob(-1, JobStatus.INCOMPLETE, 9)
    wav_jobs = {"/home/jw53959": job, wav_job_dir_2: wav_job_2}
    job_opt = OptJob(JobStatus.INCOMPLETE, 0, 0)
    job_dir_opt = "/home/jw53959/lab2"
    opt_job_2 = OptJob(JobStatus.INCOMPLETE, 3, 0)
    opt_job_dir_2 = "/home/jw53959/lab2/hi"
    opt_jobs = {job_dir_opt: job_opt, opt_job_dir_2: opt_job_2}
    set_status_for_newly_submitted_job(job_dir, 0, dos_jobs, wav_jobs, opt_jobs, False)
    assert opt_jobs == {
        job_dir_opt: OptJob(JobStatus.INCOMPLETE, 0, 0),
        opt_job_dir_2: OptJob(JobStatus.INCOMPLETE, 3, 0),
    }
    assert dos_jobs == {
        dos_job_dir: DosJob(-1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 5, 4),
        dos_job_dir_2: DosJob(-1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 6, 3),
    }
    assert wav_jobs == {
        "/home/jw53959": WavJob(-1, JobStatus.RUNNING, 0),
        wav_job_dir_2: WavJob(-1, JobStatus.INCOMPLETE, 9),
    }


def test_set_status_for_newly_submitted_job_opt_job_error():
    dos_jobs = {}
    wav_jobs = {}
    job = OptJob(JobStatus.INCOMPLETE, 0, 0)
    job_dir = "/home/jw53959/lab2"
    job_2 = OptJob(JobStatus.INCOMPLETE, 3, 0)
    job_dir_2 = "/home/jw53959/lab2/hi"
    opt_jobs = {job_dir: job, job_dir_2: job_2}
    set_status_for_newly_submitted_job(job_dir, 0, dos_jobs, wav_jobs, opt_jobs, True)
    assert opt_jobs == {
        job_dir: OptJob(JobStatus.ERROR, 0, 0),
        job_dir_2: OptJob(JobStatus.INCOMPLETE, 3, 0),
    }


def test_set_status_for_newly_submitted_job_dos_job_error():
    job_dir = "/home/jw53959/lab3/dos"
    job = DosJob(-1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 5, 4)
    dos_job_dir_2 = "/home/jw53959/lab5/"
    dos_job_2 = DosJob(-1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 6, 3)
    dos_jobs = {"/home/jw53959/lab3": job, dos_job_dir_2: dos_job_2}
    wav_jobs = {}
    job_opt = OptJob(JobStatus.INCOMPLETE, 0, 0)
    job_dir_opt = "/home/jw53959/lab2"
    opt_job_2 = OptJob(JobStatus.INCOMPLETE, 3, 0)
    opt_job_dir_2 = "/home/jw53959/lab2/hi"
    opt_jobs = {job_dir_opt: job_opt, opt_job_dir_2: opt_job_2}
    set_status_for_newly_submitted_job(job_dir, 0, dos_jobs, wav_jobs, opt_jobs, True)
    assert opt_jobs == {
        job_dir_opt: OptJob(JobStatus.INCOMPLETE, 0, 0),
        opt_job_dir_2: OptJob(JobStatus.INCOMPLETE, 3, 0),
    }
    assert dos_jobs == {
        "/home/jw53959/lab3": DosJob(-1, JobStatus.INCOMPLETE, JobStatus.ERROR, 5, 0),
        dos_job_dir_2: DosJob(-1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 6, 3),
    }


def test_set_status_for_newly_submitted_job_sc_job_error():
    job_dir = "/home/jw53959/lab3/sc"
    job = DosJob(-1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 5, 4)
    dos_job_dir_2 = "/home/jw53959/lab5/"
    dos_job_2 = DosJob(-1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 6, 3)
    dos_jobs = {"/home/jw53959/lab3": job, dos_job_dir_2: dos_job_2}
    wav_jobs = {}
    job_opt = OptJob(JobStatus.INCOMPLETE, 0, 0)
    job_dir_opt = "/home/jw53959/lab2"
    opt_job_2 = OptJob(JobStatus.INCOMPLETE, 3, 0)
    opt_job_dir_2 = "/home/jw53959/lab2/hi"
    opt_jobs = {job_dir_opt: job_opt, opt_job_dir_2: opt_job_2}
    set_status_for_newly_submitted_job(job_dir, 0, dos_jobs, wav_jobs, opt_jobs, True)
    assert opt_jobs == {
        job_dir_opt: OptJob(JobStatus.INCOMPLETE, 0, 0),
        opt_job_dir_2: OptJob(JobStatus.INCOMPLETE, 3, 0),
    }
    assert dos_jobs == {
        "/home/jw53959/lab3": DosJob(-1, JobStatus.ERROR, JobStatus.INCOMPLETE, 0, 4),
        dos_job_dir_2: DosJob(-1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 6, 3),
    }


def test_set_status_for_newly_submitted_job_wav_job_error():
    dos_job_dir = "/home/jw53959/lab3"
    job = DosJob(-1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 5, 4)
    dos_job_dir_2 = "/home/jw53959/lab5"
    dos_job_2 = DosJob(-1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 6, 3)
    dos_jobs = {"/home/jw53959/lab3": job, dos_job_dir_2: dos_job_2}
    job_dir = "/home/jw53959/wav"
    job = WavJob(-1, JobStatus.INCOMPLETE, 8)
    wav_job_dir_2 = "/home/jw53959/lab6"
    wav_job_2 = WavJob(-1, JobStatus.INCOMPLETE, 9)
    wav_jobs = {"/home/jw53959": job, wav_job_dir_2: wav_job_2}
    job_opt = OptJob(JobStatus.INCOMPLETE, 0, 0)
    job_dir_opt = "/home/jw53959/lab2"
    opt_job_2 = OptJob(JobStatus.INCOMPLETE, 3, 0)
    opt_job_dir_2 = "/home/jw53959/lab2/hi"
    opt_jobs = {job_dir_opt: job_opt, opt_job_dir_2: opt_job_2}
    set_status_for_newly_submitted_job(job_dir, 0, dos_jobs, wav_jobs, opt_jobs, True)
    assert opt_jobs == {
        job_dir_opt: OptJob(JobStatus.INCOMPLETE, 0, 0),
        opt_job_dir_2: OptJob(JobStatus.INCOMPLETE, 3, 0),
    }
    assert dos_jobs == {
        dos_job_dir: DosJob(-1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 5, 4),
        dos_job_dir_2: DosJob(-1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 6, 3),
    }
    assert wav_jobs == {
        "/home/jw53959": WavJob(-1, JobStatus.ERROR, 0),
        wav_job_dir_2: WavJob(-1, JobStatus.INCOMPLETE, 9),
    }


def test_update_job_name_short_job_name(tmp_path):
    subfile_path = os.path.join(tmp_path, "fri.sub")
    shutil.copy("test/test_files/test_subfiles/fri_short_job_name.sub", subfile_path)
    cwd = os.getcwd()
    os.chdir(tmp_path)
    update_job_name(subfile_path)
    os.chdir(cwd)
    assert os.path.isfile(subfile_path)
    with open(subfile_path) as f:
        # SBATCH -J " + "AM_" + os.getcwd().replace("/", "_") + "\n
        lines = f.readlines()
        print(lines)
        assert lines == [
            "#!/bin/bash -l\n",
            "#SBATCH --chdir=./\n",
            "#SBATCH --output=ll_out\n",
            f"#SBATCH -J AM_{str(tmp_path).replace('/', '_')}\n",
            "#SBATCH --mail-type ALL\n",
            "#SBATCH --export=ALL\n",
            "#SBATCH --partition=all.q@@core24\n",
            "#SBATCH --ntasks-per-node=24\n",
            "\n",
            "mpirun -n $NSLOTS vasp_gamma\n",
            "\n",
        ]


def test_update_job_name_long_job_name(tmp_path):
    subfile_path = os.path.join(tmp_path, "fri.sub")
    shutil.copy("test/test_files/test_subfiles/fri_long_job_name.sub", subfile_path)
    cwd = os.getcwd()
    os.chdir(tmp_path)
    update_job_name(subfile_path)
    os.chdir(cwd)
    assert os.path.isfile(subfile_path)
    with open(subfile_path) as f:
        # SBATCH -J " + "AM_" + os.getcwd().replace("/", "_") + "\n
        lines = f.readlines()
        assert lines == [
            "#!/bin/bash -l\n",
            "#SBATCH --chdir=./\n",
            "#SBATCH --output=ll_out\n",
            f"#SBATCH -J AM_{str(tmp_path).replace('/', '_')}\n",
            "#SBATCH --mail-type ALL\n",
            "#SBATCH --export=ALL\n",
            "#SBATCH --partition=all.q@@core24\n",
            "#SBATCH --ntasks-per-node=24\n",
            "\n",
            "mpirun -n $NSLOTS vasp_gamma\n",
            "\n",
        ]
import os
import shutil
import time

import automagician.constants as constants

from automagician.finish_job import (
    dos_is_complete,
    give_certificate,
    sc_is_complete,
    wav_is_complete,
    wrap_up,
)


def test_wrap_up_no_previous_attempts(tmp_path):
    old_cwd = os.getcwd()
    job_path = os.path.join(tmp_path, "job")
    shutil.copytree("test/test_files/h2_completed_run", job_path)
    run_dir = os.path.join(job_path, "run0")
    wrap_up(job_path)
    assert os.getcwd() == old_cwd
    print(run_dir)
    assert os.path.isdir(run_dir)
    assert os.path.isfile(os.path.join(run_dir, "ll_out"))


def test_wrap_up_one_previous_attempt(tmp_path):
    old_cwd = os.getcwd()
    job_path = os.path.join(tmp_path, "job")
    shutil.copytree("test/test_files/h2_completed_run", job_path)
    os.mkdir(os.path.join(job_path, "run0"))
    run_dir = os.path.join(job_path, "run1")
    wrap_up(job_path)
    assert os.getcwd() == old_cwd
    print(run_dir)
    assert os.path.isdir(run_dir)
    assert os.path.isfile(os.path.join(run_dir, "ll_out"))


def test_wrap_up_previous_attempts_deleted(tmp_path):
    old_cwd = os.getcwd()
    job_path = os.path.join(tmp_path, "job")
    shutil.copytree("test/test_files/h2_completed_run", job_path)
    os.mkdir(os.path.join(job_path, "run7"))
    run_dir = os.path.join(job_path, "run8")
    wrap_up(job_path)
    assert os.getcwd() == old_cwd
    print(run_dir)
    assert os.path.isdir(run_dir)
    assert os.path.isfile(os.path.join(run_dir, "ll_out"))


def test_wrap_up_double_digits(tmp_path):
    old_cwd = os.getcwd()
    job_path = os.path.join(tmp_path, "job")
    shutil.copytree("test/test_files/h2_completed_run", job_path)
    os.mkdir(os.path.join(job_path, "run10"))
    os.mkdir(os.path.join(job_path, "run9"))
    run_dir = os.path.join(job_path, "run11")
    wrap_up(job_path)
    assert os.getcwd() == old_cwd
    print(run_dir)
    assert os.path.isdir(run_dir)
    assert os.path.isfile(os.path.join(run_dir, "ll_out"))


def test_wrap_up_error(tmp_path):
    old_cwd = os.getcwd()
    job_path = os.path.join(tmp_path, "job")
    shutil.copytree("test/test_files/h2_completed_run", job_path)
    os.mkdir(os.path.join(job_path, "run"))
    os.mkdir(os.path.join(job_path, "run9"))
    run_dir = os.path.join(job_path, "run10")
    wrap_up(job_path)
    assert os.getcwd() == old_cwd
    print(run_dir)
    assert os.path.isdir(run_dir)
    assert os.path.isfile(os.path.join(run_dir, "ll_out"))


def test_give_certificate(tmp_path):
    give_certificate(tmp_path)
    certificate_path = os.path.join(tmp_path, "convergence_certificate")
    assert os.path.isfile(certificate_path)


def test_sc_is_complete_new_file(tmp_path):
    CHGCAR_path = os.path.join(tmp_path, "CHGCAR")
    CHGCAR_file = open(CHGCAR_path, "x")
    CHGCAR_file.close()
    complete_result = sc_is_complete(tmp_path)
    assert complete_result is False


def test_sc_is_complete_file_not_present(tmp_path):
    complete_result = sc_is_complete(tmp_path)
    assert complete_result is False


def test_sc_is_complete_old_file(tmp_path):
    CHGCAR_path = os.path.join(tmp_path, "CHGCAR")
    CHGCAR_file = open(CHGCAR_path, "x")
    CHGCAR_file.close()
    dt_epoch = time.time()
    os.utime(CHGCAR_path, (dt_epoch - 120, dt_epoch - 120))
    complete_result = sc_is_complete(tmp_path)
    assert complete_result is True


def test_dos_is_complete_new_file(tmp_path):
    DOSCAR_path = os.path.join(tmp_path, "DOSCAR")
    DOSCAR_file = open(DOSCAR_path, "x")
    DOSCAR_file.close()
    complete_result = dos_is_complete(tmp_path)
    assert complete_result is False


def test_dos_is_complete_file_not_present(tmp_path):
    complete_result = dos_is_complete(tmp_path)
    assert complete_result is False


def test_dos_is_complete_old_file(tmp_path):
    DOSCAR_path = os.path.join(tmp_path, "DOSCAR")
    DOSCAR_file = open(DOSCAR_path, "x")
    DOSCAR_file.close()
    dt_epoch = time.time()
    os.utime(DOSCAR_path, (dt_epoch - 120, dt_epoch - 120))
    complete_result = dos_is_complete(tmp_path)
    assert complete_result is True


def test_wav_is_complete_new_file(tmp_path):
    WAVECAR_path = os.path.join(tmp_path, "WAVECAR")
    WAVECAR_file = open(WAVECAR_path, "x")
    WAVECAR_file.close()
    complete_result = wav_is_complete(tmp_path)
    assert complete_result is False


def test_wav_is_complete_file_not_present(tmp_path):
    complete_result = wav_is_complete(tmp_path)
    assert complete_result is False


def test_wav_is_complete_old_file(tmp_path):
    WAVECAR_path = os.path.join(tmp_path, "WAVECAR")
    WAVECAR_file = open(WAVECAR_path, "x")
    WAVECAR_file.close()
    dt_epoch = time.time()
    os.utime(WAVECAR_path, (dt_epoch - 120, dt_epoch - 120))
    complete_result = wav_is_complete(tmp_path)
    assert complete_result is True


def test_give_duplicate_certificate(tmp_path):
    open(
        os.path.join(tmp_path, constants.CONVERGENCE_CERTIFICATE_NAME), "x"
    )
    assert give_certificate(tmp_path) == 1import os
import shutil

import pytest

from automagician.classes import JobLimitError
from automagician.create_job import (
    add_to_sub_queue,
    create_dos_from_sc,
    create_sc,
    create_wav,
)


def test_sub_hit_limit(tmp_path):
    subfile_path = str(tmp_path) + "/fri.sub"
    open(subfile_path, "w")
    jobs = []
    with pytest.raises(JobLimitError):
        add_to_sub_queue(
            job_directory=tmp_path,
            continue_past_limit=False,
            limit=1,
            sub_queue=jobs,
            machine=0,
            hit_limit=False,
        )

    assert tmp_path in jobs


def test_sub_hit_limit_allowed(tmp_path):
    subfile_path = str(tmp_path) + "/fri.sub"
    open(subfile_path, "w")
    jobs = []
    hit_limit = add_to_sub_queue(
        job_directory=tmp_path,
        continue_past_limit=True,
        limit=1,
        sub_queue=jobs,
        machine=0,
        hit_limit=False,
    )
    assert hit_limit is True
    assert tmp_path in jobs


def test_qsub_hit_limit_allowed_multiple_paths(tmp_path):
    subfile_path = str(tmp_path) + "/fri.sub"
    open(subfile_path, "w")
    sub_quene = ["/tmp/hi"]
    hit_limit = add_to_sub_queue(
        job_directory=tmp_path,
        continue_past_limit=True,
        limit=1,
        sub_queue=sub_quene,
        machine=0,
        hit_limit=False,
    )
    assert hit_limit is True
    assert sub_quene == ["/tmp/hi", tmp_path]


def test_sub_already_hit_limit(tmp_path):
    subfile_path = str(tmp_path) + "/fri.sub"
    open(subfile_path, "w")
    jobs = []
    hit_limit = add_to_sub_queue(
        job_directory=tmp_path,
        continue_past_limit=True,
        limit=1000,
        sub_queue=jobs,
        machine=0,
        hit_limit=True,
    )
    assert hit_limit is True
    assert tmp_path not in jobs


def test_sub_hit_limit_allowed_did_not_hit(tmp_path):
    subfile_path = str(tmp_path) + "/fri.sub"
    open(subfile_path, "w")
    jobs = []
    hit_limit = add_to_sub_queue(
        job_directory=tmp_path,
        continue_past_limit=True,
        limit=2,
        sub_queue=jobs,
        machine=0,
        hit_limit=False,
    )
    assert hit_limit is False
    assert tmp_path in jobs


def test_create_dos_from_sc_poscar(tmp_path):
    jobs = []
    old_cwd = os.getcwd()
    sc_job_path = os.path.join(tmp_path, "sc")
    dos_job_path = os.path.join(tmp_path, "dos")
    shutil.copytree("test/test_files/h2_sc", sc_job_path)
    os.remove(os.path.join(sc_job_path, "CONTCAR"))

    create_dos_from_sc(sc_job_path, False, 2, jobs, 0, False)
    new_cwd = os.getcwd()

    assert old_cwd == new_cwd
    assert os.path.isfile(os.path.join(dos_job_path, "INCAR"))
    assert os.path.isfile(os.path.join(dos_job_path, "fri.sub"))
    assert os.path.isfile(os.path.join(dos_job_path, "KPOINTS"))
    assert os.path.isfile(os.path.join(dos_job_path, "POTCAR"))
    assert os.path.isfile(os.path.join(dos_job_path, "CHGCAR"))
    assert os.path.isfile(os.path.join(dos_job_path, "POSCAR"))
    incar = open(os.path.join(dos_job_path, "INCAR"))
    incar_text = incar.read()
    assert "ICHARGE=11" in incar_text
    assert "LORBIT=11" in incar_text
    expected_jobs = [dos_job_path]
    assert jobs == expected_jobs


def test_create_dos_from_sc_contcar(tmp_path):
    sub_quene = []
    old_cwd = os.getcwd()
    sc_job_path = os.path.join(tmp_path, "sc")
    dos_job_path = os.path.join(tmp_path, "dos")
    shutil.copytree("test/test_files/h2_sc", sc_job_path)
    os.remove(os.path.join(sc_job_path, "POSCAR"))

    create_dos_from_sc(sc_job_path, False, 2, sub_quene, 0, False)
    new_cwd = os.getcwd()

    assert old_cwd == new_cwd
    assert os.path.isfile(os.path.join(dos_job_path, "INCAR"))
    assert os.path.isfile(os.path.join(dos_job_path, "fri.sub"))
    assert os.path.isfile(os.path.join(dos_job_path, "KPOINTS"))
    assert os.path.isfile(os.path.join(dos_job_path, "POTCAR"))
    assert os.path.isfile(os.path.join(dos_job_path, "CHGCAR"))
    assert os.path.isfile(os.path.join(dos_job_path, "CONTCAR"))
    incar = open(os.path.join(dos_job_path, "INCAR"))
    incar_text = incar.read()
    assert "ICHARGE=11" in incar_text
    assert "LORBIT=11" in incar_text
    expected_sub_quene = [dos_job_path]
    assert sub_quene == expected_sub_quene


def test_create_dos_from_sc_invalid_dir(tmp_path):
    sub_quene = []
    old_cwd = os.getcwd()
    sc_job_path = os.path.join(tmp_path, "sc")
    shutil.copytree("test/test_files/h2_sc", sc_job_path)
    os.remove(os.path.join(sc_job_path, "POSCAR"))
    os.remove(os.path.join(sc_job_path, "CONTCAR"))
    with pytest.raises(FileNotFoundError):
        create_dos_from_sc(sc_job_path, False, 2, sub_quene, 0, False)
    new_cwd = os.getcwd()

    assert old_cwd == new_cwd
    assert sub_quene == []


def test_create_wav_potcar(tmp_path):
    sub_quene = []
    old_cwd = os.getcwd()
    job_path = os.path.join(tmp_path, "job")
    wav_job_path = os.path.join(tmp_path, "wav")
    shutil.copytree("test/test_files/h2_sc", job_path)
    os.remove(os.path.join(job_path, "CONTCAR"))

    create_wav(job_path, False, 2, sub_quene, 0, False)
    new_cwd = os.getcwd()

    assert old_cwd == new_cwd
    assert os.path.isdir(wav_job_path)
    assert os.path.isfile(os.path.join(wav_job_path, "INCAR"))
    assert os.path.isfile(os.path.join(wav_job_path, "fri.sub"))
    assert os.path.isfile(os.path.join(wav_job_path, "KPOINTS"))
    assert os.path.isfile(os.path.join(wav_job_path, "POTCAR"))
    assert os.path.isfile(os.path.join(wav_job_path, "POSCAR"))
    incar = open(os.path.join(wav_job_path, "INCAR"))
    incar_text = incar.read()
    assert "IBRION=-1" in incar_text
    assert "LWAVE=.TRUE." in incar_text
    assert "NSW=0" in incar_text
    expected_sub_quene = [wav_job_path]
    assert sub_quene == expected_sub_quene


def test_create_wav_contcar(tmp_path):
    sub_quene = []
    old_cwd = os.getcwd()
    job_path = os.path.join(tmp_path, "job")
    wav_job_path = os.path.join(tmp_path, "wav")
    shutil.copytree("test/test_files/h2_sc", job_path)
    os.remove(os.path.join(job_path, "POSCAR"))

    create_wav(job_path, False, 2, sub_quene, 0, False)
    new_cwd = os.getcwd()

    assert old_cwd == new_cwd
    assert os.path.isdir(wav_job_path)
    assert os.path.isfile(os.path.join(wav_job_path, "INCAR"))
    assert os.path.isfile(os.path.join(wav_job_path, "fri.sub"))
    assert os.path.isfile(os.path.join(wav_job_path, "KPOINTS"))
    assert os.path.isfile(os.path.join(wav_job_path, "POTCAR"))
    assert os.path.isfile(os.path.join(wav_job_path, "CONTCAR"))
    incar = open(os.path.join(wav_job_path, "INCAR"))
    incar_text = incar.read()
    assert "IBRION=-1" in incar_text
    assert "LWAVE=.TRUE." in incar_text
    assert "NSW=0" in incar_text
    expected_sub_quene = [wav_job_path]
    assert sub_quene == expected_sub_quene


def test_create_sc_potcar(tmp_path):
    sub_quene = []
    old_cwd = os.getcwd()
    job_path = os.path.join(tmp_path, "job")
    sc_job_path = os.path.join(tmp_path, "sc")
    shutil.copytree("test/test_files/h2_sc", job_path)
    os.remove(os.path.join(job_path, "CONTCAR"))

    create_sc(job_path, False, 2, sub_quene, 0, False)
    new_cwd = os.getcwd()

    assert old_cwd == new_cwd
    assert os.path.isdir(sc_job_path)
    assert os.path.isfile(os.path.join(sc_job_path, "INCAR"))
    assert os.path.isfile(os.path.join(sc_job_path, "fri.sub"))
    assert os.path.isfile(os.path.join(sc_job_path, "KPOINTS"))
    assert os.path.isfile(os.path.join(sc_job_path, "POTCAR"))
    assert os.path.isfile(os.path.join(sc_job_path, "POSCAR"))
    incar = open(os.path.join(sc_job_path, "INCAR"))
    incar_text = incar.read()
    assert "IBRION=-1" in incar_text
    assert "LCHARGE=.TRUE." in incar_text
    assert "NSW=0" in incar_text
    expected_sub_quene = [sc_job_path]
    assert sub_quene == expected_sub_quene


def test_create_sc_contcar(tmp_path):
    sub_quene = []
    old_cwd = os.getcwd()
    job_path = os.path.join(tmp_path, "job")
    sc_job_path = os.path.join(tmp_path, "sc")
    shutil.copytree("test/test_files/h2_sc", job_path)
    os.remove(os.path.join(job_path, "POSCAR"))

    create_sc(job_path, False, 2, sub_quene, 0, False)
    new_cwd = os.getcwd()

    assert old_cwd == new_cwd
    assert os.path.isdir(sc_job_path)
    assert os.path.isfile(os.path.join(sc_job_path, "INCAR"))
    assert os.path.isfile(os.path.join(sc_job_path, "fri.sub"))
    assert os.path.isfile(os.path.join(sc_job_path, "KPOINTS"))
    assert os.path.isfile(os.path.join(sc_job_path, "POTCAR"))
    assert os.path.isfile(os.path.join(sc_job_path, "CONTCAR"))
    incar = open(os.path.join(sc_job_path, "INCAR"))
    incar_text = incar.read()
    assert "IBRION=-1" in incar_text
    assert "LCHARGE=.TRUE." in incar_text
    assert "NSW=0" in incar_text
    expected_sub_quene = [sc_job_path]
    assert sub_quene == expected_sub_quene
import os
import shutil

from automagician.classes import DosJob, JobStatus, OptJob, SSHConfig, WavJob
from automagician.register import exclude_regex, process_queue, register


def test_process_queue_nothing(tmp_path):
    opt_queue = []
    dos_queue = []
    wav_queue = []
    opt_jobs = {}
    dos_jobs = {}
    wav_jobs = {}
    config = SSHConfig("NoSSH")
    preliminary_results_path = os.path.join(tmp_path, "preliminary_results")
    preliminary_results = open(preliminary_results_path, "w")
    sub_queue = []
    process_queue(
        opt_queue=opt_queue,
        dos_queue=dos_queue,
        wav_queue=wav_queue,
        machine=0,
        opt_jobs=opt_jobs,
        dos_jobs=dos_jobs,
        wav_jobs=wav_jobs,
        clear_certificate=False,
        home_dir=tmp_path,
        ssh_config=config,
        preliminary_results=preliminary_results,
        continue_past_limit=True,
        limit=1000,
        sub_queue=sub_queue,
        hit_limit=False,
    )
    assert sub_queue == []


def test_process_queue_opt_unconverged_dos_wav(tmp_path):
    opt_job_1 = OptJob(JobStatus.INCOMPLETE, 0, 0)
    dos_job_1 = DosJob(-1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 0, 0)
    wav_job_1 = WavJob(-1, JobStatus.INCOMPLETE, 0)
    opt_job_path = os.path.join(tmp_path, "opt")
    shutil.copytree("test/test_files/h2", opt_job_path)
    dos_job_path = os.path.join(opt_job_path, "dos")
    os.mkdir(dos_job_path)
    wav_job_path = os.path.join(opt_job_path, "wav")
    os.mkdir(wav_job_path)
    non_existent_path = os.path.join(tmp_path, "not_here")
    opt_job_2 = OptJob(JobStatus.INCOMPLETE, 0, 0)
    opt_jobs = {opt_job_path: opt_job_1, non_existent_path: opt_job_2}
    dos_jobs = {opt_job_path: dos_job_1}
    wav_jobs = {opt_job_path: wav_job_1}
    config = SSHConfig("NoSSH")
    preliminary_results_path = os.path.join(tmp_path, "preliminary_results")
    preliminary_results = open(preliminary_results_path, "w")
    opt_queue = [opt_job_path, non_existent_path]
    dos_queue = [opt_job_path]
    wav_queue = [opt_job_path]
    sub_queue = []
    process_queue(
        opt_queue=opt_queue,
        dos_queue=dos_queue,
        wav_queue=wav_queue,
        machine=0,
        opt_jobs=opt_jobs,
        dos_jobs=dos_jobs,
        wav_jobs=wav_jobs,
        clear_certificate=False,
        home_dir=tmp_path,
        ssh_config=config,
        preliminary_results=preliminary_results,
        continue_past_limit=True,
        limit=1000,
        sub_queue=sub_queue,
        hit_limit=False,
    )
    assert sub_queue == [opt_job_path]
    assert opt_jobs == {
        non_existent_path: OptJob(JobStatus.NOT_FOUND, 0, 0),
        opt_job_path: OptJob(JobStatus.INCOMPLETE, 0, 0),
    }
    assert dos_jobs == {
        opt_job_path: DosJob(-1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 0, 0)
    }
    assert wav_jobs == {opt_job_path: WavJob(-1, JobStatus.INCOMPLETE, 0)}


def test_register_no_jobs(tmp_path):
    cwd = os.getcwd()
    opt_jobs = {}
    dos_jobs = {}
    wav_jobs = {}
    config = SSHConfig("NoSSH")
    preliminary_results_path = os.path.join(tmp_path, "preliminary_results")
    preliminary_results = open(preliminary_results_path, "w")
    sub_queue = []
    home_directory = os.path.join(tmp_path, "fake_home")
    os.chdir(tmp_path)
    register(
        opt_jobs,
        dos_jobs,
        wav_jobs,
        0,
        False,
        home_directory,
        config,
        preliminary_results,
        False,
        1000,
        sub_queue,
        False,
    )
    os.chdir(cwd)
    assert sub_queue == []
    assert opt_jobs == {}
    assert dos_jobs == {}
    assert wav_jobs == {}


def test_register_jobs(tmp_path):
    cwd = os.getcwd()
    opt_jobs = {}
    dos_jobs = {}
    wav_jobs = {}
    config = SSHConfig("NoSSH")
    preliminary_results_path = os.path.join(tmp_path, "preliminary_results")
    preliminary_results = open(preliminary_results_path, "w")
    sub_queue = []
    home_directory = os.path.join(tmp_path, "home")
    opt_job_1 = os.path.join(tmp_path, "opt_job_1")
    shutil.copytree("test/test_files/h2", opt_job_1)
    os.mkdir(os.path.join(opt_job_1, "dos"))
    os.chdir(tmp_path)
    register(
        opt_jobs,
        dos_jobs,
        wav_jobs,
        0,
        False,
        home_directory,
        config,
        preliminary_results,
        False,
        1000,
        sub_queue,
        False,
    )
    os.chdir(cwd)
    assert sub_queue == [opt_job_1]
    assert opt_jobs == {opt_job_1: OptJob(JobStatus.INCOMPLETE, 0, 0)}
    assert dos_jobs == {}
    assert wav_jobs == {}


def test_register_jobs_converged(tmp_path):
    cwd = os.getcwd()
    opt_jobs = {}
    dos_jobs = {}
    wav_jobs = {}
    config = SSHConfig("NoSSH")
    preliminary_results_path = os.path.join(tmp_path, "preliminary_results")
    preliminary_results = open(preliminary_results_path, "w")
    sub_queue = []
    home_directory = os.path.join(tmp_path, "home")
    opt_job_1 = os.path.join(tmp_path, "opt_job_1")
    shutil.copytree("test/test_files/h2_completed_run", opt_job_1)
    os.mkdir(os.path.join(opt_job_1, "dos"))
    os.chdir(tmp_path)
    register(
        opt_jobs,
        dos_jobs,
        wav_jobs,
        0,
        False,
        home_directory,
        config,
        preliminary_results,
        False,
        1000,
        sub_queue,
        False,
    )
    os.chdir(cwd)
    assert sub_queue == []
    assert opt_jobs == {opt_job_1: OptJob(JobStatus.CONVERGED, 0, 0)}
    assert dos_jobs == {}
    assert wav_jobs == {}


def test_register_empty_note(tmp_path):
    cwd = os.getcwd()
    opt_jobs = {}
    dos_jobs = {}
    wav_jobs = {}
    config = SSHConfig("NoSSH")
    preliminary_results_path = os.path.join(tmp_path, "preliminary_results")
    preliminary_results = open(preliminary_results_path, "w")
    sub_queue = []
    home_directory = os.path.join(tmp_path, "home")
    opt_job_1 = os.path.join(tmp_path, "opt_job_1")
    shutil.copytree("test/test_files/h2_completed_run", opt_job_1)
    with open(os.path.join(opt_job_1, "automagic_note"), "w+"):
        pass
    os.mkdir(os.path.join(opt_job_1, "dos"))
    os.chdir(tmp_path)
    register(
        opt_jobs,
        dos_jobs,
        wav_jobs,
        0,
        False,
        home_directory,
        config,
        preliminary_results,
        False,
        1000,
        sub_queue,
        False,
    )
    os.chdir(cwd)
    assert sub_queue == []
    assert opt_jobs == {opt_job_1: OptJob(JobStatus.CONVERGED, 0, 0)}
    assert dos_jobs == {}
    assert wav_jobs == {}


def test_register_dos_note(tmp_path):
    cwd = os.getcwd()
    opt_jobs = {}
    dos_jobs = {}
    wav_jobs = {}
    config = SSHConfig("NoSSH")
    preliminary_results_path = os.path.join(tmp_path, "preliminary_results")
    preliminary_results = open(preliminary_results_path, "w")
    sub_queue = []
    home_directory = os.path.join(tmp_path, "home")
    opt_job_1 = os.path.join(tmp_path, "opt_job_1")
    shutil.copytree("test/test_files/h2_completed_run", opt_job_1)
    with open(os.path.join(opt_job_1, "automagic_note"), "w+") as f:
        f.write("dos\n")
    os.mkdir(os.path.join(opt_job_1, "dos"))
    os.chdir(tmp_path)
    register(
        opt_jobs,
        dos_jobs,
        wav_jobs,
        0,
        False,
        home_directory,
        config,
        preliminary_results,
        False,
        1000,
        sub_queue,
        False,
    )
    os.chdir(cwd)
    assert sub_queue == [os.path.normpath(os.path.join(opt_job_1, "../sc"))]
    assert opt_jobs == {opt_job_1: OptJob(JobStatus.CONVERGED, 0, 0)}
    assert dos_jobs == {
        opt_job_1: DosJob(-1, JobStatus.RUNNING, JobStatus.INCOMPLETE, 0, 0)
    }
    assert wav_jobs == {}


def test_register_wav_note(tmp_path):
    cwd = os.getcwd()
    opt_jobs = {}
    dos_jobs = {}
    wav_jobs = {}
    config = SSHConfig("NoSSH")
    preliminary_results_path = os.path.join(tmp_path, "preliminary_results")
    preliminary_results = open(preliminary_results_path, "w")
    sub_queue = []
    home_directory = os.path.join(tmp_path, "home")
    opt_job_1 = os.path.join(tmp_path, "opt_job_1")
    shutil.copytree("test/test_files/h2_completed_run", opt_job_1)
    with open(os.path.join(opt_job_1, "automagic_note"), "w+") as f:
        f.write("wav\n")
    os.mkdir(os.path.join(opt_job_1, "dos"))
    os.chdir(tmp_path)
    register(
        opt_jobs,
        dos_jobs,
        wav_jobs,
        0,
        False,
        home_directory,
        config,
        preliminary_results,
        False,
        1000,
        sub_queue,
        False,
    )
    os.chdir(cwd)
    assert sub_queue == [os.path.normpath(os.path.join(opt_job_1, "../wav"))]
    assert opt_jobs == {opt_job_1: OptJob(JobStatus.CONVERGED, 0, 0)}
    assert dos_jobs == {}
    assert wav_jobs == {opt_job_1: WavJob(-1, JobStatus.RUNNING, 0)}


def test_register_dos_and_wav_note(tmp_path):
    cwd = os.getcwd()
    opt_jobs = {}
    dos_jobs = {}
    wav_jobs = {}
    config = SSHConfig("NoSSH")
    preliminary_results_path = os.path.join(tmp_path, "preliminary_results")
    preliminary_results = open(preliminary_results_path, "w")
    sub_queue = []
    home_directory = os.path.join(tmp_path, "home")
    opt_job_1 = os.path.join(tmp_path, "opt_job_1")
    shutil.copytree("test/test_files/h2_completed_run", opt_job_1)
    with open(os.path.join(opt_job_1, "automagic_note"), "w+") as f:
        f.write("wav\n")
        f.write("dos\n")
    os.mkdir(os.path.join(opt_job_1, "dos"))
    os.chdir(tmp_path)
    register(
        opt_jobs,
        dos_jobs,
        wav_jobs,
        0,
        False,
        home_directory,
        config,
        preliminary_results,
        False,
        1000,
        sub_queue,
        False,
    )
    os.chdir(cwd)
    assert sub_queue == [os.path.normpath(os.path.join(opt_job_1, "../wav"))]
    assert opt_jobs == {opt_job_1: OptJob(JobStatus.CONVERGED, 0, 0)}
    assert dos_jobs == {}
    assert wav_jobs == {opt_job_1: WavJob(-1, JobStatus.RUNNING, 0)}


def test_register_exclude_note(tmp_path):
    cwd = os.getcwd()
    opt_jobs = {}
    dos_jobs = {}
    wav_jobs = {}
    config = SSHConfig("NoSSH")
    preliminary_results_path = os.path.join(tmp_path, "preliminary_results")
    preliminary_results = open(preliminary_results_path, "w")
    sub_queue = []
    home_directory = os.path.join(tmp_path, "home")
    opt_job_1 = os.path.join(tmp_path, "opt_job_1")
    shutil.copytree("test/test_files/h2_completed_run", opt_job_1)
    with open(os.path.join(opt_job_1, "automagic_note"), "w+") as f:
        f.write("exclude\n")
    os.mkdir(os.path.join(opt_job_1, "dos"))
    os.mkdir(os.path.join(opt_job_1, "wav"))
    os.chdir(tmp_path)
    register(
        opt_jobs,
        dos_jobs,
        wav_jobs,
        0,
        False,
        home_directory,
        config,
        preliminary_results,
        False,
        1000,
        sub_queue,
        False,
    )
    os.chdir(cwd)
    assert sub_queue == []
    assert opt_jobs == {}
    assert dos_jobs == {}
    assert wav_jobs == {}


def test_register_neb(tmp_path):
    cwd = os.getcwd()
    opt_jobs = {}
    dos_jobs = {}
    wav_jobs = {}
    config = SSHConfig("NoSSH")
    preliminary_results_path = os.path.join(tmp_path, "preliminary_results")
    preliminary_results = open(preliminary_results_path, "w")
    sub_queue = []
    home_directory = os.path.join(tmp_path, "home")
    opt_job_1 = os.path.join(tmp_path, "opt_job_1")
    shutil.copytree("test/test_files/h2", opt_job_1)
    os.mkdir(os.path.join(opt_job_1, "band"))
    os.mkdir(os.path.join(opt_job_1, "ini"))
    os.mkdir(os.path.join(opt_job_1, "fin"))
    os.chdir(tmp_path)
    register(
        opt_jobs,
        dos_jobs,
        wav_jobs,
        0,
        False,
        home_directory,
        config,
        preliminary_results,
        False,
        1000,
        sub_queue,
        False,
    )
    os.chdir(cwd)
    assert sub_queue == []
    assert opt_jobs == {}
    assert dos_jobs == {}
    assert wav_jobs == {}


def test_register_neb_captalized(tmp_path):
    cwd = os.getcwd()
    opt_jobs = {}
    dos_jobs = {}
    wav_jobs = {}
    config = SSHConfig("NoSSH")
    preliminary_results_path = os.path.join(tmp_path, "preliminary_results")
    preliminary_results = open(preliminary_results_path, "w")
    sub_queue = []
    home_directory = os.path.join(tmp_path, "home")
    opt_job_1 = os.path.join(tmp_path, "opt_job_1")
    shutil.copytree("test/test_files/h2", opt_job_1)
    os.mkdir(os.path.join(opt_job_1, "BAND"))
    os.mkdir(os.path.join(opt_job_1, "INI"))
    os.mkdir(os.path.join(opt_job_1, "FIN"))
    os.chdir(tmp_path)
    register(
        opt_jobs,
        dos_jobs,
        wav_jobs,
        0,
        False,
        home_directory,
        config,
        preliminary_results,
        False,
        1000,
        sub_queue,
        False,
    )
    os.chdir(cwd)
    assert sub_queue == []
    assert opt_jobs == {}
    assert dos_jobs == {}
    assert wav_jobs == {}


def test_exclude_regex_no_invalid():
    assert exclude_regex("/home/jw53939") is False


def test_exclude_regex_no_invalid_2():
    assert exclude_regex("/home/jw53939/lab1") is False


def test_exclude_regex_run_num():
    assert exclude_regex("/home/jw53939/lab1/h2/run0") is True


def test_exclude_regex_run_no_num():
    assert exclude_regex("/home/jw53939/lab1/h2/run") is True


def test_exclude_regex_run_long_num():
    assert exclude_regex("/home/jw53939/lab1/h2/run1234567") is True


def test_exclude_regex_run_substring():
    assert exclude_regex("/home/jw53939/lab1/h2run/job") is False


def test_exclude_regex_run_not_final_dir():
    assert exclude_regex("/home/jw53939/lab1/run9/job") is True


def test_exclude_regex_dos():
    assert exclude_regex("/home/jw53939/lab1/dos") is True


def test_exclude_regex_wav():
    assert exclude_regex("/home/jw53939/lab1/wav") is True


def test_exclude_regex_sc():
    assert exclude_regex("/home/jw53939/lab1/sc") is True


def test_exclude_regex_band():
    assert exclude_regex("/home/jw53939/lab1/Ini") is True
    assert exclude_regex("/home/jw53939/lab1/INI") is False
    assert exclude_regex("/home/jw53939/lab1/ini") is True
    assert exclude_regex("/home/jw53939/lab1/Fin") is True
    assert exclude_regex("/home/jw53939/lab1/fin") is True
    assert exclude_regex("/home/jw53939/lab1/FIN") is False


def test_exclude_regex_home():
    assert exclude_regex("/home") is False
import os

import pytest

import automagician.classes
from automagician.classes import DosJob, GoneJob, JobStatus, Machine, OptJob, WavJob
from automagician.database import Database


def test_db_init_empty_file(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    print("first db init sucessfull")
    names = database.db.execute("select name from sqlite_master where type='table'")
    assert check_db_tables(names)
    # Test to see if something was present if it gets overwriten
    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        ("/tmp", automagician.classes.JobStatus.CONVERGED.value, 0, 0),
    )
    database.db.connection.commit()
    database = Database(database_path)

    print("Second db init passed")
    names = database.db.execute("select name from sqlite_master where type='table'")
    assert check_db_tables(names)
    opt_jobs = database.db.execute("SELECT * from opt_jobs").fetchall()
    assert len(opt_jobs) == 1
    # Test to see that data was the same
    job = opt_jobs[0]
    assert job[0] == "/tmp"
    assert job[1] == automagician.classes.JobStatus.CONVERGED.value
    assert job[2] == 0
    assert job[3] == 0

    database.db.execute("DROP TABLE dos_jobs")
    database = Database(database_path)
    names = database.db.execute("select name from sqlite_master where type='table'")
    assert check_db_tables(names)

    opt_jobs = database.db.execute("SELECT * from opt_jobs").fetchall()
    assert len(opt_jobs) == 1
    # Test to see that data was the same
    job = opt_jobs[0]
    assert job[0] == "/tmp"
    assert job[1] == automagician.classes.JobStatus.CONVERGED.value
    assert job[2] == 0
    assert job[3] == 0
    database.db.connection.commit()


def test_get_string_from_db_names(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    names = database.get_string_from_db(
        "select name from sqlite_master where type='table'"
    )

    assert names == "opt_jobs"


def test_get_string_from_db_invalid_command(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    with pytest.raises(Exception):
        database.get_string_from_db(
            "select nothing from sqlite_master where type='table'"
        )


def test_get_string_from_db_none(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    names = database.get_string_from_db("SELECT * from opt_jobs")

    assert names == ""


def test_delpwd_remove_one(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        ("/tmp", automagician.classes.JobStatus.CONVERGED.value, 2, 4),
    )
    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        ("/tmp/automagician", automagician.classes.JobStatus.NOT_FOUND.value, 1, 3),
    )
    database.db.connection.commit()
    database.delpwd("/tmp")
    entries = database.db.execute("select * from opt_jobs").fetchall()
    print(entries)
    assert len(entries) == 1
    assert entries[0][0] == "/tmp/automagician"
    assert entries[0][1] == automagician.classes.JobStatus.NOT_FOUND.value
    assert entries[0][2] == 1
    assert entries[0][3] == 3


def test_delpwd_remove_none(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        ("/tmp/automagician", automagician.classes.JobStatus.NOT_FOUND.value, 1, 3),
    )
    database.db.connection.commit()
    database.delpwd("/nonsense")
    entries = database.db.execute("select * from opt_jobs").fetchall()
    print(entries)
    assert len(entries) == 1
    assert entries[0][0] == "/tmp/automagician"
    assert entries[0][1] == automagician.classes.JobStatus.NOT_FOUND.value
    assert entries[0][2] == 1
    assert entries[0][3] == 3


def test_delpwd_remove_all(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        ("/tmp", automagician.classes.JobStatus.CONVERGED.value, 2, 4),
    )
    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        ("/tmp", automagician.classes.JobStatus.NOT_FOUND.value, 1, 3),
    )
    database.db.connection.commit()
    database.delpwd("/tmp")
    entries = database.db.execute("select * from opt_jobs").fetchall()
    print(entries)
    assert len(entries) == 0


def test_write_plain_text_db_with_1_value(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    opt_job_1 = OptJob(JobStatus.NOT_FOUND, Machine.FRI, Machine.FRI)
    database.add_opt_job_to_db(opt_job_1, "/tmp/hi")
    plain_text_path = os.path.join(tmp_path, "plain_text_db")
    database.write_plain_text_db(plain_text_path)

    assert os.path.exists(plain_text_path)
    with open(plain_text_path, "r") as f:
        lines = f.readlines()
        assert lines[0] == "OPT JOBS\n"
        assert lines[1] == "   status    | home machine |    last on    | job dir\n"
        assert lines[2] == "-------------|--------------|---------------|--------\n"
        assert lines[3] == "NOT_FOUND    |FRI           |FRI            |/tmp/hi\n"
        assert len(lines) == 4


def test_write_plain_text_db_with_multiple_opt(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    opt_jobs = {
        "/tmp/job1": OptJob(
            JobStatus.NOT_FOUND, Machine.FRI, Machine.FRONTERA_TACC
        ),
        "/tmp/job2": OptJob(
            JobStatus.CONVERGED, Machine.HALIFAX, Machine.LS6_TACC
        ),
        "/tmp/job3": OptJob(
            JobStatus.ERROR, Machine.STAMPEDE2_TACC, Machine.STAMPEDE2_TACC
        ),
        "/tmp/job4": OptJob(JobStatus.RUNNING, Machine.LS6_TACC, Machine.HALIFAX),
        "/tmp/job5": OptJob(
            JobStatus.INCOMPLETE, Machine.FRONTERA_TACC, Machine.FRI
        ),
    }
    database.write_job_statuses(opt_jobs, {}, {})

    plain_text_path = os.path.join(tmp_path, "plain_text_db")
    database.write_plain_text_db(plain_text_path)

    assert os.path.exists(plain_text_path)
    with open(plain_text_path, "r") as f:
        lines = f.readlines()
        assert lines[0] == "OPT JOBS\n"
        assert lines[1] == "   status    | home machine |    last on    | job dir\n"
        assert lines[2] == "-------------|--------------|---------------|--------\n"
        assert lines[3] == "NOT_FOUND    |FRI           |FRONTERA_TACC  |/tmp/job1\n"
        assert lines[4] == "CONVERGED    |HALIFAX       |LS6_TACC       |/tmp/job2\n"
        assert lines[5] == "ERROR        |STAMPEDE2_TACC|STAMPEDE2_TACC |/tmp/job3\n"
        assert lines[6] == "RUNNING      |LS6_TACC      |HALIFAX        |/tmp/job4\n"
        assert lines[7] == "INCOMPLETE   |FRONTERA_TACC |FRI            |/tmp/job5\n"
        assert len(lines) == 8


def test_write_plain_text_db_with_multiple_opt_dos_wav(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    opt_jobs = {
        "/tmp/job1": OptJob(
            JobStatus.NOT_FOUND, Machine.FRI, Machine.FRONTERA_TACC
        ),
        "/tmp/job2": OptJob(
            JobStatus.CONVERGED, Machine.HALIFAX, Machine.LS6_TACC
        ),
        "/tmp/job3": OptJob(
            JobStatus.ERROR, Machine.STAMPEDE2_TACC, Machine.STAMPEDE2_TACC
        ),
        "/tmp/job4": OptJob(JobStatus.RUNNING, Machine.LS6_TACC, Machine.HALIFAX),
        "/tmp/job5": OptJob(
            JobStatus.INCOMPLETE, Machine.FRONTERA_TACC, Machine.FRI
        ),
    }
    dos_jobs = {
        "/tmp/job1": DosJob(
            -1,
            JobStatus.NOT_FOUND,
            JobStatus.INCOMPLETE,
            Machine.FRI,
            Machine.FRONTERA_TACC,
        ),
        "/tmp/job2": DosJob(
            -1,
            JobStatus.CONVERGED,
            JobStatus.RUNNING,
            Machine.HALIFAX,
            Machine.LS6_TACC,
        ),
        "/tmp/job3": DosJob(
            -1,
            JobStatus.ERROR,
            JobStatus.ERROR,
            Machine.STAMPEDE2_TACC,
            Machine.STAMPEDE2_TACC,
        ),
        "/tmp/job4": DosJob(
            -1,
            JobStatus.RUNNING,
            JobStatus.CONVERGED,
            Machine.LS6_TACC,
            Machine.HALIFAX,
        ),
        "/tmp/job5": DosJob(
            -1,
            JobStatus.INCOMPLETE,
            JobStatus.NOT_FOUND,
            Machine.FRONTERA_TACC,
            Machine.FRI,
        ),
    }
    wav_jobs = {
        "/tmp/job1": WavJob(-1, JobStatus.NOT_FOUND, Machine.FRI),
        "/tmp/job2": WavJob(-1, JobStatus.CONVERGED, Machine.HALIFAX),
        "/tmp/job3": WavJob(-1, JobStatus.ERROR, Machine.STAMPEDE2_TACC),
        "/tmp/job4": WavJob(-1, JobStatus.RUNNING, Machine.LS6_TACC),
        "/tmp/job5": WavJob(-1, JobStatus.INCOMPLETE, Machine.FRONTERA_TACC),
    }
    database.write_job_statuses(opt_jobs, dos_jobs, wav_jobs)

    plain_text_path = os.path.join(tmp_path, "plain_text_db")
    database.write_plain_text_db(plain_text_path)

    assert os.path.exists(plain_text_path)
    with open(plain_text_path, "r") as f:
        lines = f.readlines()
        assert lines[0] == "OPT JOBS\n"
        assert lines[1] == "   status    | home machine |    last on    | job dir\n"
        assert lines[2] == "-------------|--------------|---------------|--------\n"
        assert lines[3] == "NOT_FOUND    |FRI           |FRONTERA_TACC  |/tmp/job1\n"
        assert lines[4] == "CONVERGED    |HALIFAX       |LS6_TACC       |/tmp/job2\n"
        assert lines[5] == "ERROR        |STAMPEDE2_TACC|STAMPEDE2_TACC |/tmp/job3\n"
        assert lines[6] == "RUNNING      |LS6_TACC      |HALIFAX        |/tmp/job4\n"
        assert lines[7] == "INCOMPLETE   |FRONTERA_TACC |FRI            |/tmp/job5\n"
        assert lines[8] == "DOS JOBS\n"
        assert (
            lines[9]
            == "  sc status  |  dos status |  sc last on  |  dos last on  | job dir\n"
        )
        assert (
            lines[10]
            == "-------------|-------------|--------------|---------------|--------\n"
        )
        assert (
            lines[11]
            == "NOT_FOUND    |INCOMPLETE   |FRI           |FRONTERA_TACC  |/tmp/job1/dos\n"
        )
        assert (
            lines[12]
            == "CONVERGED    |RUNNING      |HALIFAX       |LS6_TACC       |/tmp/job2/dos\n"
        )
        assert (
            lines[13]
            == "ERROR        |ERROR        |STAMPEDE2_TACC|STAMPEDE2_TACC |/tmp/job3/dos\n"
        )
        assert (
            lines[14]
            == "RUNNING      |CONVERGED    |LS6_TACC      |HALIFAX        |/tmp/job4/dos\n"
        )
        assert (
            lines[15]
            == "INCOMPLETE   |NOT_FOUND    |FRONTERA_TACC |FRI            |/tmp/job5/dos\n"
        )
        assert lines[16] == "WAV JOBS\n"
        assert lines[17] == "  wav status  |  wav last on  | job dir\n"
        assert lines[18] == "--------------|---------------|--------\n"
        assert lines[19] == "NOT_FOUND     |FRI            |/tmp/job1/wav\n"
        assert lines[20] == "CONVERGED     |HALIFAX        |/tmp/job2/wav\n"
        assert lines[21] == "ERROR         |STAMPEDE2_TACC |/tmp/job3/wav\n"
        assert lines[22] == "RUNNING       |LS6_TACC       |/tmp/job4/wav\n"
        assert lines[23] == "INCOMPLETE    |FRONTERA_TACC  |/tmp/job5/wav\n"
        assert len(lines) == 24


def test_write_plain_text_db_empty(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    plain_text_path = os.path.join(tmp_path, "plain_text_db")
    database.write_plain_text_db(plain_text_path)
    assert os.path.exists(plain_text_path)
    assert os.path.exists(plain_text_path)
    with open(plain_text_path, "r") as f:
        lines = f.readlines()
        assert lines[0] == "NO OPT JOBS FOUND\n"
        assert len(lines) == 1


def test_get_opt_jobs(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        (
            "/tmp",
            JobStatus.CONVERGED.value,
            Machine.FRI.value,
            Machine.FRONTERA_TACC.value,
        ),
    )
    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        (
            "/hi",
            JobStatus.NOT_FOUND.value,
            Machine.HALIFAX.value,
            Machine.LS6_TACC.value,
        ),
    )

    database.db.execute(
        "INSERT into dos_jobs values (?,?,?,?, ?)",
        (
            "/hi",
            -1,
            JobStatus.NOT_FOUND.value,
            Machine.STAMPEDE2_TACC.value,
            Machine.FRI.value,
        ),
    )
    database.db.connection.commit()

    result = database.get_opt_jobs()
    assert result == {
        "/tmp": OptJob(
            status=JobStatus.CONVERGED,
            home_machine=Machine.FRI,
            last_on=Machine.FRONTERA_TACC,
        ),
        "/hi": OptJob(
            status=JobStatus.NOT_FOUND,
            home_machine=Machine.HALIFAX,
            last_on=Machine.LS6_TACC,
        ),
    }


def test_get_dos_jobs(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        ("/tmp", JobStatus.CONVERGED.value, Machine.FRI, Machine.HALIFAX),
    )

    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        (
            "/tmp/hi",
            JobStatus.RUNNING.value,
            Machine.STAMPEDE2_TACC,
            Machine.FRONTERA_TACC,
        ),
    )
    database.db.connection.commit()
    opt_id_1 = int(
        database.get_string_from_db(
            'select rowid from opt_jobs where dir = "' + "/tmp" + '"'
        )
    )
    opt_id_2 = int(
        database.get_string_from_db(
            'select rowid from opt_jobs where dir = "' + "/tmp/hi" + '"'
        )
    )

    database.db.execute(
        "INSERT into dos_jobs values (?,?,?,?,?)",
        (
            opt_id_1,
            JobStatus.CONVERGED.value,
            JobStatus.NOT_FOUND.value,
            Machine.LS6_TACC,
            Machine.FRI,
        ),
    )

    database.db.execute(
        "INSERT into dos_jobs values (?,?,?,?,?)",
        (
            opt_id_2,
            JobStatus.RUNNING.value,
            JobStatus.CONVERGED.value,
            Machine.HALIFAX,
            Machine.FRONTERA_TACC,
        ),
    )
    database.db.connection.commit()

    result = database.get_dos_jobs()
    assert result == {
        "/tmp/dos": DosJob(
            opt_id=opt_id_1,
            sc_status=JobStatus.CONVERGED,
            dos_status=JobStatus.NOT_FOUND,
            sc_last_on=Machine.LS6_TACC,
            dos_last_on=Machine.FRI,
        ),
        "/tmp/hi/dos": DosJob(
            opt_id=opt_id_2,
            sc_status=JobStatus.RUNNING,
            dos_status=JobStatus.CONVERGED,
            sc_last_on=Machine.HALIFAX,
            dos_last_on=Machine.FRONTERA_TACC,
        ),
    }


def test_get_wav_jobs(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        ("/tmp", JobStatus.CONVERGED.value, Machine.FRI, Machine.HALIFAX),
    )

    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        ("/tmp/hi", JobStatus.RUNNING.value, Machine.FRI, Machine.HALIFAX),
    )
    database.db.connection.commit()
    opt_id_1 = int(
        database.get_string_from_db(
            'select rowid from opt_jobs where dir = "' + "/tmp" + '"'
        )
    )
    opt_id_2 = int(
        database.get_string_from_db(
            'select rowid from opt_jobs where dir = "' + "/tmp/hi" + '"'
        )
    )

    database.db.execute(
        "INSERT into wav_jobs values (?,?,?)",
        (opt_id_1, JobStatus.CONVERGED.value, Machine.FRI),
    )

    database.db.execute(
        "INSERT into wav_jobs values (?,?,?)",
        (opt_id_2, JobStatus.RUNNING.value, Machine.HALIFAX),
    )
    database.db.connection.commit()

    result = database.get_wav_jobs()
    assert result == {
        "/tmp/wav": WavJob(
            opt_id=opt_id_1,
            wav_status=JobStatus.CONVERGED,
            wav_last_on=Machine.FRI,
        ),
        "/tmp/hi/wav": WavJob(
            opt_id=opt_id_2,
            wav_status=JobStatus.RUNNING,
            wav_last_on=Machine.HALIFAX,
        ),
    }


def test_write_job_status_dos_and_wav(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)

    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        (
            "/home/jw53959/opt_job_1",
            JobStatus.RUNNING.value,
            Machine.FRI,
            Machine.HALIFAX,
        ),
    )

    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        (
            "/home/jw53959/opt_job_2",
            JobStatus.CONVERGED.value,
            Machine.HALIFAX,
            Machine.STAMPEDE2_TACC,
        ),
    )

    database.db.connection.commit()

    opt_id_1 = int(
        database.get_string_from_db(
            'select rowid from opt_jobs where dir = "' + "/home/jw53959/opt_job_1" + '"'
        )
    )
    opt_id_2 = int(
        database.get_string_from_db(
            'select rowid from opt_jobs where dir = "' + "/home/jw53959/opt_job_2" + '"'
        )
    )

    opt_jobs = {
        "/home/jw53959/opt_job_1": OptJob(
            JobStatus.RUNNING, Machine.FRI, Machine.HALIFAX
        ),
        "/home/jw53959/opt_job_2": OptJob(
            JobStatus.CONVERGED, Machine.HALIFAX, Machine.STAMPEDE2_TACC
        ),
    }
    dos_jobs = {
        "/home/jw53959/opt_job_1/dos": DosJob(
            opt_id_1,
            JobStatus.RUNNING,
            JobStatus.CONVERGED,
            Machine.FRI,
            Machine.HALIFAX,
        ),
        "/home/jw53959/opt_job_2/dos": DosJob(
            opt_id_2,
            JobStatus.RUNNING,
            JobStatus.CONVERGED,
            Machine.STAMPEDE2_TACC,
            Machine.LS6_TACC,
        ),
    }
    wav_jobs = {
        "/home/jw53959/opt_job_1/wav": WavJob(
            opt_id_1, JobStatus.ERROR, Machine.FRI
        ),
        "/home/jw53959/opt_job_2/wav": WavJob(
            opt_id_2, JobStatus.NOT_FOUND, Machine.HALIFAX
        ),
    }
    database.write_job_statuses(opt_jobs, dos_jobs, wav_jobs)

    assert opt_jobs == {
        "/home/jw53959/opt_job_1": OptJob(
            JobStatus.RUNNING, Machine.FRI, Machine.HALIFAX
        ),
        "/home/jw53959/opt_job_2": OptJob(
            JobStatus.CONVERGED, Machine.HALIFAX, Machine.STAMPEDE2_TACC
        ),
    }
    assert dos_jobs == {
        "/home/jw53959/opt_job_1/dos": DosJob(
            opt_id_1,
            JobStatus.RUNNING,
            JobStatus.CONVERGED,
            Machine.FRI,
            Machine.HALIFAX,
        ),
        "/home/jw53959/opt_job_2/dos": DosJob(
            opt_id_2,
            JobStatus.RUNNING,
            JobStatus.CONVERGED,
            Machine.STAMPEDE2_TACC,
            Machine.LS6_TACC,
        ),
    }

    assert wav_jobs == {
        "/home/jw53959/opt_job_1/wav": WavJob(
            opt_id_1, JobStatus.ERROR, Machine.FRI
        ),
        "/home/jw53959/opt_job_2/wav": WavJob(
            opt_id_2, JobStatus.NOT_FOUND, Machine.HALIFAX
        ),
    }

    assert database.get_opt_jobs() == {
        "/home/jw53959/opt_job_1": OptJob(
            JobStatus.RUNNING, Machine.FRI, Machine.HALIFAX
        ),
        "/home/jw53959/opt_job_2": OptJob(
            JobStatus.CONVERGED, Machine.HALIFAX, Machine.STAMPEDE2_TACC
        ),
    }

    assert database.get_dos_jobs() == {
        "/home/jw53959/opt_job_1/dos": DosJob(
            opt_id_1,
            JobStatus.RUNNING,
            JobStatus.CONVERGED,
            Machine.FRI,
            Machine.HALIFAX,
        ),
        "/home/jw53959/opt_job_2/dos": DosJob(
            opt_id_2,
            JobStatus.RUNNING,
            JobStatus.CONVERGED,
            Machine.STAMPEDE2_TACC,
            Machine.LS6_TACC,
        ),
    }

    assert database.get_wav_jobs() == {
        "/home/jw53959/opt_job_1/wav": WavJob(
            opt_id_1, JobStatus.ERROR, Machine.FRI
        ),
        "/home/jw53959/opt_job_2/wav": WavJob(
            opt_id_2, JobStatus.NOT_FOUND, Machine.HALIFAX
        ),
    }


def test_write_job_status_opt_no_id_dos_and_wav(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)

    opt_job_1 = OptJob(JobStatus.RUNNING, Machine.FRI, Machine.FRI)
    opt_job_2 = OptJob(JobStatus.CONVERGED, Machine.FRI, Machine.FRI)
    dos_job_1 = DosJob(
        -1,
        JobStatus.RUNNING,
        JobStatus.CONVERGED,
        Machine.FRONTERA_TACC,
        Machine.HALIFAX,
    )
    dos_job_2 = DosJob(
        -1,
        JobStatus.RUNNING,
        JobStatus.CONVERGED,
        Machine.LS6_TACC,
        Machine.STAMPEDE2_TACC,
    )
    wav_job_1 = WavJob(-1, JobStatus.ERROR, Machine.FRI)
    wav_job_2 = WavJob(-1, JobStatus.NOT_FOUND, Machine.FRONTERA_TACC)

    opt_jobs = {
        "/home/jw53959/opt_job_1": opt_job_1,
        "/home/jw53959/opt_job_2": opt_job_2,
    }
    dos_jobs = {
        "/home/jw53959/opt_job_1/dos": dos_job_1,
        "/home/jw53959/opt_job_2/dos": dos_job_2,
    }
    wav_jobs = {
        "/home/jw53959/opt_job_1/wav": wav_job_1,
        "/home/jw53959/opt_job_2/wav": wav_job_2,
    }
    database.write_job_statuses(opt_jobs, dos_jobs, wav_jobs)

    assert opt_jobs == {
        "/home/jw53959/opt_job_1": opt_job_1,
        "/home/jw53959/opt_job_2": opt_job_2,
    }
    assert dos_jobs == {
        "/home/jw53959/opt_job_1/dos": dos_job_1,
        "/home/jw53959/opt_job_2/dos": dos_job_2,
    }

    assert wav_jobs == {
        "/home/jw53959/opt_job_1/wav": wav_job_1,
        "/home/jw53959/opt_job_2/wav": wav_job_2,
    }

    assert database.get_opt_jobs() == {
        "/home/jw53959/opt_job_1": opt_job_1,
        "/home/jw53959/opt_job_2": opt_job_2,
    }

    assert database.get_dos_jobs() == {
        "/home/jw53959/opt_job_1/dos": dos_job_1,
        "/home/jw53959/opt_job_2/dos": dos_job_2,
    }

    assert database.get_wav_jobs() == {
        "/home/jw53959/opt_job_1/wav": wav_job_1,
        "/home/jw53959/opt_job_2/wav": wav_job_2,
    }


def test_write_job_status_update_all(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)

    opt_job_1 = OptJob(JobStatus.RUNNING, Machine.FRI, Machine.HALIFAX)
    opt_job_2 = OptJob(JobStatus.CONVERGED, Machine.FRI, Machine.FRONTERA_TACC)

    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        (
            "/home/jw53959/opt_job_1",
            JobStatus.RUNNING.value,
            Machine.FRI,
            Machine.HALIFAX,
        ),
    )

    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        (
            "/home/jw53959/opt_job_2",
            JobStatus.CONVERGED.value,
            Machine.FRI,
            Machine.FRONTERA_TACC,
        ),
    )

    database.db.connection.commit()

    opt_id_1 = int(
        database.get_string_from_db(
            'select rowid from opt_jobs where dir = "' + "/home/jw53959/opt_job_1" + '"'
        )
    )
    opt_id_2 = int(
        database.get_string_from_db(
            'select rowid from opt_jobs where dir = "' + "/home/jw53959/opt_job_2" + '"'
        )
    )

    dos_job_1 = DosJob(
        opt_id_1,
        JobStatus.RUNNING,
        JobStatus.CONVERGED,
        Machine.FRI,
        Machine.HALIFAX,
    )
    dos_job_2 = DosJob(
        opt_id_2,
        JobStatus.RUNNING,
        JobStatus.CONVERGED,
        Machine.FRI,
        Machine.FRONTERA_TACC,
    )
    wav_job_1 = WavJob(opt_id_1, JobStatus.ERROR, Machine.LS6_TACC)
    wav_job_2 = WavJob(opt_id_2, JobStatus.NOT_FOUND, Machine.STAMPEDE2_TACC)

    opt_jobs = {
        "/home/jw53959/opt_job_1": opt_job_1,
        "/home/jw53959/opt_job_2": opt_job_2,
    }
    dos_jobs = {
        "/home/jw53959/opt_job_1/dos": dos_job_1,
        "/home/jw53959/opt_job_2/dos": dos_job_2,
    }
    wav_jobs = {
        "/home/jw53959/opt_job_1/wav": wav_job_1,
        "/home/jw53959/opt_job_2/wav": wav_job_2,
    }
    database.write_job_statuses(opt_jobs, dos_jobs, wav_jobs)

    opt_jobs["/home/jw53959/opt_job_1"] = OptJob(
        JobStatus.CONVERGED, Machine.FRONTERA_TACC, Machine.HALIFAX
    )
    dos_jobs["/home/jw53959/opt_job_1/dos"] = DosJob(
        opt_id_1,
        JobStatus.CONVERGED,
        JobStatus.RUNNING,
        Machine.STAMPEDE2_TACC,
        Machine.FRONTERA_TACC,
    )
    wav_jobs["/home/jw53959/opt_job_1/wav"] = WavJob(
        opt_id_1, JobStatus.RUNNING, Machine.FRI
    )

    database.write_job_statuses(opt_jobs, dos_jobs, wav_jobs)

    assert database.get_opt_jobs() == {
        "/home/jw53959/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRONTERA_TACC, Machine.HALIFAX
        ),
        "/home/jw53959/opt_job_2": opt_job_2,
    }

    assert database.get_dos_jobs() == {
        "/home/jw53959/opt_job_1/dos": DosJob(
            opt_id_1,
            JobStatus.CONVERGED,
            JobStatus.RUNNING,
            Machine.STAMPEDE2_TACC,
            Machine.FRONTERA_TACC,
        ),
        "/home/jw53959/opt_job_2/dos": dos_job_2,
    }

    assert database.get_wav_jobs() == {
        "/home/jw53959/opt_job_1/wav": WavJob(
            opt_id_1, JobStatus.RUNNING, Machine.FRI
        ),
        "/home/jw53959/opt_job_2/wav": wav_job_2,
    }


def test_write_job_status_no_opt(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)

    dos_job_1 = DosJob(
        -1, JobStatus.RUNNING, JobStatus.CONVERGED, Machine.FRI, Machine.FRI
    )
    dos_job_2 = DosJob(
        -1, JobStatus.RUNNING, JobStatus.CONVERGED, Machine.FRI, Machine.FRI
    )
    wav_job_1 = WavJob(-1, JobStatus.ERROR, Machine.FRI)
    wav_job_2 = WavJob(-1, JobStatus.NOT_FOUND, Machine.FRI)

    opt_jobs = {}
    dos_jobs = {
        "/home/jw53959/opt_job_1/dos": dos_job_1,
        "/home/jw53959/opt_job_2/dos": dos_job_2,
    }
    wav_jobs = {
        "/home/jw53959/opt_job_1/wav": wav_job_1,
        "/home/jw53959/opt_job_2/wav": wav_job_2,
    }
    database.write_job_statuses(opt_jobs, dos_jobs, wav_jobs)
    assert opt_jobs == {}
    assert dos_jobs == {
        "/home/jw53959/opt_job_1/dos": dos_job_1,
        "/home/jw53959/opt_job_2/dos": dos_job_2,
    }

    assert wav_jobs == {
        "/home/jw53959/opt_job_1/wav": wav_job_1,
        "/home/jw53959/opt_job_2/wav": wav_job_2,
    }

    assert database.get_opt_jobs() == {}

    assert database.get_dos_jobs() == {}

    assert database.get_wav_jobs() == {}


def test_add_opt_to_db_add(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    opt_job_1 = OptJob(JobStatus.CONVERGED, Machine.FRI, Machine.HALIFAX)
    opt_job_2 = OptJob(
        JobStatus.CONVERGED, Machine.STAMPEDE2_TACC, Machine.FRONTERA_TACC
    )

    database.add_opt_job_to_db(opt_job_1, "/tmp/opt_job_1", True)
    assert database.get_opt_jobs() == {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRI, Machine.HALIFAX
        )
    }
    database.add_opt_job_to_db(opt_job_2, "/tmp/opt_job_2", True)
    assert database.get_opt_jobs() == {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRI, Machine.HALIFAX
        ),
        "/tmp/opt_job_2": OptJob(
            JobStatus.CONVERGED, Machine.STAMPEDE2_TACC, Machine.FRONTERA_TACC
        ),
    }


def test_add_opt_to_db_update(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    opt_job_1 = OptJob(JobStatus.CONVERGED, Machine.FRI, Machine.HALIFAX)
    opt_job_2 = OptJob(
        JobStatus.CONVERGED, Machine.STAMPEDE2_TACC, Machine.FRONTERA_TACC
    )

    database.add_opt_job_to_db(opt_job_1, "/tmp/opt_job_1")
    database.add_opt_job_to_db(opt_job_2, "/tmp/opt_job_2", True)
    assert database.get_opt_jobs() == {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRI, Machine.HALIFAX
        ),
        "/tmp/opt_job_2": OptJob(
            JobStatus.CONVERGED, Machine.STAMPEDE2_TACC, Machine.FRONTERA_TACC
        ),
    }
    opt_job_2 = OptJob(JobStatus.NOT_FOUND, Machine.FRI, Machine.LS6_TACC)
    database.add_opt_job_to_db(opt_job_2, "/tmp/opt_job_2", True)
    assert database.get_opt_jobs() == {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRI, Machine.HALIFAX
        ),
        "/tmp/opt_job_2": OptJob(
            JobStatus.NOT_FOUND, Machine.FRI, Machine.LS6_TACC
        ),
    }

    assert database.get_dos_jobs() == {}
    assert database.get_wav_jobs() == {}


def test_add_dos_to_db_invalid(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    dos_job_1 = DosJob(
        -1, JobStatus.INCOMPLETE, JobStatus.ERROR, Machine.FRI, Machine.LS6_TACC
    )
    with pytest.raises(ValueError):
        database.add_dos_job_to_db(dos_job_1)

    assert database.get_opt_jobs() == {}
    assert database.get_dos_jobs() == {}
    assert database.get_wav_jobs() == {}


def test_add_dos_to_db_opt_dir_specified_invalid(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    dos_job_1 = DosJob(
        -1,
        JobStatus.INCOMPLETE,
        JobStatus.ERROR,
        Machine.FRI,
        Machine.HALIFAX,
    )
    with pytest.raises(ValueError):
        database.add_dos_job_to_db(dos_job_1, "/tmp/opt_job_1")

    assert database.get_opt_jobs() == {}
    assert database.get_dos_jobs() == {}
    assert database.get_wav_jobs() == {}


def test_add_dos_to_db_opt_dir_specified_valid(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    opt_job_1 = OptJob(JobStatus.CONVERGED, Machine.FRI, Machine.HALIFAX)
    database.add_opt_job_to_db(opt_job_1, "/tmp/opt_job_1", True)
    dos_job_1 = DosJob(
        -1,
        JobStatus.INCOMPLETE,
        JobStatus.ERROR,
        Machine.FRONTERA_TACC,
        Machine.STAMPEDE2_TACC,
    )
    database.add_dos_job_to_db(
        dos_job_1, "/tmp/opt_job_1", commit=True, add_opt_id=False
    )
    assert dos_job_1.opt_id == -1

    assert database.get_opt_jobs() == {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRI, Machine.HALIFAX
        )
    }
    dos_jobs = database.get_dos_jobs()
    assert len(dos_jobs) == 1
    dos_job_db = dos_jobs["/tmp/opt_job_1/dos"]

    opt_id = database.db.execute(
        "SELECT rowid from opt_jobs WHERE dir = ?", ["/tmp/opt_job_1"]
    ).fetchone()[0]
    assert dos_job_db.opt_id == opt_id
    assert dos_job_db.sc_status == JobStatus.INCOMPLETE
    assert dos_job_db.dos_status == JobStatus.ERROR
    assert dos_job_db.sc_last_on == Machine.FRONTERA_TACC
    assert dos_job_db.dos_last_on == Machine.STAMPEDE2_TACC
    assert database.get_wav_jobs() == {}


def test_add_dos_to_db_opt_dir_specified_add_opt_id(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    opt_job_1 = OptJob(JobStatus.CONVERGED, Machine.FRONTERA_TACC, Machine.FRI)
    database.add_opt_job_to_db(opt_job_1, "/tmp/opt_job_1", True)
    dos_job_1 = DosJob(
        -1,
        JobStatus.INCOMPLETE,
        JobStatus.ERROR,
        Machine.HALIFAX,
        Machine.FRI,
    )
    database.add_dos_job_to_db(dos_job_1, "/tmp/opt_job_1", commit=True)

    opt_id = database.db.execute(
        "SELECT rowid from opt_jobs WHERE dir = ?", ["/tmp/opt_job_1"]
    ).fetchone()[0]
    assert dos_job_1.opt_id == opt_id
    dos_id = dos_job_1.opt_id

    assert database.get_opt_jobs() == {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRONTERA_TACC, Machine.FRI
        )
    }
    assert database.get_dos_jobs() == {
        "/tmp/opt_job_1/dos": DosJob(
            dos_id,
            JobStatus.INCOMPLETE,
            JobStatus.ERROR,
            Machine.HALIFAX,
            Machine.FRI,
        )
    }
    assert database.get_wav_jobs() == {}


def test_add_dos_to_db_opt_id_specified_add(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    opt_job_1 = OptJob(JobStatus.CONVERGED, Machine.HALIFAX, Machine.FRI)
    database.add_opt_job_to_db(opt_job_1, "/tmp/opt_job_1", True)
    opt_id = database.db.execute(
        "SELECT rowid from opt_jobs WHERE dir = ?", ["/tmp/opt_job_1"]
    ).fetchone()[0]
    dos_job_1 = DosJob(
        opt_id,
        JobStatus.INCOMPLETE,
        JobStatus.ERROR,
        Machine.FRI,
        Machine.STAMPEDE2_TACC,
    )
    database.add_dos_job_to_db(dos_job_1, commit=True)

    opt_id = database.db.execute(
        "SELECT rowid from opt_jobs WHERE dir = ?", ["/tmp/opt_job_1"]
    ).fetchone()[0]
    assert dos_job_1.opt_id == opt_id
    dos_id = dos_job_1.opt_id

    assert database.get_opt_jobs() == {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.HALIFAX, Machine.FRI
        )
    }
    assert database.get_dos_jobs() == {
        "/tmp/opt_job_1/dos": DosJob(
            dos_id,
            JobStatus.INCOMPLETE,
            JobStatus.ERROR,
            Machine.FRI,
            Machine.STAMPEDE2_TACC,
        )
    }
    assert database.get_wav_jobs() == {}


def test_add_dos_to_db_opt_id_specified_update(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    opt_job_1 = OptJob(JobStatus.CONVERGED, Machine.FRI, Machine.STAMPEDE2_TACC)
    database.add_opt_job_to_db(opt_job_1, "/tmp/opt_job_1", True)
    opt_id = database.db.execute(
        "SELECT rowid from opt_jobs WHERE dir = ?", ["/tmp/opt_job_1"]
    ).fetchone()[0]
    dos_job_1 = DosJob(
        opt_id,
        JobStatus.INCOMPLETE,
        JobStatus.ERROR,
        Machine.FRI,
        Machine.STAMPEDE2_TACC,
    )
    database.add_dos_job_to_db(dos_job_1, commit=True)
    dos_job_2 = DosJob(
        opt_id,
        JobStatus.CONVERGED,
        JobStatus.CONVERGED,
        Machine.FRONTERA_TACC,
        Machine.FRI,
    )
    database.add_dos_job_to_db(dos_job_2)

    assert dos_job_1.opt_id == opt_id
    assert dos_job_2.opt_id == opt_id

    assert database.get_opt_jobs() == {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRI, Machine.STAMPEDE2_TACC
        )
    }
    assert database.get_dos_jobs() == {
        "/tmp/opt_job_1/dos": DosJob(
            opt_id,
            JobStatus.CONVERGED,
            JobStatus.CONVERGED,
            Machine.FRONTERA_TACC,
            Machine.FRI,
        )
    }
    assert database.get_wav_jobs() == {}


def test_add_wav_to_db_invalid(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    wav_job_1 = WavJob(-1, JobStatus.INCOMPLETE, Machine.FRI)
    with pytest.raises(ValueError):
        database.add_wav_job_to_db(wav_job_1)

    assert database.get_opt_jobs() == {}
    assert database.get_dos_jobs() == {}
    assert database.get_wav_jobs() == {}


def test_add_wav_to_db_opt_dir_specified_invalid(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    wav_job_1 = WavJob(-1, JobStatus.INCOMPLETE, Machine.FRONTERA_TACC)
    with pytest.raises(ValueError):
        database.add_wav_job_to_db(wav_job_1, "/tmp/opt_job_1")

    assert database.get_opt_jobs() == {}
    assert database.get_dos_jobs() == {}
    assert database.get_wav_jobs() == {}


def test_add_wav_to_db_opt_dir_specified_valid(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    opt_job_1 = OptJob(JobStatus.CONVERGED, Machine.FRONTERA_TACC, Machine.FRI)
    database.add_opt_job_to_db(opt_job_1, "/tmp/opt_job_1", True)
    wav_job_1 = WavJob(-1, JobStatus.INCOMPLETE, Machine.FRI)
    database.add_wav_job_to_db(
        wav_job_1, "/tmp/opt_job_1", commit=True, add_opt_id=False
    )
    assert wav_job_1.opt_id == -1

    assert database.get_opt_jobs() == {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRONTERA_TACC, Machine.FRI
        )
    }
    wav_jobs = database.get_wav_jobs()
    assert len(wav_jobs) == 1
    wav_job_db = wav_jobs["/tmp/opt_job_1/wav"]
    assert wav_job_1.opt_id == -1
    assert wav_job_db.wav_status == JobStatus.INCOMPLETE
    assert wav_job_db.wav_last_on == Machine.FRI
    assert database.get_dos_jobs() == {}


def test_add_wav_to_db_opt_dir_specified_add_opt_id(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    opt_job_1 = OptJob(
        JobStatus.CONVERGED, Machine.FRONTERA_TACC, Machine.HALIFAX
    )
    database.add_opt_job_to_db(opt_job_1, "/tmp/opt_job_1", True)
    wav_job_1 = WavJob(-1, JobStatus.INCOMPLETE, Machine.LS6_TACC)

    database.add_wav_job_to_db(wav_job_1, "/tmp/opt_job_1", commit=True)

    opt_id = database.db.execute(
        "SELECT rowid from opt_jobs WHERE dir = ?", ["/tmp/opt_job_1"]
    ).fetchone()[0]
    assert wav_job_1.opt_id == opt_id
    wav_id = wav_job_1.opt_id

    assert database.get_opt_jobs() == {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRONTERA_TACC, Machine.HALIFAX
        )
    }
    assert database.get_wav_jobs() == {
        "/tmp/opt_job_1/wav": WavJob(wav_id, JobStatus.INCOMPLETE, Machine.LS6_TACC)
    }
    assert database.get_dos_jobs() == {}


def test_add_wav_to_db_opt_id_specified_add(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    opt_job_1 = OptJob(JobStatus.CONVERGED, Machine.FRI, Machine.FRI)
    database.add_opt_job_to_db(opt_job_1, "/tmp/opt_job_1", True)
    opt_id = database.db.execute(
        "SELECT rowid from opt_jobs WHERE dir = ?", ["/tmp/opt_job_1"]
    ).fetchone()[0]
    print(opt_id)
    wav_job_1 = WavJob(opt_id, JobStatus.INCOMPLETE, Machine.FRONTERA_TACC)
    database.add_wav_job_to_db(wav_job_1, commit=True)
    assert wav_job_1.opt_id == opt_id

    assert database.get_opt_jobs() == {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRI, Machine.FRI
        )
    }
    assert database.get_wav_jobs() == {
        "/tmp/opt_job_1/wav": WavJob(
            opt_id, JobStatus.INCOMPLETE, Machine.FRONTERA_TACC
        )
    }
    assert database.get_dos_jobs() == {}


def test_add_wav_to_db_opt_id_specified_update(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    opt_job_1 = OptJob(JobStatus.CONVERGED, Machine.FRI, Machine.FRI)
    database.add_opt_job_to_db(opt_job_1, "/tmp/opt_job_1", True)
    opt_id = database.db.execute(
        "SELECT rowid from opt_jobs WHERE dir = ?", ["/tmp/opt_job_1"]
    ).fetchone()[0]
    wav_job_1 = WavJob(opt_id, JobStatus.INCOMPLETE, Machine.FRONTERA_TACC)
    database.add_wav_job_to_db(wav_job_1, commit=True)
    wav_job_2 = WavJob(opt_id, JobStatus.CONVERGED, Machine.FRI)
    database.add_wav_job_to_db(wav_job_2)
    assert wav_job_1.opt_id == opt_id
    assert wav_job_2.opt_id == opt_id

    assert database.get_opt_jobs() == {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRI, Machine.FRI
        )
    }
    assert database.get_wav_jobs() == {
        "/tmp/opt_job_1/wav": WavJob(opt_id, JobStatus.CONVERGED, Machine.FRI)
    }
    assert database.get_dos_jobs() == {}


def test_add_gone_to_db_add(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    gone_job_1 = GoneJob(
        "/tmp/opt_job_1", JobStatus.CONVERGED, Machine.FRONTERA_TACC, Machine.FRI
    )
    gone_job_2 = GoneJob(
        "/tmp/opt_job_2", JobStatus.CONVERGED, Machine.HALIFAX, Machine.LS6_TACC
    )

    database.add_gone_job_to_db(gone_job_1, True)
    assert database.get_gone_jobs() == {
        "/tmp/opt_job_1": GoneJob(
            "/tmp/opt_job_1",
            JobStatus.CONVERGED,
            Machine.FRONTERA_TACC,
            Machine.FRI,
        )
    }
    database.add_gone_job_to_db(gone_job_2, True)
    assert database.get_gone_jobs() == {
        "/tmp/opt_job_1": GoneJob(
            "/tmp/opt_job_1",
            JobStatus.CONVERGED,
            Machine.FRONTERA_TACC,
            Machine.FRI,
        ),
        "/tmp/opt_job_2": GoneJob(
            "/tmp/opt_job_2",
            JobStatus.CONVERGED,
            Machine.HALIFAX,
            Machine.LS6_TACC,
        ),
    }


def test_add_gone_to_db_update(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    gone_job_1 = GoneJob(
        "/tmp/opt_job_1", JobStatus.CONVERGED, Machine.FRONTERA_TACC, Machine.FRI
    )
    gone_job_2 = GoneJob(
        "/tmp/opt_job_2", JobStatus.CONVERGED, Machine.HALIFAX, Machine.LS6_TACC
    )

    database.add_gone_job_to_db(gone_job_1)
    database.add_gone_job_to_db(gone_job_2, True)
    assert database.get_opt_jobs() == {}
    assert database.get_gone_jobs() == {
        "/tmp/opt_job_1": GoneJob(
            "/tmp/opt_job_1",
            JobStatus.CONVERGED,
            Machine.FRONTERA_TACC,
            Machine.FRI,
        ),
        "/tmp/opt_job_2": GoneJob(
            "/tmp/opt_job_2",
            JobStatus.CONVERGED,
            Machine.HALIFAX,
            Machine.LS6_TACC,
        ),
    }
    gone_job_2 = GoneJob(
        "/tmp/opt_job_2", JobStatus.NOT_FOUND, Machine.FRI, Machine.HALIFAX
    )
    database.add_gone_job_to_db(gone_job_2, True)
    assert database.get_gone_jobs() == {
        "/tmp/opt_job_1": GoneJob(
            "/tmp/opt_job_1",
            JobStatus.CONVERGED,
            Machine.FRONTERA_TACC,
            Machine.FRI,
        ),
        "/tmp/opt_job_2": GoneJob(
            "/tmp/opt_job_2",
            JobStatus.NOT_FOUND,
            Machine.FRI,
            Machine.HALIFAX,
        ),
    }
    assert database.get_opt_jobs() == {}

    assert database.get_dos_jobs() == {}
    assert database.get_wav_jobs() == {}


def test_reset_job_status(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)

    opt_jobs = {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRI, Machine.LS6_TACC
        ),
        "/tmp/opt_job_2": OptJob(
            JobStatus.ERROR, Machine.HALIFAX, Machine.FRONTERA_TACC
        ),
        "/tmp/opt_job_3": OptJob(
            JobStatus.NOT_FOUND, Machine.STAMPEDE2_TACC, Machine.STAMPEDE2_TACC
        ),
        "/tmp/opt_job_4": OptJob(
            JobStatus.INCOMPLETE, Machine.FRONTERA_TACC, Machine.HALIFAX
        ),
        "/tmp/opt_job_5": OptJob(JobStatus.RUNNING, Machine.LS6_TACC, Machine.FRI),
    }
    dos_jobs = {}
    wav_jobs = {}
    database.write_job_statuses(opt_jobs=opt_jobs, dos_jobs=dos_jobs, wav_jobs=wav_jobs)

    database.reset_job_status()

    opt_jobs = {
        "/tmp/opt_job_1": OptJob(
            JobStatus.INCOMPLETE, Machine.FRI, Machine.LS6_TACC
        ),
        "/tmp/opt_job_2": OptJob(
            JobStatus.INCOMPLETE, Machine.HALIFAX, Machine.FRONTERA_TACC
        ),
        "/tmp/opt_job_3": OptJob(
            JobStatus.INCOMPLETE, Machine.STAMPEDE2_TACC, Machine.STAMPEDE2_TACC
        ),
        "/tmp/opt_job_4": OptJob(
            JobStatus.INCOMPLETE, Machine.FRONTERA_TACC, Machine.HALIFAX
        ),
        "/tmp/opt_job_5": OptJob(
            JobStatus.INCOMPLETE, Machine.LS6_TACC, Machine.FRI
        ),
    }


def check_db_tables(names: list[str]):
    tables = 0
    for name in names:
        trimmed_name = name[0]
        if trimmed_name == "opt_jobs":
            tables |= 1
        elif trimmed_name == "dos_jobs":
            tables |= 2
        elif trimmed_name == "wav_jobs":
            tables |= 4
        elif trimmed_name == "gone_jobs":
            tables |= 8
        elif trimmed_name == "insta_submit":
            tables |= 16
        else:
            tables |= 32
    return tables == 31
