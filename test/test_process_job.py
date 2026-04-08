# pylint: disable=all
import os
import pathlib
import shutil
import time

import pytest

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
from automagician.machine import get_subfile
from automagician.process_job import (
    check_error,
    check_has_opt,
    determine_box_convergence,
    determine_convergence,
    get_submitted_jobs,
    gone_job_check,
    grep_ll_out_convergence,
    is_isif3,
    process_converged,
    process_dos,
    process_opt,
    process_unconverged,
)
from automagician.small_functions import classify_job_dir


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


import unittest.mock as mock


@mock.patch("subprocess.check_output")
@mock.patch("subprocess.call")
def test_get_submitted_jobs_fri(mock_call, mock_check_output):
    # squeue output with header, one running job, and newline at the end
    mock_check_output.return_value = "JOBID STATE WORKDIR\n12345 R /home/user/job1\n"

    # machine 0 is FRI
    machine = Machine.FRI
    opt_jobs = {
        "/home/user/job1": OptJob(JobStatus.RUNNING, Machine.FRI, Machine.FRI),
        "/home/user/job2": OptJob(JobStatus.CONVERGED, Machine.FRI, Machine.FRI),
    }
    dos_jobs = {
        "/home/user/job3": DosJob(
            opt_id=-1,
            sc_status=JobStatus.RUNNING,
            dos_status=JobStatus.RUNNING,
            sc_last_on=Machine.FRI,
            dos_last_on=Machine.FRI,
        )
    }
    wav_jobs = {
        "/home/user/job4": WavJob(
            opt_id=-1, wav_status=JobStatus.RUNNING, wav_last_on=Machine.FRI
        )
    }
    tacc_queue_sizes = [0, 0, 0]

    # Mock classify_job_dir to say it's an opt job
    with mock.patch(
        "automagician.small_functions.classify_job_dir", return_value="opt"
    ):
        get_submitted_jobs(machine, opt_jobs, dos_jobs, wav_jobs, tacc_queue_sizes)

    # All jobs previously RUNNING should be reset to INCOMPLETE before processing the slurm queue
    assert (
        opt_jobs["/home/user/job1"].status == JobStatus.RUNNING
    )  # it gets updated to running by the mock squeue
    assert opt_jobs["/home/user/job2"].status == JobStatus.CONVERGED
    assert dos_jobs["/home/user/job3"].sc_status == JobStatus.INCOMPLETE
    assert dos_jobs["/home/user/job3"].dos_status == JobStatus.INCOMPLETE
    assert wav_jobs["/home/user/job4"].wav_status == JobStatus.INCOMPLETE


@mock.patch("subprocess.check_output")
@mock.patch("subprocess.call")
def test_get_submitted_jobs_tacc(mock_call, mock_check_output):
    # squeue output with header, one running job, and newline at the end
    mock_check_output.return_value = "JOBID STATE WORKDIR\n12345 R /home/user/job1\n"

    # machine 2 is STAMPEDE2_TACC
    machine = Machine.STAMPEDE2_TACC
    opt_jobs = {
        "/home/user/job1": OptJob(
            JobStatus.RUNNING, Machine.STAMPEDE2_TACC, Machine.STAMPEDE2_TACC
        ),
        "/home/user/job2": OptJob(
            JobStatus.CONVERGED, Machine.STAMPEDE2_TACC, Machine.STAMPEDE2_TACC
        ),
    }
    dos_jobs = {
        "/home/user/job3": DosJob(
            opt_id=-1,
            sc_status=JobStatus.RUNNING,
            dos_status=JobStatus.RUNNING,
            sc_last_on=Machine.STAMPEDE2_TACC,
            dos_last_on=Machine.STAMPEDE2_TACC,
        )
    }
    wav_jobs = {
        "/home/user/job4": WavJob(
            opt_id=-1,
            wav_status=JobStatus.INCOMPLETE,
            wav_last_on=Machine.STAMPEDE2_TACC,
        )
    }
    tacc_queue_sizes = [0, 0, 0]

    # Mock classify_job_dir to say it's an opt job
    with mock.patch(
        "automagician.small_functions.classify_job_dir", return_value="opt"
    ):
        get_submitted_jobs(machine, opt_jobs, dos_jobs, wav_jobs, tacc_queue_sizes)

    # Queue sizes should be updated based on running jobs
    # wav job was incomplete so it increments counter and sets wav status to running
    assert tacc_queue_sizes[0] == 4  # 1 opt + 2 dos (sc + dos) + 1 wav
    assert tacc_queue_sizes[1] == 0
    assert tacc_queue_sizes[2] == 0

    # The job1 should be set back to RUNNING by the slurm queue parsing
    assert opt_jobs["/home/user/job1"].status == JobStatus.RUNNING
    assert opt_jobs["/home/user/job2"].status == JobStatus.CONVERGED
    assert dos_jobs["/home/user/job3"].sc_status == JobStatus.INCOMPLETE
    assert dos_jobs["/home/user/job3"].dos_status == JobStatus.INCOMPLETE
    assert wav_jobs["/home/user/job4"].wav_status == JobStatus.RUNNING


@mock.patch("subprocess.check_output")
@mock.patch("subprocess.call")
def test_get_submitted_jobs_squeue_parsing(mock_call, mock_check_output):
    # squeue output with three jobs: 1 successful opt, 1 failing dos (OOM), 1 successful wav
    mock_check_output.return_value = "JOBID STATE WORKDIR\n12345 R /home/user/job_opt\n67890 OOM /home/user/job_dos/dos\n11111 R /home/user/job_wav/wav\n"

    machine = Machine.FRI
    opt_jobs = {}
    dos_jobs = {}
    wav_jobs = {}
    tacc_queue_sizes = [0, 0, 0]

    # Need a side effect for classify_job_dir to classify based on dir name
    def mock_classify(job_dir):
        if "dos" in job_dir:
            return "dos"
        if "wav" in job_dir:
            return "wav"
        return "opt"

    def mock_get_opt_dir(job_dir):
        if "dos" in job_dir:
            return "/home/user/job_dos"
        if "wav" in job_dir:
            return "/home/user/job_wav"
        return job_dir

    with mock.patch(
        "automagician.small_functions.classify_job_dir", side_effect=mock_classify
    ), mock.patch(
        "automagician.small_functions.get_opt_dir", side_effect=mock_get_opt_dir
    ):
        get_submitted_jobs(machine, opt_jobs, dos_jobs, wav_jobs, tacc_queue_sizes)

    # Scancel should be called for the failing job
    mock_call.assert_called_with(["scancel", "67890"])

    # Opt job should be created and running
    assert "/home/user/job_opt" in opt_jobs
    assert opt_jobs["/home/user/job_opt"].status == JobStatus.RUNNING

    # Dos job should be created with dos in ERROR status
    assert "/home/user/job_dos" in dos_jobs
    assert dos_jobs["/home/user/job_dos"].dos_status == JobStatus.ERROR
    # since we mocked dos specifically, sc_status is the default initialization

    # Wav job should be created and running
    assert "/home/user/job_wav" in wav_jobs
    assert wav_jobs["/home/user/job_wav"].wav_status == JobStatus.RUNNING


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
