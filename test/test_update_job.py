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
