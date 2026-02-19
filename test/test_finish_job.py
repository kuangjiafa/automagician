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
    assert give_certificate(tmp_path) == 1