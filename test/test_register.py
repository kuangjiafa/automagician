import os
import shutil
from unittest.mock import MagicMock, patch

from automagician.classes import DosJob, JobStatus, OptJob, SSHConfig, WavJob
from automagician.register import exclude_regex, process_queue, register


@patch("automagician.process_job.subprocess.call")
@patch("automagician.finish_job.subprocess.run")
def test_process_queue_nothing(mock_run, mock_call, tmp_path):
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


@patch("automagician.process_job.subprocess.call")
@patch("automagician.finish_job.subprocess.run")
def test_process_queue_opt_unconverged_dos_wav(mock_run, mock_call, tmp_path):
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


@patch("automagician.process_job.subprocess.call")
@patch("automagician.finish_job.subprocess.run")
def test_register_no_jobs(mock_run, mock_call, tmp_path):
    cwd = os.getcwd()
    try:
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
    finally:
        os.chdir(cwd)
    assert sub_queue == []
    assert opt_jobs == {}
    assert dos_jobs == {}
    assert wav_jobs == {}


@patch("automagician.process_job.subprocess.call")
@patch("automagician.finish_job.subprocess.run")
def test_register_jobs(mock_run, mock_call, tmp_path):
    cwd = os.getcwd()
    try:
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
    finally:
        os.chdir(cwd)
    assert sub_queue == [opt_job_1]
    assert opt_jobs == {opt_job_1: OptJob(JobStatus.INCOMPLETE, 0, 0)}
    assert dos_jobs == {}
    assert wav_jobs == {}


@patch("automagician.process_job.subprocess.call")
@patch("automagician.finish_job.subprocess.run")
def test_register_jobs_converged(mock_run, mock_call, tmp_path):
    cwd = os.getcwd()
    try:
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
    finally:
        os.chdir(cwd)
    assert sub_queue == []
    assert opt_jobs == {opt_job_1: OptJob(JobStatus.CONVERGED, 0, 0)}
    assert dos_jobs == {}
    assert wav_jobs == {}


@patch("automagician.process_job.subprocess.call")
@patch("automagician.finish_job.subprocess.run")
def test_register_empty_note(mock_run, mock_call, tmp_path):
    cwd = os.getcwd()
    try:
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
    finally:
        os.chdir(cwd)
    assert sub_queue == []
    assert opt_jobs == {opt_job_1: OptJob(JobStatus.CONVERGED, 0, 0)}
    assert dos_jobs == {}
    assert wav_jobs == {}


@patch("automagician.process_job.subprocess.call")
@patch("automagician.finish_job.subprocess.run")
def test_register_dos_note(mock_run, mock_call, tmp_path):
    cwd = os.getcwd()
    try:
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
    finally:
        os.chdir(cwd)
    assert sub_queue == [os.path.normpath(os.path.join(opt_job_1, "../sc"))]
    assert opt_jobs == {opt_job_1: OptJob(JobStatus.CONVERGED, 0, 0)}
    assert dos_jobs == {
        opt_job_1: DosJob(-1, JobStatus.RUNNING, JobStatus.INCOMPLETE, 0, 0)
    }
    assert wav_jobs == {}


@patch("automagician.process_job.subprocess.call")
@patch("automagician.finish_job.subprocess.run")
def test_register_wav_note(mock_run, mock_call, tmp_path):
    cwd = os.getcwd()
    try:
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
    finally:
        os.chdir(cwd)
    assert sub_queue == [os.path.normpath(os.path.join(opt_job_1, "../wav"))]
    assert opt_jobs == {opt_job_1: OptJob(JobStatus.CONVERGED, 0, 0)}
    assert dos_jobs == {}
    assert wav_jobs == {opt_job_1: WavJob(-1, JobStatus.RUNNING, 0)}


@patch("automagician.process_job.subprocess.call")
@patch("automagician.finish_job.subprocess.run")
def test_register_dos_and_wav_note(mock_run, mock_call, tmp_path):
    cwd = os.getcwd()
    try:
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
    finally:
        os.chdir(cwd)
    assert sub_queue == [os.path.normpath(os.path.join(opt_job_1, "../wav"))]
    assert opt_jobs == {opt_job_1: OptJob(JobStatus.CONVERGED, 0, 0)}
    assert dos_jobs == {}
    assert wav_jobs == {opt_job_1: WavJob(-1, JobStatus.RUNNING, 0)}


@patch("automagician.process_job.subprocess.call")
@patch("automagician.finish_job.subprocess.run")
def test_register_exclude_note(mock_run, mock_call, tmp_path):
    cwd = os.getcwd()
    try:
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
    finally:
        os.chdir(cwd)
    assert sub_queue == []
    assert opt_jobs == {}
    assert dos_jobs == {}
    assert wav_jobs == {}


@patch("automagician.process_job.subprocess.call")
@patch("automagician.finish_job.subprocess.run")
def test_register_neb(mock_run, mock_call, tmp_path):
    cwd = os.getcwd()
    try:
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
    finally:
        os.chdir(cwd)
    assert sub_queue == []
    assert opt_jobs == {}
    assert dos_jobs == {}
    assert wav_jobs == {}


@patch("automagician.process_job.subprocess.call")
@patch("automagician.finish_job.subprocess.run")
def test_register_neb_captalized(mock_run, mock_call, tmp_path):
    cwd = os.getcwd()
    try:
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
    finally:
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
