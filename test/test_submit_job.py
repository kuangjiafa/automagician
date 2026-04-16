# pylint: disable=all
import logging
import os
import subprocess as _subprocess_real
from unittest.mock import MagicMock, call, patch

from automagician.classes import DosJob, JobStatus, Machine, OptJob, SSHConfig, WavJob
from automagician.database import Database
from automagician.process_job import get_submitted_jobs, submit_queue


def fix_subprocess(*args, **kwargs):
    if args[0][0] == "squeue":
        with open("test/test_files/sample_squeue", "r") as f:
            mock = MagicMock()
            mock.stdout = f.read()
            return mock
    if args[0][0] == "sbatch":
        mock = MagicMock()
        print(args[0][1])
        if "error" in args[0][1]:
            mock.returncode = 1
        else:
            mock.returncode = 0
        return mock
    raise AssertionError()


@patch("automagician.process_job.subprocess")
def test_submit_queue_no_jobs(monkeypatch, tmp_path):
    monkeypatch.run = MagicMock(side_effect=fix_subprocess)
    db = Database(os.path.join(tmp_path, "test_db"))
    config = SSHConfig("NoSSH")
    sub_queue = []
    tacc_quene_sizes = [0, 0, 0]
    opt_jobs = {}
    dos_jobs = {}
    wav_jobs = {}
    cwd = os.getcwd()
    submit_queue(
        machine=Machine.FRI,
        balance=False,
        ssh_config=config,
        sub_queue=sub_queue,
        home=tmp_path,
        tacc_queue_sizes=tacc_quene_sizes,
        opt_jobs=opt_jobs,
        wav_jobs=wav_jobs,
        dos_jobs=dos_jobs,
        database=db,
        limit=999,
    )
    assert cwd == os.getcwd()
    monkeypatch.run.assert_called_once_with(["squeue"], capture_output=True)


@patch("automagician.process_job.subprocess")
def test_submit_queue_1_job(monkeypatch, tmp_path):
    monkeypatch.run = MagicMock(side_effect=fix_subprocess)
    db = Database(os.path.join(tmp_path, "test_db"))
    job1_path = os.path.join(tmp_path, "job1")
    os.mkdir(job1_path)
    config = SSHConfig("NoSSH")
    sub_queue = [job1_path]
    opt_job_submit = OptJob(JobStatus.INCOMPLETE, 0, 0)
    tacc_quene_sizes = [0, 0, 0]
    opt_jobs = {job1_path: opt_job_submit}
    dos_jobs = {}
    wav_jobs = {}
    cwd = os.getcwd()
    submit_queue(
        machine=Machine.FRI,
        balance=False,
        ssh_config=config,
        sub_queue=sub_queue,
        home=tmp_path,
        tacc_queue_sizes=tacc_quene_sizes,
        opt_jobs=opt_jobs,
        wav_jobs=wav_jobs,
        dos_jobs=dos_jobs,
        database=db,
        limit=999,
    )
    assert cwd == os.getcwd()
    monkeypatch.run.assert_has_calls(
        [
            call(["squeue"], capture_output=True),
            call(["sbatch", os.path.join(job1_path, "fri.sub")]),
        ]
    )
    assert opt_jobs == {job1_path: OptJob(JobStatus.RUNNING, 0, 0)}


@patch("automagician.process_job.subprocess")
def test_submit_queue_2_jobs(monkeypatch, tmp_path):
    monkeypatch.run = MagicMock(side_effect=fix_subprocess)
    db = Database(os.path.join(tmp_path, "test_db"))
    job1_path = os.path.join(tmp_path, "job1")
    job2_path = os.path.join(tmp_path, "job2")
    os.mkdir(job1_path)
    os.mkdir(job2_path)
    config = SSHConfig("NoSSH")
    sub_queue = [job1_path]
    opt_job_submit = OptJob(JobStatus.INCOMPLETE, 0, 0)
    opt_job_submit2 = OptJob(JobStatus.INCOMPLETE, 0, 0)
    tacc_quene_sizes = [0, 0, 0]
    opt_jobs = {job1_path: opt_job_submit, job2_path: opt_job_submit2}
    dos_jobs = {}
    wav_jobs = {}
    cwd = os.getcwd()
    submit_queue(
        machine=Machine.FRI,
        balance=False,
        ssh_config=config,
        sub_queue=sub_queue,
        home=tmp_path,
        tacc_queue_sizes=tacc_quene_sizes,
        opt_jobs=opt_jobs,
        wav_jobs=wav_jobs,
        dos_jobs=dos_jobs,
        database=db,
        limit=999,
    )
    assert cwd == os.getcwd()
    monkeypatch.run.assert_has_calls(
        [
            call(["squeue"], capture_output=True),
            call(["sbatch", os.path.join(job1_path, "fri.sub")]),
        ]
    )
    assert opt_jobs == {
        job1_path: OptJob(JobStatus.RUNNING, 0, 0),
        job2_path: OptJob(JobStatus.INCOMPLETE, 0, 0),
    }


@patch("automagician.process_job.subprocess")
def test_submit_queue_2_jobs_sbatch_error(monkeypatch, tmp_path):
    monkeypatch.run = MagicMock(side_effect=fix_subprocess)
    db = Database(os.path.join(tmp_path, "test_db"))
    job1_path = os.path.join(tmp_path, "job1")
    job2_path = os.path.join(tmp_path, "job2")
    job_err_path = os.path.join(tmp_path, "joberror")
    os.mkdir(job1_path)
    os.mkdir(job2_path)
    os.mkdir(job_err_path)
    config = SSHConfig("NoSSH")
    sub_queue = [job1_path, job2_path, job_err_path]
    opt_job_submit = OptJob(JobStatus.INCOMPLETE, 0, 0)
    opt_job_submit2 = OptJob(JobStatus.INCOMPLETE, 0, 0)
    opt_job_submit3 = OptJob(JobStatus.INCOMPLETE, 0, 0)
    tacc_quene_sizes = [0, 0, 0]
    opt_jobs = {
        job1_path: opt_job_submit,
        job2_path: opt_job_submit2,
        job_err_path: opt_job_submit3,
    }
    dos_jobs = {}
    wav_jobs = {}
    cwd = os.getcwd()
    submit_queue(
        machine=Machine.FRI,
        balance=False,
        ssh_config=config,
        sub_queue=sub_queue,
        home=tmp_path,
        tacc_queue_sizes=tacc_quene_sizes,
        opt_jobs=opt_jobs,
        wav_jobs=wav_jobs,
        dos_jobs=dos_jobs,
        database=db,
        limit=999,
    )
    assert cwd == os.getcwd()
    monkeypatch.run.assert_has_calls(
        [
            call(["squeue"], capture_output=True),
            call(["sbatch", os.path.join(job1_path, "fri.sub")]),
            call(["sbatch", os.path.join(job2_path, "fri.sub")]),
            call(["sbatch", os.path.join(job_err_path, "fri.sub")]),
        ]
    )
    assert opt_jobs == {
        job1_path: OptJob(JobStatus.RUNNING, 0, 0),
        job2_path: OptJob(JobStatus.RUNNING, 0, 0),
        job_err_path: OptJob(JobStatus.ERROR, 0, 0),
    }


@patch("automagician.process_job.subprocess")
def test_submit_queue_2_jobs_limit_1(monkeypatch, tmp_path):
    monkeypatch.run = MagicMock(side_effect=fix_subprocess)
    db = Database(os.path.join(tmp_path, "test_db"))
    job1_path = os.path.join(tmp_path, "job1")
    job2_path = os.path.join(tmp_path, "job2")
    os.mkdir(job1_path)
    os.mkdir(job2_path)
    config = SSHConfig("NoSSH")
    sub_queue = [job1_path]
    opt_job_submit = OptJob(JobStatus.INCOMPLETE, 0, 0)
    opt_job_submit2 = OptJob(JobStatus.INCOMPLETE, 0, 0)
    tacc_quene_sizes = [0, 0, 0]
    opt_jobs = {job1_path: opt_job_submit, job2_path: opt_job_submit2}
    dos_jobs = {}
    wav_jobs = {}
    cwd = os.getcwd()
    submit_queue(
        machine=Machine.FRI,
        balance=False,
        ssh_config=config,
        sub_queue=sub_queue,
        home=tmp_path,
        tacc_queue_sizes=tacc_quene_sizes,
        opt_jobs=opt_jobs,
        wav_jobs=wav_jobs,
        dos_jobs=dos_jobs,
        database=db,
        limit=1,
    )
    assert cwd == os.getcwd()
    monkeypatch.run.assert_not_called()
    assert opt_jobs == {
        job1_path: OptJob(JobStatus.INCOMPLETE, 0, 0),
        job2_path: OptJob(JobStatus.INCOMPLETE, 0, 0),
    }


@patch("automagician.process_job.subprocess")
def test_submit_queue_2_jobs_acutally_submitted_hit_limit(monkeypatch, tmp_path):
    monkeypatch.run = MagicMock(side_effect=fix_subprocess)
    db = Database(os.path.join(tmp_path, "test_db"))
    job1_path = os.path.join(tmp_path, "job1")
    job2_path = os.path.join(tmp_path, "job2")
    os.mkdir(job1_path)
    os.mkdir(job2_path)
    config = SSHConfig("NoSSH")
    sub_queue = [job1_path, job2_path]
    opt_job_submit = OptJob(JobStatus.INCOMPLETE, 0, 0)
    opt_job_submit2 = OptJob(JobStatus.INCOMPLETE, 0, 0)
    tacc_quene_sizes = [0, 0, 0]
    opt_jobs = {job1_path: opt_job_submit, job2_path: opt_job_submit2}
    dos_jobs = {}
    wav_jobs = {}
    cwd = os.getcwd()
    submit_queue(
        machine=Machine.FRI,
        balance=False,
        ssh_config=config,
        sub_queue=sub_queue,
        home=tmp_path,
        tacc_queue_sizes=tacc_quene_sizes,
        opt_jobs=opt_jobs,
        wav_jobs=wav_jobs,
        dos_jobs=dos_jobs,
        database=db,
        limit=2,
    )
    assert cwd == os.getcwd()
    monkeypatch.run.assert_not_called()
    assert opt_jobs == {
        job1_path: OptJob(JobStatus.INCOMPLETE, 0, 0),
        job2_path: OptJob(JobStatus.INCOMPLETE, 0, 0),
    }


@patch("automagician.process_job.subprocess")
def test_get_submitted_job_not_in_dictionary(monkeypatch):
    mock_squeue = ""
    with open("test/test_files/sample_squeue_submit_job", "r") as f:
        mock_squeue = f.read()
    monkeypatch.check_output = MagicMock(return_value=mock_squeue)
    monkeypatch.call = MagicMock(return_value="")
    opt_jobs = {"/home/jw53959/test": OptJob(JobStatus.RUNNING, 0, 0)}
    dos_jobs = {
        "/home/jw53959/test": DosJob(-1, JobStatus.RUNNING, JobStatus.RUNNING, 0, 0)
    }
    wav_jobs = {"/home/jw53959/test": WavJob(-1, JobStatus.RUNNING, 0)}
    tacc_quene_sizes = [0, 0, 0]

    get_submitted_jobs(0, opt_jobs, dos_jobs, wav_jobs, tacc_quene_sizes)

    assert tacc_quene_sizes == [0, 0, 0]
    monkeypatch.check_output.assert_called_once_with(
        ["squeue", "-u", os.environ["USER"], "-o", "%A %t %Z"],
        stderr=monkeypatch.STDOUT,
        text=True,
    )
    assert opt_jobs == {
        "/home/jw53959/test": OptJob(JobStatus.INCOMPLETE, 0, 0),
        "/home/dx858/ZTEST/12to8": OptJob(JobStatus.RUNNING, 0, 0),
        "/home/dx858/ZTEST/13to17": OptJob(JobStatus.RUNNING, 0, 0),
        "/home/dx858/ZTEST/13to19": OptJob(JobStatus.RUNNING, 0, 0),
        "/home/dx858/ZTEST/12to16": OptJob(JobStatus.RUNNING, 0, 0),
        "/home/dx858/ZTEST/12to17": OptJob(JobStatus.ERROR, 0, 0),
        "/home/dx858/ZTEST/12to18": OptJob(JobStatus.ERROR, 0, 0),
        "/home/dx858/ZTEST/12to19": OptJob(JobStatus.ERROR, 0, 0),
        "/home/dx858/ZTEST/12to20": OptJob(JobStatus.ERROR, 0, 0),
        "/home/dx858/ZTEST/12to21": OptJob(JobStatus.ERROR, 0, 0),
        "/home/dx858/ZTEST/12to22": OptJob(JobStatus.ERROR, 0, 0),
    }
    assert dos_jobs == {
        "/home/jw53959/test": DosJob(
            -1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 0, 0
        ),
        "/home/dx858/ZTEST/12to16": DosJob(
            -1, JobStatus.RUNNING, JobStatus.RUNNING, 0, 0
        ),
        "/home/dx858/ZTEST/12to17": DosJob(-1, JobStatus.ERROR, JobStatus.ERROR, 0, 0),
        "/home/dx858/ZTEST/12to18": DosJob(-1, JobStatus.ERROR, JobStatus.ERROR, 0, 0),
        "/home/dx858/ZTEST/12to19": DosJob(-1, JobStatus.ERROR, JobStatus.ERROR, 0, 0),
        "/home/dx858/ZTEST/12to20": DosJob(-1, JobStatus.ERROR, JobStatus.ERROR, 0, 0),
        "/home/dx858/ZTEST/12to21": DosJob(-1, JobStatus.ERROR, JobStatus.ERROR, 0, 0),
        "/home/dx858/ZTEST/12to22": DosJob(-1, JobStatus.ERROR, JobStatus.ERROR, 0, 0),
    }
    assert wav_jobs == {
        "/home/jw53959/test": WavJob(-1, JobStatus.INCOMPLETE, 0),
        "/home/dx858/ZTEST/12to16": WavJob(-1, JobStatus.RUNNING, 0),
        "/home/dx858/ZTEST/12to17": WavJob(-1, JobStatus.ERROR, 0),
        "/home/dx858/ZTEST/12to18": WavJob(-1, JobStatus.ERROR, 0),
        "/home/dx858/ZTEST/12to19": WavJob(-1, JobStatus.ERROR, 0),
        "/home/dx858/ZTEST/12to20": WavJob(-1, JobStatus.ERROR, 0),
        "/home/dx858/ZTEST/12to21": WavJob(-1, JobStatus.ERROR, 0),
        "/home/dx858/ZTEST/12to22": WavJob(-1, JobStatus.ERROR, 0),
    }

    scancel_calls = [
        call(["scancel", "53244"]),
        call(["scancel", "53245"]),
        call(["scancel", "53246"]),
        call(["scancel", "53247"]),
        call(["scancel", "53248"]),
        call(["scancel", "53249"]),
        call(["scancel", "53251"]),
        call(["scancel", "53252"]),
        call(["scancel", "53253"]),
        call(["scancel", "53254"]),
        call(["scancel", "53255"]),
        call(["scancel", "53256"]),
        call(["scancel", "53258"]),
        call(["scancel", "53259"]),
        call(["scancel", "53260"]),
        call(["scancel", "53261"]),
        call(["scancel", "53262"]),
        call(["scancel", "53263"]),
        call(["scancel", "53265"]),
        call(["scancel", "53266"]),
        call(["scancel", "53267"]),
        call(["scancel", "53268"]),
        call(["scancel", "53269"]),
        call(["scancel", "53270"]),
    ]
    monkeypatch.call.assert_has_calls(scancel_calls)


@patch("automagician.process_job.subprocess")
def test_get_submitted_job_in_dictionary(monkeypatch):
    mock_squeue = ""
    with open("test/test_files/sample_squeue_submit_job", "r") as f:
        mock_squeue = f.read()
    monkeypatch.check_output = MagicMock(return_value=mock_squeue)
    monkeypatch.call = MagicMock(return_value="")
    opt_jobs = {
        "/home/dx858/ZTEST/12to8": OptJob(JobStatus.INCOMPLETE, 1, 2),
        "/home/dx858/ZTEST/13to17": OptJob(JobStatus.INCOMPLETE, 3, 4),
        "/home/dx858/ZTEST/13to19": OptJob(JobStatus.INCOMPLETE, 5, 6),
        "/home/dx858/ZTEST/12to16": OptJob(JobStatus.INCOMPLETE, 7, 8),
        "/home/dx858/ZTEST/12to17": OptJob(JobStatus.INCOMPLETE, 9, 10),
        "/home/dx858/ZTEST/12to18": OptJob(JobStatus.INCOMPLETE, 11, 12),
        "/home/dx858/ZTEST/12to19": OptJob(JobStatus.INCOMPLETE, 13, 14),
        "/home/dx858/ZTEST/12to20": OptJob(JobStatus.INCOMPLETE, 15, 16),
        "/home/dx858/ZTEST/12to21": OptJob(JobStatus.INCOMPLETE, 17, 18),
        "/home/dx858/ZTEST/12to22": OptJob(JobStatus.INCOMPLETE, 19, 20),
    }
    dos_jobs = {
        "/home/dx858/ZTEST/12to16": DosJob(
            -1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 1, 2
        ),
        "/home/dx858/ZTEST/12to17": DosJob(
            -1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 3, 4
        ),
        "/home/dx858/ZTEST/12to18": DosJob(
            -1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 5, 6
        ),
        "/home/dx858/ZTEST/12to19": DosJob(
            -1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 7, 8
        ),
        "/home/dx858/ZTEST/12to20": DosJob(
            -1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 9, 10
        ),
        "/home/dx858/ZTEST/12to21": DosJob(
            -1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 11, 12
        ),
        "/home/dx858/ZTEST/12to22": DosJob(
            -1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, 13, 14
        ),
    }
    wav_jobs = {
        "/home/dx858/ZTEST/12to16": WavJob(-1, JobStatus.INCOMPLETE, 1),
        "/home/dx858/ZTEST/12to17": WavJob(-1, JobStatus.INCOMPLETE, 2),
        "/home/dx858/ZTEST/12to18": WavJob(-1, JobStatus.INCOMPLETE, 3),
        "/home/dx858/ZTEST/12to19": WavJob(-1, JobStatus.INCOMPLETE, 4),
        "/home/dx858/ZTEST/12to20": WavJob(-1, JobStatus.INCOMPLETE, 5),
        "/home/dx858/ZTEST/12to21": WavJob(-1, JobStatus.INCOMPLETE, 6),
        "/home/dx858/ZTEST/12to22": WavJob(-1, JobStatus.INCOMPLETE, 7),
    }
    tacc_quene_sizes = [0, 0, 0]

    get_submitted_jobs(0, opt_jobs, dos_jobs, wav_jobs, tacc_quene_sizes)

    assert tacc_quene_sizes == [0, 0, 0]

    assert opt_jobs == {
        "/home/dx858/ZTEST/12to8": OptJob(JobStatus.RUNNING, 1, 0),
        "/home/dx858/ZTEST/13to17": OptJob(JobStatus.RUNNING, 3, 0),
        "/home/dx858/ZTEST/13to19": OptJob(JobStatus.RUNNING, 5, 0),
        "/home/dx858/ZTEST/12to16": OptJob(JobStatus.RUNNING, 7, 0),
        "/home/dx858/ZTEST/12to17": OptJob(JobStatus.ERROR, 9, 0),
        "/home/dx858/ZTEST/12to18": OptJob(JobStatus.ERROR, 11, 0),
        "/home/dx858/ZTEST/12to19": OptJob(JobStatus.ERROR, 13, 0),
        "/home/dx858/ZTEST/12to20": OptJob(JobStatus.ERROR, 15, 0),
        "/home/dx858/ZTEST/12to21": OptJob(JobStatus.ERROR, 17, 0),
        "/home/dx858/ZTEST/12to22": OptJob(JobStatus.ERROR, 19, 0),
    }
    assert dos_jobs == {
        "/home/dx858/ZTEST/12to16": DosJob(
            -1, JobStatus.RUNNING, JobStatus.RUNNING, 0, 0
        ),
        "/home/dx858/ZTEST/12to17": DosJob(-1, JobStatus.ERROR, JobStatus.ERROR, 0, 0),
        "/home/dx858/ZTEST/12to18": DosJob(-1, JobStatus.ERROR, JobStatus.ERROR, 0, 0),
        "/home/dx858/ZTEST/12to19": DosJob(-1, JobStatus.ERROR, JobStatus.ERROR, 0, 0),
        "/home/dx858/ZTEST/12to20": DosJob(-1, JobStatus.ERROR, JobStatus.ERROR, 0, 0),
        "/home/dx858/ZTEST/12to21": DosJob(-1, JobStatus.ERROR, JobStatus.ERROR, 0, 0),
        "/home/dx858/ZTEST/12to22": DosJob(-1, JobStatus.ERROR, JobStatus.ERROR, 0, 0),
    }
    assert wav_jobs == {
        "/home/dx858/ZTEST/12to16": WavJob(-1, JobStatus.RUNNING, 0),
        "/home/dx858/ZTEST/12to17": WavJob(-1, JobStatus.ERROR, 0),
        "/home/dx858/ZTEST/12to18": WavJob(-1, JobStatus.ERROR, 0),
        "/home/dx858/ZTEST/12to19": WavJob(-1, JobStatus.ERROR, 0),
        "/home/dx858/ZTEST/12to20": WavJob(-1, JobStatus.ERROR, 0),
        "/home/dx858/ZTEST/12to21": WavJob(-1, JobStatus.ERROR, 0),
        "/home/dx858/ZTEST/12to22": WavJob(-1, JobStatus.ERROR, 0),
    }

    scancel_calls = [
        call(["scancel", "53244"]),
        call(["scancel", "53245"]),
        call(["scancel", "53246"]),
        call(["scancel", "53247"]),
        call(["scancel", "53248"]),
        call(["scancel", "53249"]),
        call(["scancel", "53251"]),
        call(["scancel", "53252"]),
        call(["scancel", "53253"]),
        call(["scancel", "53254"]),
        call(["scancel", "53255"]),
        call(["scancel", "53256"]),
        call(["scancel", "53258"]),
        call(["scancel", "53259"]),
        call(["scancel", "53260"]),
        call(["scancel", "53261"]),
        call(["scancel", "53262"]),
        call(["scancel", "53263"]),
        call(["scancel", "53265"]),
        call(["scancel", "53266"]),
        call(["scancel", "53267"]),
        call(["scancel", "53268"]),
        call(["scancel", "53269"]),
        call(["scancel", "53270"]),
    ]
    monkeypatch.call.assert_has_calls(scancel_calls)


@patch("automagician.process_job.subprocess")
def test_get_submitted_jobs_tacc_dos_queue_accounting(mock_subprocess):
    """Test DOS queue accounting on TACC machines uses dos_last_on field.

    This test verifies:
    1. DOS jobs with RUNNING dos_status increment tacc_queue_sizes correctly
    2. The dos_last_on field is used (not opt_jobs[..].last_on)
    3. No KeyError occurs when opt_jobs lacks the DOS job entry
    """
    # Mock subprocess to return empty squeue output (no jobs in queue)
    mock_subprocess.check_output = MagicMock(return_value="")
    mock_subprocess.call = MagicMock(return_value="")

    # Setup: DOS job on STAMPEDE2 (machine 2) without a matching opt_job
    dos_job_dir = "/home/test_user/test_job/dos"
    dos_jobs = {
        dos_job_dir: DosJob(
            opt_id=123,
            sc_status=JobStatus.INCOMPLETE,
            dos_status=JobStatus.RUNNING,  # This is running
            sc_last_on=Machine.FRI,  # SC was on FRI
            dos_last_on=Machine.STAMPEDE2_TACC,  # DOS on STAMPEDE2 (value 2, tacc_queue_sizes[2-2=0])
        )
    }
    opt_jobs = {}  # No matching opt_job - this is the key test case
    wav_jobs = {}
    tacc_queue_sizes = [0, 0, 0]  # [STAMPEDE2, FRONTERA, LS6]

    # Call get_submitted_jobs with STAMPEDE2_TACC machine
    get_submitted_jobs(
        Machine.STAMPEDE2_TACC,  # machine = 2
        opt_jobs,
        dos_jobs,
        wav_jobs,
        tacc_queue_sizes,
    )

    # Assert: tacc_queue_sizes[0] (STAMPEDE2) should be incremented
    # dos_last_on=2 (STAMPEDE2), so tacc_queue_sizes[2-2=0] should be 1
    assert tacc_queue_sizes == [1, 0, 0]
    # DOS job should be marked INCOMPLETE since it's not actually in the queue
    assert dos_jobs[dos_job_dir].dos_status == JobStatus.INCOMPLETE


@patch("automagician.process_job.subprocess")
def test_get_submitted_jobs_tacc_dos_queue_accounting_with_sc(mock_subprocess):
    """Test DOS queue accounting on TACC with both SC and DOS running."""
    # Mock subprocess to return empty squeue output
    mock_subprocess.check_output = MagicMock(return_value="")
    mock_subprocess.call = MagicMock(return_value="")

    dos_job_dir = "/home/test_user/test_job/dos"
    dos_jobs = {
        dos_job_dir: DosJob(
            opt_id=456,
            sc_status=JobStatus.RUNNING,  # SC is also running
            dos_status=JobStatus.RUNNING,  # DOS is running
            sc_last_on=Machine.FRONTERA_TACC,  # SC on FRONTERA (value 3, tacc_queue_sizes[3-2=1])
            dos_last_on=Machine.LS6_TACC,  # DOS on LS6 (value 4, tacc_queue_sizes[4-2=2])
        )
    }
    opt_jobs = {}  # No matching opt_job
    wav_jobs = {}
    tacc_queue_sizes = [0, 0, 0]

    # Call with LS6_TACC machine
    get_submitted_jobs(
        Machine.LS6_TACC,  # machine = 4
        opt_jobs,
        dos_jobs,
        wav_jobs,
        tacc_queue_sizes,
    )

    # Assert: Both SC and DOS should increment their respective buckets
    # sc_last_on=3 (FRONTERA), tacc_queue_sizes[3-2=1] should be 1
    # dos_last_on=4 (LS6), tacc_queue_sizes[4-2=2] should be 1
    assert tacc_queue_sizes == [0, 1, 1]
    # DOS should be marked INCOMPLETE since on LS6 machine but not in queue
    assert dos_jobs[dos_job_dir].dos_status == JobStatus.INCOMPLETE
    # SC should remain RUNNING since on different machine (FRONTERA)
    assert dos_jobs[dos_job_dir].sc_status == JobStatus.RUNNING


# ---------------------------------------------------------------------------
# sacct integration tests
# ---------------------------------------------------------------------------

def _make_sacct_result(stdout: str, returncode: int = 0) -> MagicMock:
    """Helper: build a mock subprocess.run result for a sacct call."""
    result = MagicMock()
    result.returncode = returncode
    result.stdout = stdout
    result.stderr = ""
    return result


def _empty_squeue_mock() -> MagicMock:
    """squeue output with only a header line — no running jobs."""
    return MagicMock(return_value="")


@patch("automagician.process_job.subprocess")
def test_get_submitted_jobs_sacct_warns_on_node_fail(mock_subprocess, caplog):
    """A job that vanishes from squeue with a NODE_FAIL sacct record emits a WARNING."""
    job_dir = "/home/user/test_job_nf"

    mock_subprocess.check_output = _empty_squeue_mock()
    mock_subprocess.call = MagicMock(return_value=0)
    mock_subprocess.TimeoutExpired = _subprocess_real.TimeoutExpired
    mock_subprocess.run = MagicMock(
        return_value=_make_sacct_result(
            f"12345|NODE_FAIL|{job_dir}|0:9|0:9|c401-001||01:23:45\n"
        )
    )

    opt_jobs = {job_dir: OptJob(JobStatus.RUNNING, Machine.FRI, Machine.FRI)}

    with caplog.at_level(logging.WARNING):
        get_submitted_jobs(Machine.FRI, opt_jobs, {}, {}, [0, 0, 0])

    warning_texts = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert any("NODE_FAIL" in m for m in warning_texts), warning_texts
    assert any(job_dir in m for m in warning_texts), warning_texts
    # sacct logging does not alter job status — processing continues normally
    assert opt_jobs[job_dir].status == JobStatus.INCOMPLETE


@patch("automagician.process_job.subprocess")
def test_get_submitted_jobs_sacct_warns_on_oom(mock_subprocess, caplog):
    """A job that vanishes from squeue with an OUT_OF_MEMORY sacct record emits a WARNING."""
    job_dir = "/home/user/test_job_oom"

    mock_subprocess.check_output = _empty_squeue_mock()
    mock_subprocess.call = MagicMock(return_value=0)
    mock_subprocess.TimeoutExpired = _subprocess_real.TimeoutExpired
    mock_subprocess.run = MagicMock(
        return_value=_make_sacct_result(
            f"99999|OUT_OF_MEMORY|{job_dir}|0:125|0:125|||00:30:00\n"
        )
    )

    opt_jobs = {job_dir: OptJob(JobStatus.RUNNING, Machine.FRI, Machine.FRI)}

    with caplog.at_level(logging.WARNING):
        get_submitted_jobs(Machine.FRI, opt_jobs, {}, {}, [0, 0, 0])

    warning_texts = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert any("OUT_OF_MEMORY" in m for m in warning_texts), warning_texts
    assert any(job_dir in m for m in warning_texts), warning_texts


@patch("automagician.process_job.subprocess")
def test_get_submitted_jobs_sacct_no_warning_for_completed(mock_subprocess, caplog):
    """A job that vanishes from squeue but sacct reports COMPLETED emits no sacct WARNING."""
    job_dir = "/home/user/test_job_ok"

    mock_subprocess.check_output = _empty_squeue_mock()
    mock_subprocess.call = MagicMock(return_value=0)
    mock_subprocess.TimeoutExpired = _subprocess_real.TimeoutExpired
    mock_subprocess.run = MagicMock(
        return_value=_make_sacct_result(
            f"11111|COMPLETED|{job_dir}|0:0|0:0|||02:00:00\n"
        )
    )

    opt_jobs = {job_dir: OptJob(JobStatus.RUNNING, Machine.FRI, Machine.FRI)}

    with caplog.at_level(logging.WARNING):
        get_submitted_jobs(Machine.FRI, opt_jobs, {}, {}, [0, 0, 0])

    sacct_warnings = [
        r.message for r in caplog.records
        if r.levelno == logging.WARNING and "sacct:" in r.message
    ]
    assert sacct_warnings == [], sacct_warnings


@patch("automagician.process_job.subprocess")
def test_get_submitted_jobs_sacct_timeout_no_crash(mock_subprocess, caplog):
    """When sacct times out, get_submitted_jobs does not raise and emits no failure WARNING."""
    job_dir = "/home/user/test_job_sacct_timeout"

    mock_subprocess.check_output = _empty_squeue_mock()
    mock_subprocess.call = MagicMock(return_value=0)
    mock_subprocess.TimeoutExpired = _subprocess_real.TimeoutExpired
    mock_subprocess.run = MagicMock(
        side_effect=_subprocess_real.TimeoutExpired(["sacct"], 30)
    )

    opt_jobs = {job_dir: OptJob(JobStatus.RUNNING, Machine.FRI, Machine.FRI)}

    # Must not raise
    with caplog.at_level(logging.WARNING):
        get_submitted_jobs(Machine.FRI, opt_jobs, {}, {}, [0, 0, 0])

    sacct_failure_warnings = [
        r.message for r in caplog.records
        if r.levelno == logging.WARNING and "sacct:" in r.message
        and any(s in r.message for s in ("NODE_FAIL", "TIMEOUT", "FAILED", "OUT_OF_MEMORY", "BOOT_FAIL", "PREEMPTED"))
    ]
    assert sacct_failure_warnings == [], sacct_failure_warnings


@patch("automagician.process_job.subprocess")
def test_get_submitted_jobs_sacct_nonzero_exit_no_crash(mock_subprocess, caplog):
    """When sacct returns a non-zero exit code, no failure WARNING is emitted and no crash occurs."""
    job_dir = "/home/user/test_job_sacct_err"

    mock_subprocess.check_output = _empty_squeue_mock()
    mock_subprocess.call = MagicMock(return_value=0)
    mock_subprocess.TimeoutExpired = _subprocess_real.TimeoutExpired
    mock_subprocess.run = MagicMock(
        return_value=_make_sacct_result("", returncode=1)
    )

    opt_jobs = {job_dir: OptJob(JobStatus.RUNNING, Machine.FRI, Machine.FRI)}

    with caplog.at_level(logging.WARNING):
        get_submitted_jobs(Machine.FRI, opt_jobs, {}, {}, [0, 0, 0])

    sacct_failure_warnings = [
        r.message for r in caplog.records
        if r.levelno == logging.WARNING and "sacct:" in r.message
        and any(s in r.message for s in ("NODE_FAIL", "TIMEOUT", "FAILED", "OUT_OF_MEMORY", "BOOT_FAIL", "PREEMPTED"))
    ]
    assert sacct_failure_warnings == [], sacct_failure_warnings


@patch("automagician.process_job.subprocess")
def test_get_submitted_jobs_tacc_wav_running_on_current_machine(mock_subprocess):
    """A RUNNING wav job on the current TACC machine should count toward a queue
    slot (increment tacc_queue_sizes) and be reset to INCOMPLETE so it gets
    reprocessed next cycle.

    Before the fix, the condition checked INCOMPLETE instead of RUNNING, so
    running wav jobs were never reset and stayed stuck at RUNNING forever.
    """
    mock_subprocess.check_output = MagicMock(return_value="")
    mock_subprocess.call = MagicMock(return_value="")

    wav_job_dir = "/home/test_user/test_job"
    wav_jobs = {
        wav_job_dir: WavJob(
            opt_id=-1,
            wav_status=JobStatus.RUNNING,
            wav_last_on=Machine.STAMPEDE2_TACC,  # value 2, tacc_queue_sizes[0]
        )
    }
    opt_jobs = {}
    dos_jobs = {}
    tacc_queue_sizes = [0, 0, 0]

    get_submitted_jobs(Machine.STAMPEDE2_TACC, opt_jobs, dos_jobs, wav_jobs, tacc_queue_sizes)

    # Queue slot must be counted.
    assert tacc_queue_sizes == [1, 0, 0]
    # Job must be reset to INCOMPLETE so process_wav picks it up next cycle.
    assert wav_jobs[wav_job_dir].wav_status == JobStatus.INCOMPLETE


@patch("automagician.process_job.subprocess")
def test_get_submitted_jobs_tacc_wav_running_on_other_machine(mock_subprocess):
    """A RUNNING wav job on a *different* TACC machine should still count
    toward that machine's queue slot but must NOT be reset to INCOMPLETE
    (we can't verify its state from this machine).
    """
    mock_subprocess.check_output = MagicMock(return_value="")
    mock_subprocess.call = MagicMock(return_value="")

    wav_job_dir = "/home/test_user/test_job"
    wav_jobs = {
        wav_job_dir: WavJob(
            opt_id=-1,
            wav_status=JobStatus.RUNNING,
            wav_last_on=Machine.FRONTERA_TACC,  # value 3, tacc_queue_sizes[1]
        )
    }
    opt_jobs = {}
    dos_jobs = {}
    tacc_queue_sizes = [0, 0, 0]

    # Running on STAMPEDE2 but wav job is on FRONTERA.
    get_submitted_jobs(Machine.STAMPEDE2_TACC, opt_jobs, dos_jobs, wav_jobs, tacc_queue_sizes)

    # FRONTERA slot (index 1) incremented.
    assert tacc_queue_sizes == [0, 1, 0]
    # Status must remain RUNNING — we don't own this slot.
    assert wav_jobs[wav_job_dir].wav_status == JobStatus.RUNNING


@patch("automagician.process_job.subprocess")
def test_get_submitted_jobs_tacc_wav_incomplete_not_counted(mock_subprocess):
    """An INCOMPLETE wav job must NOT increment tacc_queue_sizes.

    Before the fix, the condition was inverted: INCOMPLETE jobs were counted
    and then promoted to RUNNING, silently inflating queue counts.
    """
    mock_subprocess.check_output = MagicMock(return_value="")
    mock_subprocess.call = MagicMock(return_value="")

    wav_job_dir = "/home/test_user/test_job"
    wav_jobs = {
        wav_job_dir: WavJob(
            opt_id=-1,
            wav_status=JobStatus.INCOMPLETE,
            wav_last_on=Machine.STAMPEDE2_TACC,
        )
    }
    opt_jobs = {}
    dos_jobs = {}
    tacc_queue_sizes = [0, 0, 0]

    get_submitted_jobs(Machine.STAMPEDE2_TACC, opt_jobs, dos_jobs, wav_jobs, tacc_queue_sizes)

    # Counter must stay zero — the job isn't running.
    assert tacc_queue_sizes == [0, 0, 0]
    # Status must remain INCOMPLETE — must not be promoted to RUNNING.
    assert wav_jobs[wav_job_dir].wav_status == JobStatus.INCOMPLETE
