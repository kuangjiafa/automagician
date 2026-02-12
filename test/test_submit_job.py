# pylint: disable=all
import os
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
        ["squeue", "-u", os.environ["USER"], "-o", "%A %t %Z"]
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
def test_get_submitted_jobs_tacc_dos_queue_accounting(monkeypatch):
    """Test DOS queue accounting on TACC machines uses dos_last_on field.
    
    This test verifies:
    1. DOS jobs with RUNNING dos_status increment tacc_queue_sizes correctly
    2. The dos_last_on field is used (not opt_jobs[..].last_on)
    3. No KeyError occurs when opt_jobs lacks the DOS job entry
    """
    # Mock subprocess to return empty squeue output (no jobs in queue)
    monkeypatch.check_output = MagicMock(return_value="")
    monkeypatch.call = MagicMock(return_value="")
    
    # Setup: DOS job on STAMPEDE2 (machine 2) without a matching opt_job
    dos_job_dir = "/home/test_user/test_job/dos"
    dos_jobs = {
        dos_job_dir: DosJob(
            opt_id=123,
            sc_status=JobStatus.INCOMPLETE,
            dos_status=JobStatus.RUNNING,  # This is running
            sc_last_on=Machine.FRI,  # SC was on FRI
            dos_last_on=Machine.STAMPEDE2_TACC,  # DOS is on STAMPEDE2 (index 2, so tacc_queue_sizes[0])
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
def test_get_submitted_jobs_tacc_dos_queue_accounting_with_sc(monkeypatch):
    """Test DOS queue accounting on TACC with both SC and DOS running."""
    # Mock subprocess to return empty squeue output
    monkeypatch.check_output = MagicMock(return_value="")
    monkeypatch.call = MagicMock(return_value="")
    
    dos_job_dir = "/home/test_user/test_job/dos"
    dos_jobs = {
        dos_job_dir: DosJob(
            opt_id=456,
            sc_status=JobStatus.RUNNING,  # SC is also running
            dos_status=JobStatus.RUNNING,  # DOS is running
            sc_last_on=Machine.FRONTERA_TACC,  # SC on FRONTERA (index 3, tacc_queue_sizes[1])
            dos_last_on=Machine.LS6_TACC,  # DOS on LS6 (index 4, tacc_queue_sizes[2])
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
