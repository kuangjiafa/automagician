"""
Comprehensive tests to improve coverage for:
  - process_job.py
  - register.py
  - update_job.py
  - machine.py
  - finish_job.py
"""
import io
import os
import socket
import subprocess
from unittest.mock import MagicMock, patch

import pytest

import automagician.constants as constants
from automagician.classes import DosJob, JobStatus, Machine, OptJob, SSHConfig, WavJob
from automagician.finish_job import (
    dos_is_complete,
    give_certificate,
    optimizer_review,
    sc_is_complete,
    wav_is_complete,
    wrap_up,
)
from automagician.machine import (
    get_machine_name,
    get_machine_number,
    write_lockfile,
)
from automagician.process_job import (
    _get_submitted_jobs_slurm,
    check_error,
    determine_box_convergence,
    get_residueSFE,
    get_submitted_jobs,
    grep_ll_out_convergence,
    is_isif3,
    process_converged,
)
from automagician.register import exclude_regex
from automagician.update_job import (
    fix_error,
    get_error_message,
    log_error,
    switch_subfile,
)

# ---------------------------------------------------------------------------
# process_job — check_error
# ---------------------------------------------------------------------------

VASP_ERROR_MARKER = "I REFUSE TO CONTINUE WITH THIS SICK JOB"


def test_check_error_has_error(tmp_path):
    ll_out = tmp_path / "ll_out"
    ll_out.write_text(f"Some output\n {VASP_ERROR_MARKER}\nMore output\n")
    assert check_error(str(tmp_path)) is True


def test_check_error_no_error(tmp_path):
    ll_out = tmp_path / "ll_out"
    ll_out.write_text("reached required accuracy - stopping structural energy minimisation\n")
    assert check_error(str(tmp_path)) is False


def test_check_error_missing_ll_out(tmp_path):
    assert check_error(str(tmp_path)) is False


def test_check_error_empty_ll_out(tmp_path):
    ll_out = tmp_path / "ll_out"
    ll_out.write_text("")
    assert check_error(str(tmp_path)) is False


# ---------------------------------------------------------------------------
# process_job — grep_ll_out_convergence
# ---------------------------------------------------------------------------

CONVERGENCE_MSG = "reached required accuracy - stopping structural energy minimisation"


def test_grep_ll_out_convergence_converged(tmp_path):
    ll_out = tmp_path / "ll_out"
    ll_out.write_text(f"Some output\n {CONVERGENCE_MSG}\n")
    result = grep_ll_out_convergence(str(ll_out))
    assert result is True


def test_grep_ll_out_convergence_not_converged(tmp_path):
    ll_out = tmp_path / "ll_out"
    ll_out.write_text("Some output without the magic string\n")
    result = grep_ll_out_convergence(str(ll_out))
    assert result is False


def test_grep_ll_out_convergence_missing_file(tmp_path):
    result = grep_ll_out_convergence(str(tmp_path / "ll_out"))
    assert result is False


# ---------------------------------------------------------------------------
# process_job — is_isif3
# ---------------------------------------------------------------------------


def test_is_isif3_true(tmp_path):
    incar = tmp_path / "INCAR"
    incar.write_text("ENCUT = 500\nISIF = 3\nISMEAR = 0\n")
    assert is_isif3(str(tmp_path)) is True


def test_is_isif3_true_with_spaces(tmp_path):
    incar = tmp_path / "INCAR"
    incar.write_text("ISIF   =   3\n")
    assert is_isif3(str(tmp_path)) is True


def test_is_isif3_false_different_value(tmp_path):
    incar = tmp_path / "INCAR"
    incar.write_text("ENCUT = 500\nISIF = 2\nISMEAR = 0\n")
    assert is_isif3(str(tmp_path)) is False


def test_is_isif3_false_absent(tmp_path):
    incar = tmp_path / "INCAR"
    incar.write_text("ENCUT = 500\nISMEAR = 0\n")
    assert is_isif3(str(tmp_path)) is False


# ---------------------------------------------------------------------------
# process_job — determine_box_convergence
# ---------------------------------------------------------------------------


def test_determine_box_convergence_empty(tmp_path):
    fe_dat = tmp_path / "fe.dat"
    fe_dat.write_text("")
    assert determine_box_convergence(str(tmp_path)) is False


def test_determine_box_convergence_one_line(tmp_path):
    fe_dat = tmp_path / "fe.dat"
    fe_dat.write_text("-1.23456\n")
    assert determine_box_convergence(str(tmp_path)) is True


def test_determine_box_convergence_multiple_lines(tmp_path):
    fe_dat = tmp_path / "fe.dat"
    fe_dat.write_text("-1.23456\n-2.34567\n")
    assert determine_box_convergence(str(tmp_path)) is False


# ---------------------------------------------------------------------------
# process_job — get_residueSFE
# ---------------------------------------------------------------------------


def test_get_residueSFE_returns_zeros(tmp_path):
    result = get_residueSFE(str(tmp_path))
    assert result == (0, 0.0, 0.0)


# ---------------------------------------------------------------------------
# process_job — process_converged
# ---------------------------------------------------------------------------


def test_process_converged_sets_status(tmp_path):
    job_dir = str(tmp_path)
    opt_jobs = {job_dir: OptJob(JobStatus.INCOMPLETE, Machine.FRI, Machine.FRI)}
    process_converged(job_dir, opt_jobs)
    assert opt_jobs[job_dir].status == JobStatus.CONVERGED
    assert os.path.isfile(os.path.join(job_dir, constants.CONVERGENCE_CERTIFICATE_NAME))


def test_process_converged_idempotent(tmp_path):
    job_dir = str(tmp_path)
    opt_jobs = {job_dir: OptJob(JobStatus.INCOMPLETE, Machine.FRI, Machine.FRI)}
    process_converged(job_dir, opt_jobs)
    process_converged(job_dir, opt_jobs)  # second call with existing certificate
    assert opt_jobs[job_dir].status == JobStatus.CONVERGED


# ---------------------------------------------------------------------------
# process_job — _get_submitted_jobs_slurm
# ---------------------------------------------------------------------------

_SQUEUE_HEADER = "JOBID ST WORK_DIR"


def _make_squeue_output(*job_lines):
    return "\n".join([_SQUEUE_HEADER] + list(job_lines) + [""])


def test_get_submitted_jobs_slurm_opt_running(tmp_path):
    opt_dir = str(tmp_path / "opt_job")
    squeue_out = _make_squeue_output(f"123 R {opt_dir}")
    opt_jobs = {}
    dos_jobs = {}
    wav_jobs = {}
    with patch(
        "subprocess.check_output",
        return_value=squeue_out.encode(),
    ):
        _get_submitted_jobs_slurm(Machine.FRI, opt_jobs, dos_jobs, wav_jobs)

    assert opt_dir in opt_jobs
    assert opt_jobs[opt_dir].status == JobStatus.RUNNING


def test_get_submitted_jobs_slurm_opt_error_status(tmp_path):
    opt_dir = str(tmp_path / "opt_job")
    squeue_out = _make_squeue_output(f"123 F {opt_dir}")
    opt_jobs = {}
    dos_jobs = {}
    wav_jobs = {}
    with patch("subprocess.check_output", return_value=squeue_out.encode()):
        with patch("subprocess.call"):
            _get_submitted_jobs_slurm(Machine.FRI, opt_jobs, dos_jobs, wav_jobs)

    assert opt_jobs[opt_dir].status == JobStatus.ERROR


def test_get_submitted_jobs_slurm_dos_job(tmp_path):
    opt_dir = str(tmp_path / "job")
    dos_dir = opt_dir + "/dos"
    squeue_out = _make_squeue_output(f"124 R {dos_dir}")
    opt_jobs = {}
    dos_jobs = {}
    wav_jobs = {}
    with patch("subprocess.check_output", return_value=squeue_out.encode()):
        _get_submitted_jobs_slurm(Machine.FRI, opt_jobs, dos_jobs, wav_jobs)

    assert opt_dir in dos_jobs
    assert dos_jobs[opt_dir].dos_status == JobStatus.RUNNING


def test_get_submitted_jobs_slurm_wav_job(tmp_path):
    opt_dir = str(tmp_path / "job")
    wav_dir = opt_dir + "/wav"
    squeue_out = _make_squeue_output(f"125 R {wav_dir}")
    opt_jobs = {}
    dos_jobs = {}
    wav_jobs = {}
    with patch("subprocess.check_output", return_value=squeue_out.encode()):
        _get_submitted_jobs_slurm(Machine.FRI, opt_jobs, dos_jobs, wav_jobs)

    assert opt_dir in wav_jobs
    assert wav_jobs[opt_dir].wav_status == JobStatus.RUNNING


def test_get_submitted_jobs_slurm_sc_job(tmp_path):
    opt_dir = str(tmp_path / "job")
    sc_dir = opt_dir + "/sc"
    squeue_out = _make_squeue_output(f"126 R {sc_dir}")
    opt_jobs = {}
    dos_jobs = {}
    wav_jobs = {}
    with patch("subprocess.check_output", return_value=squeue_out.encode()):
        _get_submitted_jobs_slurm(Machine.FRI, opt_jobs, dos_jobs, wav_jobs)

    assert opt_dir in dos_jobs
    assert dos_jobs[opt_dir].sc_status == JobStatus.RUNNING


def test_get_submitted_jobs_slurm_updates_existing_opt_job(tmp_path):
    opt_dir = str(tmp_path / "opt_job")
    squeue_out = _make_squeue_output(f"123 R {opt_dir}")
    opt_jobs = {opt_dir: OptJob(JobStatus.INCOMPLETE, Machine.FRI, Machine.FRI)}
    dos_jobs = {}
    wav_jobs = {}
    with patch("subprocess.check_output", return_value=squeue_out.encode()):
        _get_submitted_jobs_slurm(Machine.FRI, opt_jobs, dos_jobs, wav_jobs)

    assert opt_jobs[opt_dir].status == JobStatus.RUNNING


# ---------------------------------------------------------------------------
# process_job — get_submitted_jobs (TACC and FRI paths)
# ---------------------------------------------------------------------------


def test_get_submitted_jobs_fri_resets_running(tmp_path):
    opt_dir = str(tmp_path / "opt_job")
    opt_jobs = {opt_dir: OptJob(JobStatus.RUNNING, Machine.FRI, Machine.FRI)}
    dos_jobs = {}
    wav_jobs = {}
    tacc_queue_sizes = [0, 0, 0]
    # Patch _get_submitted_jobs_slurm so it doesn't run squeue
    with patch("automagician.process_job._get_submitted_jobs_slurm"):
        get_submitted_jobs(Machine.FRI, opt_jobs, dos_jobs, wav_jobs, tacc_queue_sizes)

    # Status should be reset to INCOMPLETE before slurm query
    assert opt_jobs[opt_dir].status == JobStatus.INCOMPLETE


def test_get_submitted_jobs_fri_resets_dos_running(tmp_path):
    opt_dir = str(tmp_path / "job")
    dos_jobs = {
        opt_dir: DosJob(-1, JobStatus.RUNNING, JobStatus.RUNNING, Machine.FRI, Machine.FRI)
    }
    opt_jobs = {}
    wav_jobs = {}
    tacc_queue_sizes = [0, 0, 0]
    with patch("automagician.process_job._get_submitted_jobs_slurm"):
        get_submitted_jobs(Machine.FRI, opt_jobs, dos_jobs, wav_jobs, tacc_queue_sizes)

    assert dos_jobs[opt_dir].sc_status == JobStatus.INCOMPLETE
    assert dos_jobs[opt_dir].dos_status == JobStatus.INCOMPLETE


def test_get_submitted_jobs_fri_resets_wav_running(tmp_path):
    opt_dir = str(tmp_path / "job")
    wav_jobs = {opt_dir: WavJob(-1, JobStatus.RUNNING, Machine.FRI)}
    opt_jobs = {}
    dos_jobs = {}
    tacc_queue_sizes = [0, 0, 0]
    with patch("automagician.process_job._get_submitted_jobs_slurm"):
        get_submitted_jobs(Machine.FRI, opt_jobs, dos_jobs, wav_jobs, tacc_queue_sizes)

    assert wav_jobs[opt_dir].wav_status == JobStatus.INCOMPLETE


def test_get_submitted_jobs_tacc_counts_running(tmp_path):
    opt_dir = str(tmp_path / "opt_job")
    # STAMPEDE2_TACC is index 2; last_on - 2 = 0
    opt_jobs = {
        opt_dir: OptJob(JobStatus.RUNNING, Machine.STAMPEDE2_TACC, Machine.STAMPEDE2_TACC)
    }
    dos_jobs = {}
    wav_jobs = {}
    tacc_queue_sizes = [0, 0, 0]
    with patch("automagician.process_job._get_submitted_jobs_slurm"):
        get_submitted_jobs(
            Machine.STAMPEDE2_TACC, opt_jobs, dos_jobs, wav_jobs, tacc_queue_sizes
        )

    assert tacc_queue_sizes[0] == 1


# ---------------------------------------------------------------------------
# register — exclude_regex
# ---------------------------------------------------------------------------


def test_exclude_regex_run_dir():
    assert exclude_regex("/home/user/job/run0") is True
    assert exclude_regex("/home/user/job/run10") is True


def test_exclude_regex_dos_dir():
    assert exclude_regex("/home/user/job/dos") is True


def test_exclude_regex_wav_dir():
    assert exclude_regex("/home/user/job/wav") is True


def test_exclude_regex_ini_dir():
    assert exclude_regex("/home/user/job/ini") is True
    assert exclude_regex("/home/user/job/Ini") is True


def test_exclude_regex_fin_dir():
    assert exclude_regex("/home/user/job/fin") is True
    assert exclude_regex("/home/user/job/Fin") is True


def test_exclude_regex_sc_dir():
    assert exclude_regex("/home/user/job/sc") is True


def test_exclude_regex_normal_job():
    assert exclude_regex("/home/user/job") is False
    assert exclude_regex("/home/user/some_calc") is False


# ---------------------------------------------------------------------------
# update_job — get_error_message
# ---------------------------------------------------------------------------


def test_get_error_message_with_errors(tmp_path):
    ll_out = tmp_path / "ll_out"
    ll_out.write_text("Normal line\nERROR: something went wrong\nAnother line\n")
    result = get_error_message(str(tmp_path))
    assert result == ["ERROR: something went wrong"]


def test_get_error_message_lowercase_error(tmp_path):
    ll_out = tmp_path / "ll_out"
    ll_out.write_text("This is a normal error in the system\n")
    result = get_error_message(str(tmp_path))
    assert result == ["This is a normal error in the system"]


def test_get_error_message_no_errors(tmp_path):
    ll_out = tmp_path / "ll_out"
    ll_out.write_text("Normal output\nAll is fine\n")
    result = get_error_message(str(tmp_path))
    assert result == []


def test_get_error_message_missing_ll_out(tmp_path):
    result = get_error_message(str(tmp_path))
    assert result == []


# ---------------------------------------------------------------------------
# update_job — log_error
# ---------------------------------------------------------------------------


def test_log_error_writes_to_error_log(tmp_path):
    job_dir = tmp_path / "job"
    home_dir = tmp_path / "home"
    job_dir.mkdir()
    home_dir.mkdir()
    ll_out = job_dir / "ll_out"
    ll_out.write_text("ERRORs: something bad happened\n")

    log_error(str(job_dir), str(home_dir))

    error_log = home_dir / "error_log.dat"
    assert error_log.exists()
    content = error_log.read_text()
    assert str(job_dir) in content
    assert "ERRORs: something bad happened" in content


def test_log_error_no_errors_in_ll_out(tmp_path):
    job_dir = tmp_path / "job"
    home_dir = tmp_path / "home"
    job_dir.mkdir()
    home_dir.mkdir()
    ll_out = job_dir / "ll_out"
    ll_out.write_text("Normal output\n")

    log_error(str(job_dir), str(home_dir))

    error_log = home_dir / "error_log.dat"
    # File is created but empty since no errors found
    assert error_log.exists()
    assert error_log.read_text() == ""


def test_log_error_appends_to_existing_log(tmp_path):
    job_dir = tmp_path / "job"
    home_dir = tmp_path / "home"
    job_dir.mkdir()
    home_dir.mkdir()
    error_log = home_dir / "error_log.dat"
    error_log.write_text("Existing entry\n")
    ll_out = job_dir / "ll_out"
    ll_out.write_text("ERROR: new error\n")

    log_error(str(job_dir), str(home_dir))

    content = error_log.read_text()
    assert "Existing entry" in content
    assert "new error" in content


# ---------------------------------------------------------------------------
# update_job — fix_error
# ---------------------------------------------------------------------------


def test_fix_error_zbrent_no_contcar(tmp_path):
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    ll_out = job_dir / "ll_out"
    ll_out.write_text("ERROR ZBRENT: fatal error in bracketing\n")
    result = fix_error(str(job_dir))
    assert result is False


def test_fix_error_zbrent_empty_contcar(tmp_path):
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    ll_out = job_dir / "ll_out"
    ll_out.write_text("ERROR ZBRENT: fatal error in bracketing\n")
    contcar = job_dir / "CONTCAR"
    contcar.write_text("")  # empty CONTCAR
    result = fix_error(str(job_dir))
    assert result is False


def test_fix_error_zbrent_with_contcar(tmp_path):
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    ll_out = job_dir / "ll_out"
    ll_out.write_text("ERROR ZBRENT: fatal error in bracketing\n")
    contcar = job_dir / "CONTCAR"
    contcar.write_text("H\n1\n")  # non-empty CONTCAR
    with patch("automagician.update_job.finish_job.wrap_up") as mock_wrap_up:
        result = fix_error(str(job_dir))
    assert result is True
    mock_wrap_up.assert_called_once_with(str(job_dir))


def test_fix_error_no_known_error(tmp_path):
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    ll_out = job_dir / "ll_out"
    ll_out.write_text("Normal output\n")
    result = fix_error(str(job_dir))
    assert result is False


def test_fix_error_no_ll_out(tmp_path):
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    result = fix_error(str(job_dir))
    assert result is False


# ---------------------------------------------------------------------------
# update_job — switch_subfile (no-subfile path)
# ---------------------------------------------------------------------------


def test_switch_subfile_no_subfile(tmp_path):
    # When the subfile doesn't exist the function should return without doing anything
    job_dir = str(tmp_path)
    cwd_before = os.getcwd()
    switch_subfile(job_dir, "halifax.sub", "fri.sub", Machine.FRI)
    assert os.getcwd() == cwd_before  # CWD restored


def test_switch_subfile_restores_cwd(tmp_path):
    job_dir = str(tmp_path)
    cwd_before = os.getcwd()
    # Even if something inside switch_subfile fails, CWD should be restored
    with patch("subprocess.call"):
        switch_subfile(job_dir, "halifax.sub", "fri.sub", Machine.FRI)
    assert os.getcwd() == cwd_before


# ---------------------------------------------------------------------------
# machine — get_machine_name
# ---------------------------------------------------------------------------


def test_get_machine_name_fri():
    assert get_machine_name(Machine.FRI) == "fri.cm.utexas.edu"


def test_get_machine_name_halifax():
    assert get_machine_name(Machine.HALIFAX) == "halifax.cm.utexas.edu"


def test_get_machine_name_stampede2():
    assert get_machine_name(Machine.STAMPEDE2_TACC) == "stampede2.tacc.utexas.edu"


def test_get_machine_name_frontera():
    assert get_machine_name(Machine.FRONTERA_TACC) == "frontera.tacc.utexas.edu"


def test_get_machine_name_ls6():
    assert get_machine_name(Machine.LS6_TACC) == "ls6.tacc.utexas.edu"


def test_get_machine_name_unknown():
    assert get_machine_name(Machine.UNKNOWN) == "localhost"


# ---------------------------------------------------------------------------
# machine — get_machine_number
# ---------------------------------------------------------------------------


def test_get_machine_number_fri():
    with patch("socket.gethostname", return_value="fri.cm.utexas.edu"):
        result = get_machine_number()
    assert result == Machine.FRI


def test_get_machine_number_halifax():
    with patch("socket.gethostname", return_value="halifax.cm.utexas.edu"):
        result = get_machine_number()
    assert result == Machine.HALIFAX


def test_get_machine_number_stampede2():
    with patch("socket.gethostname", return_value="stampede2.tacc.utexas.edu"):
        result = get_machine_number()
    assert result == Machine.STAMPEDE2_TACC


def test_get_machine_number_login_node():
    """Login nodes have 'login<N>.' prefix that should be stripped."""
    with patch("socket.gethostname", return_value="login1.fri.cm.utexas.edu"):
        result = get_machine_number()
    assert result == Machine.FRI


def test_get_machine_number_unknown():
    with patch("socket.gethostname", return_value="someother.server.edu"):
        result = get_machine_number()
    assert result == Machine.UNKNOWN


# ---------------------------------------------------------------------------
# machine — write_lockfile (NoSSH path, no existing lockfile)
# ---------------------------------------------------------------------------


def test_write_lockfile_creates_lockfile(tmp_path, monkeypatch):
    lock_dir = str(tmp_path / "lockdir")
    lock_file = os.path.join(lock_dir, "lock")
    monkeypatch.setattr(constants, "LOCK_DIR", lock_dir)
    monkeypatch.setattr(constants, "LOCK_FILE", lock_file)

    ssh_config = SSHConfig(config="NoSSH")
    write_lockfile(ssh_config, Machine.STAMPEDE2_TACC)

    assert os.path.isfile(lock_file)
    content = open(lock_file).read()
    assert "machine" in content
    assert "pid" in content


def test_write_lockfile_exits_if_lockfile_exists(tmp_path, monkeypatch):
    lock_dir = str(tmp_path / "lockdir")
    os.makedirs(lock_dir, mode=0o700)
    lock_file = os.path.join(lock_dir, "lock")
    open(lock_file, "w").close()  # create existing lockfile

    monkeypatch.setattr(constants, "LOCK_DIR", lock_dir)
    monkeypatch.setattr(constants, "LOCK_FILE", lock_file)

    ssh_config = SSHConfig(config="NoSSH")
    with patch("automagician.machine.subprocess.call"):
        with pytest.raises(SystemExit):
            write_lockfile(ssh_config, Machine.STAMPEDE2_TACC)


def test_write_lockfile_creates_lockdir_if_missing(tmp_path, monkeypatch):
    lock_dir = str(tmp_path / "new_lockdir")
    lock_file = os.path.join(lock_dir, "lock")
    monkeypatch.setattr(constants, "LOCK_DIR", lock_dir)
    monkeypatch.setattr(constants, "LOCK_FILE", lock_file)

    ssh_config = SSHConfig(config="NoSSH")
    write_lockfile(ssh_config, Machine.STAMPEDE2_TACC)

    assert os.path.isdir(lock_dir)


def test_write_lockfile_raises_permission_error_if_not_owned(tmp_path, monkeypatch):
    lock_dir = str(tmp_path / "lockdir")
    os.makedirs(lock_dir, mode=0o700)
    lock_file = os.path.join(lock_dir, "lock")

    monkeypatch.setattr(constants, "LOCK_DIR", lock_dir)
    monkeypatch.setattr(constants, "LOCK_FILE", lock_file)

    with patch("os.stat") as mock_stat:
        mock_stat_result = MagicMock()
        mock_stat_result.st_uid = os.getuid() + 1  # different owner
        mock_stat_result.st_mode = 0o40700  # dir with 700
        mock_stat.return_value = mock_stat_result

        ssh_config = SSHConfig(config="NoSSH")
        with pytest.raises(PermissionError):
            write_lockfile(ssh_config, Machine.STAMPEDE2_TACC)


# ---------------------------------------------------------------------------
# finish_job — optimizer_review (stub function)
# ---------------------------------------------------------------------------


def test_optimizer_review_returns_none(tmp_path):
    result = optimizer_review(str(tmp_path))
    assert result is None


# ---------------------------------------------------------------------------
# finish_job — wrap_up (mock vfin.pl subprocess, verify CWD is restored)
# ---------------------------------------------------------------------------


def _make_job_dir(tmp_path):
    """Create a minimal job directory with ll_out."""
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    (job_dir / "ll_out").write_text("some output\n")
    return str(job_dir)


def test_wrap_up_no_previous_runs_restores_cwd(tmp_path):
    cwd_before = os.getcwd()
    job_dir = _make_job_dir(tmp_path)
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)
        # wrap_up calls shutil.move after subprocess.run; ll_out -> run0
        try:
            wrap_up(job_dir)
        except Exception:
            pass
    # CWD must always be restored
    assert os.getcwd() == cwd_before


def test_wrap_up_subprocess_raises_restores_cwd(tmp_path):
    """Even if vfin.pl raises FileNotFoundError, CWD must be restored."""
    cwd_before = os.getcwd()
    job_dir = _make_job_dir(tmp_path)
    with patch("subprocess.run", side_effect=FileNotFoundError("vfin.pl")):
        with pytest.raises(FileNotFoundError):
            wrap_up(job_dir)
    assert os.getcwd() == cwd_before


def test_wrap_up_creates_run0_dir(tmp_path):
    job_dir = _make_job_dir(tmp_path)

    def fake_run(cmd, stdout, stderr):
        # vfin.pl would create run0; simulate that
        run_dir = os.path.join(job_dir, cmd[1])
        os.makedirs(run_dir, exist_ok=True)

    with patch("subprocess.run", side_effect=fake_run):
        wrap_up(job_dir)

    assert os.path.isdir(os.path.join(job_dir, "run0"))
    assert os.path.isfile(os.path.join(job_dir, "run0", "ll_out"))


def test_wrap_up_increments_run_number(tmp_path):
    job_dir = _make_job_dir(tmp_path)
    os.makedirs(os.path.join(job_dir, "run0"))
    os.makedirs(os.path.join(job_dir, "run1"))

    def fake_run(cmd, stdout, stderr):
        run_dir = os.path.join(job_dir, cmd[1])
        os.makedirs(run_dir, exist_ok=True)

    with patch("subprocess.run", side_effect=fake_run):
        wrap_up(job_dir)

    assert os.path.isdir(os.path.join(job_dir, "run2"))
    assert os.path.isfile(os.path.join(job_dir, "run2", "ll_out"))


def test_wrap_up_handles_non_numeric_run_dir(tmp_path):
    """A directory named 'run' (no number) should not break the increment logic."""
    job_dir = _make_job_dir(tmp_path)
    os.makedirs(os.path.join(job_dir, "run"))  # no numeric suffix

    def fake_run(cmd, stdout, stderr):
        run_dir = os.path.join(job_dir, cmd[1])
        os.makedirs(run_dir, exist_ok=True)

    with patch("subprocess.run", side_effect=fake_run):
        wrap_up(job_dir)

    # The 'run' dir has no number so largest_number should be 0, creating run0
    assert os.path.isdir(os.path.join(job_dir, "run0"))
