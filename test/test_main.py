# pylint: disable=all
from unittest.mock import MagicMock, patch

from automagician.main import main_wrapper, set_up_parser


def make_args(**kwargs):
    """Build a default args namespace, overriding with any kwargs."""
    parser = set_up_parser()
    args = parser.parse_args([])
    for k, v in kwargs.items():
        setattr(args, k, v)
    return args


def _mock_database():
    """Return a MagicMock shaped like Database with empty job dicts."""
    db = MagicMock()
    db.get_opt_jobs.return_value = {}
    db.get_dos_jobs.return_value = {}
    db.get_wav_jobs.return_value = {}
    db.db.execute.return_value = []
    return db


# ---------------------------------------------------------------------------
# Phase 1 (initialization) failure tests
# ---------------------------------------------------------------------------


@patch("automagician.main.machine_file.get_machine_number", side_effect=RuntimeError("no machine"))
def test_init_failure_get_machine_returns_cleanly(mock_get_machine):
    """An init failure must return cleanly, not raise UnboundLocalError."""
    args = make_args()
    # Before the fix this raised UnboundLocalError in the outer except handler.
    main_wrapper(args)  # must not raise


@patch("automagician.main.machine_file.automagic_exit")
@patch("automagician.main.machine_file.get_machine_number", side_effect=KeyError("HOME"))
def test_init_failure_does_not_call_automagic_exit(mock_get_machine, mock_exit):
    """When the lockfile was never written, automagic_exit must not be called."""
    args = make_args()
    main_wrapper(args)
    mock_exit.assert_not_called()


@patch("automagician.main.process_job.submit_queue")
@patch("automagician.main.machine_file.get_machine_number", side_effect=OSError("db gone"))
def test_init_failure_does_not_submit_queue(mock_get_machine, mock_submit):
    """When init fails there is nothing to submit."""
    args = make_args()
    main_wrapper(args)
    mock_submit.assert_not_called()


@patch("automagician.main.machine_file.automagic_exit")
@patch("automagician.main.machine_file.write_lockfile")
@patch("automagician.main.machine_file.ssh_scp_init")
@patch("automagician.main.machine_file.get_machine_number", return_value=0)
def test_init_failure_after_lockfile_calls_automagic_exit(
    mock_machine, mock_ssh_init, mock_lockfile, mock_exit, tmp_path
):
    """If init fails after the lockfile is written, automagic_exit must release it."""
    mock_ssh = MagicMock()
    mock_ssh.config = "NoSSH"
    mock_ssh_init.return_value = mock_ssh
    # Database raises — lockfile was already written at this point.
    with patch("automagician.main.Database", side_effect=RuntimeError("db failed")):
        with patch.dict("os.environ", {"HOME": str(tmp_path)}):
            args = make_args()
            main_wrapper(args)
    mock_exit.assert_called_once()


# ---------------------------------------------------------------------------
# Phase 2 (execution) failure tests
# ---------------------------------------------------------------------------


@patch("automagician.main.machine_file.automagic_exit")
@patch("automagician.main.process_job.submit_queue")
@patch("automagician.main.process_job.gone_job_check")
@patch("automagician.main.process_job.get_submitted_jobs")
@patch("automagician.main.Database")
@patch("automagician.main.machine_file.write_lockfile")
@patch("automagician.main.machine_file.ssh_scp_init")
@patch("automagician.main.machine_file.get_machine_number", return_value=0)
def test_execution_failure_in_finally_does_not_raise(
    mock_machine,
    mock_ssh_init,
    mock_lockfile,
    MockDatabase,
    mock_get_submitted,
    mock_gone,
    mock_submit,
    mock_exit,
    tmp_path,
):
    """If submit_queue raises in the inner finally, the outer except must handle
    it without UnboundLocalError — all Phase 1 vars are defined.

    submit_queue fails on the first call (inner finally) then succeeds on the
    second call (outer except recovery), which is the realistic failure mode.
    """
    mock_ssh = MagicMock()
    mock_ssh.config = "NoSSH"
    mock_ssh_init.return_value = mock_ssh
    MockDatabase.return_value = _mock_database()

    # Fail on first call (inner finally), succeed on second (outer except).
    mock_submit.side_effect = [RuntimeError("sbatch gone"), None]

    args = make_args()
    with patch.dict("os.environ", {"HOME": str(tmp_path)}):
        main_wrapper(args)  # must not raise


@patch("automagician.main.machine_file.automagic_exit")
@patch("automagician.main.process_job.submit_queue")
@patch("automagician.main.process_job.gone_job_check")
@patch("automagician.main.process_job.get_submitted_jobs")
@patch("automagician.main.Database")
@patch("automagician.main.machine_file.write_lockfile")
@patch("automagician.main.machine_file.ssh_scp_init")
@patch("automagician.main.machine_file.get_machine_number", return_value=0)
def test_execution_failure_calls_automagic_exit(
    mock_machine,
    mock_ssh_init,
    mock_lockfile,
    MockDatabase,
    mock_get_submitted,
    mock_gone,
    mock_submit,
    mock_exit,
    tmp_path,
):
    """After a Phase 2 failure the lockfile must always be released."""
    mock_ssh = MagicMock()
    mock_ssh.config = "NoSSH"
    mock_ssh_init.return_value = mock_ssh
    MockDatabase.return_value = _mock_database()
    mock_submit.side_effect = [RuntimeError("sbatch gone"), None]

    args = make_args()
    with patch.dict("os.environ", {"HOME": str(tmp_path)}):
        main_wrapper(args)

    mock_exit.assert_called()
