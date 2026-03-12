import sys
from unittest.mock import MagicMock

import pytest

# Mock fabric before importing anything else
mock_fabric = MagicMock()
sys.modules["fabric"] = mock_fabric
sys.modules["fabric.transfer"] = MagicMock()
sys.modules["fabric.connection"] = MagicMock()

try:
    from automagician.classes import SshScp
    from automagician.small_functions import classify_job_dir, get_opt_dir, scp_get_dir
except ImportError:
    from automagician.small_functions import (
        archive_converged,
        classify_job_dir,
        get_opt_dir,
    )



def test_classify_job_dir():
    assert classify_job_dir("/home/user/dos") == "dos"
    assert classify_job_dir("/home/user/sc") == "sc"
    assert classify_job_dir("/home/user/wav") == "wav"
    assert classify_job_dir("/home/user/opt") == "opt"
    assert classify_job_dir("/home/user/dos/other") == "opt"


def test_get_opt_dir():
    assert get_opt_dir("/home/user/dos") == "/home/user"
    assert get_opt_dir("/home/user/sc") == "/home/user"
    assert get_opt_dir("/home/user/wav") == "/home/user"
    assert get_opt_dir("/home/user/opt") == "/home/user/opt"


def test_scp_get_dir_injection():
    # If scp_get_dir is not defined (import failed), skip
    if "scp_get_dir" not in globals():
        pytest.skip("scp_get_dir not available")

    mock_ssh = MagicMock()
    mock_scp = MagicMock()
    # Mock return value of ssh.run
    mock_result = MagicMock()
    mock_result.stdout = "file1\nfile2"
    mock_ssh.run.return_value = mock_result

    mock_ssh_scp = SshScp(ssh=mock_ssh, scp=mock_scp)

    remote_path = "remote/path; rm -rf /"
    local_path = "local/path"

    scp_get_dir(remote_path, local_path, mock_ssh_scp)

    # Check if the command passed to ssh.run was escaped
    args, _ = mock_ssh.run.call_args
    command = args[0]
    print(f"Command executed: {command}")

    import shlex

    expected_command_start = "cd " + shlex.quote(remote_path)

    assert command.startswith(expected_command_start)


def test_archive_converged(tmp_path):
    if "archive_converged" not in globals():
        pytest.skip("archive_converged not available")

    # Setup files
    converged_file = tmp_path / "converged_jobs.dat"
    converged_file.write_text("dummy_job_data\n")

    archive_file = tmp_path / "archive_converged.dat"

    # Call function
    archive_converged(str(tmp_path))

    # Assert source is gone and destination exists
    assert not converged_file.exists()
    assert archive_file.exists()
    assert archive_file.read_text() == "dummy_job_data\n"

    # Assert raises FileNotFoundError if file is missing
    with pytest.raises(FileNotFoundError):
        archive_converged(str(tmp_path))
