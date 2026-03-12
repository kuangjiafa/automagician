import sys
from unittest.mock import MagicMock
import pytest
import shlex

# Mock fabric before importing anything else
mock_fabric = MagicMock()
sys.modules["fabric"] = mock_fabric
sys.modules["fabric.transfer"] = MagicMock()
sys.modules["fabric.connection"] = MagicMock()

# Mock SshScp if it cannot be imported (because fabric is missing)
try:
    from automagician.classes import SshScp
except (ImportError, NameError):
    from dataclasses import dataclass
    @dataclass
    class SshScp:
        ssh: MagicMock
        scp: MagicMock

from automagician.machine import scp_get_dir

def test_scp_get_dir_machine_injection():
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

    expected_command_start = "cd " + shlex.quote(remote_path)
    # We also updated ; to &&
    expected_command_full = "cd " + shlex.quote(remote_path) + " && find . -type f | cut -c 2-"

    assert command == expected_command_full
