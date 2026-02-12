from automagician.classes import Machine, SSHConfig
from automagician.machine import *
import logging
import unittest.mock
import tempfile
import shlex
import pytest

def test_is_oden():
    assert is_oden(Machine.FRI)
    assert is_oden(Machine.HALIFAX)
    assert not is_oden(Machine.UNKNOWN)
    assert not is_oden(Machine.FRONTERA_TACC)
    assert not is_oden(Machine.LS6_TACC)
    assert not is_oden(Machine.STAMPEDE2_TACC)


def test_is_tacc():
    assert not is_tacc(Machine.FRI)
    assert not is_tacc(Machine.HALIFAX)
    assert not is_tacc(Machine.UNKNOWN)
    assert is_tacc(Machine.FRONTERA_TACC)
    assert is_tacc(Machine.LS6_TACC)
    assert is_tacc(Machine.STAMPEDE2_TACC)

def test_ssh_scp_init():
    logger = logging.Logger("test")
    ssh_config = ssh_scp_init(
        machine = get_machine_number(),
        home_dir = os.environ["HOME"],
        balance = True,
        logger = logger
    )

    no_balance = ssh_scp_init(
        machine=get_machine_number(),
        home_dir=os.environ["HOME"],
        balance=False,
        logger=logger
    )

    no_fabric = True
    wo_fabric = ssh_scp_init(
        machine=Machine.FRI,
        home_dir=os.environ["HOME"],
        balance=True,
        logger=logger
    )

    assert isinstance(ssh_config, SSHConfig)
    assert isinstance(no_balance, SSHConfig)
    assert isinstance(wo_fabric, SSHConfig)
    assert no_balance.config == "NoSSH"
    assert wo_fabric.config == "NoSSH"


def test_scp_put_dir_quotes_paths():
    # Mock SSHConfig to match real object structure
    # SSHConfig has a .config field which is either "NoSSH" or SshScp(ssh, scp)
    mock_ssh_config = unittest.mock.Mock(spec=SSHConfig)
    mock_ssh_scp = unittest.mock.Mock()
    mock_ssh_scp.ssh = unittest.mock.Mock()
    mock_ssh_scp.scp = unittest.mock.Mock()
    mock_ssh_config.config = mock_ssh_scp

    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a directory with a name that could cause injection if unquoted
        malicious_name = "; echo 'hacked'"
        os.makedirs(os.path.join(temp_dir, malicious_name))

        # Create a file inside it so `find` finds something
        file_path = os.path.join(temp_dir, malicious_name, "file.txt")
        with open(file_path, "w") as f:
            f.write("content")

        # Call scp_put_dir
        # remote path base
        remote_base = "remote_dir"
        scp_put_dir(temp_dir, remote_base, mock_ssh_config)

        # Check calls to ssh.run
        # We expect mkdir -p for the directory
        found_mkdir = False

        # Calculate expected quoted path
        # scp_put_dir does: dirname = os.path.dirname(remote + f[1:])
        # find returns paths relative to `.` e.g. `./; echo 'hacked'/file.txt`
        # f[1:] is `/<malicious_name>/file.txt`
        # remote + f[1:] is `remote_dir/<malicious_name>/file.txt`
        # dirname is `remote_dir/<malicious_name>`

        # So we expect: mkdir -p <quoted_dirname>

        expected_dirname = os.path.dirname(remote_base + f"/{malicious_name}/file.txt")
        expected_cmd = "mkdir -p " + shlex.quote(expected_dirname)

        for call in mock_ssh_config.config.ssh.run.call_args_list:
            args, _ = call
            cmd = args[0]
            if "mkdir -p" in cmd:
                # Check if it matches exactly the quoted command
                if cmd == expected_cmd:
                    found_mkdir = True
                    break

        assert found_mkdir, f"Expected command '{expected_cmd}' not found in calls: {mock_ssh_config.config.ssh.run.call_args_list}"


def test_scp_put_dir_raises_on_nossh():
    """Test that scp_put_dir raises ValueError when ssh_config.config is 'NoSSH'"""
    # Create a mock SSHConfig with config set to "NoSSH"
    mock_ssh_config = unittest.mock.Mock(spec=SSHConfig)
    mock_ssh_config.config = "NoSSH"
    
    # Create a temporary directory to use as local path
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a simple file so the directory is not empty
        file_path = os.path.join(temp_dir, "test.txt")
        with open(file_path, "w") as f:
            f.write("test content")
        
        # Verify that calling scp_put_dir with NoSSH config raises ValueError
        with pytest.raises(
            ValueError, match="SSH configuration is required for scp_put_dir"
        ):
            scp_put_dir(temp_dir, "remote_dir", mock_ssh_config)
