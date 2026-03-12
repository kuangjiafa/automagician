import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Ensure src is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import automagician.constants as constants
from automagician.classes import Machine, SSHConfig
from automagician.machine import write_lockfile


class TestSecureLockfile(unittest.TestCase):
    @patch("automagician.machine.os.makedirs")
    @patch("automagician.machine.subprocess.run")
    @patch("automagician.machine.os.path.isdir")
    @patch(
        "automagician.machine.exists"
    )  # Patch exists from os.path imported as exists in machine.py
    def test_write_lockfile_secure_creation(
        self, mock_exists, mock_isdir, mock_subprocess_run, mock_makedirs
    ):
        # Setup mocks
        mock_isdir.return_value = False  # Lock directory does not exist locally
        mock_exists.return_value = False  # Lock file does not exist locally

        mock_ssh = MagicMock()
        mock_ssh_config = MagicMock()
        mock_ssh_config.config = MagicMock()
        mock_ssh_config.config.ssh = mock_ssh

        # Configure ssh.run side effect to handle different commands
        def ssh_run_side_effect(*args, **kwargs):
            cmd = args[0]
            result = MagicMock()
            if "mkdir" in cmd or "stat -c" in cmd:
                # Directory setup command
                result.ok = True
                result.stdout = ""
                result.stderr = ""
            elif "test -e" in cmd:
                # Check for lock file existence
                result.ok = False  # Does not exist
            else:
                result.ok = True
            return result

        mock_ssh.run.side_effect = ssh_run_side_effect

        # Mock open for writing lock file
        with patch("builtins.open", new_callable=MagicMock):
            write_lockfile(mock_ssh_config, Machine.FRI)

        # --- Local Security Checks ---

        # Verify that chmod 777 was NOT called
        for call in mock_subprocess_run.call_args_list:
            args = call[0][0]
            if "chmod" in args and "777" in args:
                self.fail("Security Vulnerability: chmod 777 called on lock directory")

        # Verify that os.makedirs was called with secure mode (0o700)
        secure_mode_found = False
        for call in mock_makedirs.call_args_list:
            # Check kwargs for mode
            if "mode" in call.kwargs and call.kwargs["mode"] == 0o700:
                secure_mode_found = True
            # Check args position 1 for mode
            elif len(call.args) > 1 and call.args[1] == 0o700:
                secure_mode_found = True

        if not secure_mode_found:
            self.fail(
                "Security Vulnerability: os.makedirs not called with secure mode (0o700)"
            )

        # --- Remote Security Checks ---

        # Check that remote command includes secure mkdir and ownership check
        secure_remote_mkdir_found = False
        secure_remote_ownership_check_found = False

        for call in mock_ssh.run.call_args_list:
            cmd = call[0][0]
            # Check for mkdir with mode
            if "mkdir" in cmd and ("-m 700" in cmd or "-m=700" in cmd):
                secure_remote_mkdir_found = True

            # Check for ownership verification
            if "stat -c" in cmd and "$(id -u)" in cmd:
                secure_remote_ownership_check_found = True

        if not secure_remote_mkdir_found:
            self.fail(
                "Security Vulnerability: Remote command is missing secure mkdir (-m 700)"
            )

        if not secure_remote_ownership_check_found:
            self.fail(
                "Security Vulnerability: Remote command is missing ownership check (stat -c ... id -u)"
            )


if __name__ == "__main__":
    unittest.main()
