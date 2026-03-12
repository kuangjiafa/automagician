import os
import unittest
from unittest.mock import MagicMock, patch

import automagician.constants as constants


class TestSecurityRepro(unittest.TestCase):
    @patch("automagician.machine.exists")
    @patch("automagician.machine.os.makedirs")
    @patch("automagician.machine.subprocess.run")
    @patch("automagician.machine.open", create=True)
    def test_write_lockfile_command_injection_fixed(
        self, mock_open, mock_subprocess_run, mock_makedirs, mock_exists
    ):
        # We need to import things inside the test because we'll be patching things
        from automagician.classes import Machine
        from automagician.machine import write_lockfile

        # Setup
        mock_exists.return_value = False
        mock_ssh = MagicMock()

        # We need it to return different things for different calls
        def mock_run_side_effect(cmd, **kwargs):
            print(f"Mock SSH run: {cmd}")
            res = MagicMock()
            if "test -d" in cmd:
                res.ok = True
            elif "test -e" in cmd:
                res.ok = False
            return res

        mock_ssh.run.side_effect = mock_run_side_effect

        # Manually construct an object that looks like SSHConfig with a SshScp-like config
        class MockConfig:
            pass

        ssh_config = MockConfig()
        ssh_scp = MockConfig()
        ssh_scp.ssh = mock_ssh
        ssh_config.config = ssh_scp

        # Malicious USER environment variable
        malicious_user = 'user"; touch /tmp/pwned; echo "'
        with patch.dict(os.environ, {"USER": malicious_user}):
            # We need to reload constants because it's already imported and uses os.environ['USER']
            import importlib

            importlib.reload(constants)

            write_lockfile(ssh_config, Machine.FRI)

            # Check the command passed to ssh.run
            all_cmds = [call[0][0] for call in mock_ssh.run.call_args_list]
            echo_cmds = [cmd for cmd in all_cmds if cmd.startswith("echo")]

            self.assertTrue(len(echo_cmds) > 0, f"No echo command found in {all_cmds}")
            command = echo_cmds[0]
            print(f"\nFinal echo command: {command}")

            # If fixed, the malicious payload should be inside a quoted string
            # shlex.quote uses single quotes by default if there are no single quotes in the string
            # Our malicious_user has double quotes but no single quotes.
            self.assertIn('user: user"; touch /tmp/pwned; echo "', command)

            # Ensure it is enclosed in single quotes (start of the quoted string)
            self.assertIn('\'user: user"; touch /tmp/pwned; echo "', command)


if __name__ == "__main__":
    unittest.main()
