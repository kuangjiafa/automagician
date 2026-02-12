import os

import pytest

import automagician.constants as constants
import automagician.main


def test_automagician_lockfile_present():
    lockfile_path = constants.LOCK_FILE
    if not os.path.exists(constants.LOCK_DIR):
        os.makedirs(constants.LOCK_DIR)
    with open(lockfile_path, "w") as lockfile:
        lockfile.write("MockLockfile")

    with pytest.raises(SystemExit):
        automagician.main.main()
    os.remove(lockfile_path)
    os.rmdir(constants.LOCK_DIR)
    assert True
