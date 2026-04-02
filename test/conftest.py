import os

import pytest


@pytest.fixture(autouse=True)
def restore_cwd():
    """Restore the working directory after every test.

    Tests that call os.chdir() without a try/finally leave the process CWD
    changed when they fail mid-way.  This autouse fixture ensures the CWD is
    always reset to whatever it was before the test started, preventing
    cascading failures in tests that use relative paths.
    """
    original = os.getcwd()
    yield
    os.chdir(original)
