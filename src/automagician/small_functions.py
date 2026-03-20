from __future__ import annotations

import logging
import os
import re
import shlex
import shutil
import subprocess
from typing import Literal

try:
    from automagician.classes import SshScp

    def scp_get_dir(remote: str, local: str, ssh_scp: SshScp) -> None:
        """Puts files inside the remote directory to the local directory

        Args:
            remote: the directory on the remote machine to transfer files from
            local: the directory on the local machine to transfer files to
        """
        for f in ssh_scp.ssh.run(
            "cd " + shlex.quote(remote) + " && find . -type f | cut -c 2-"
        ).stdout.split("\n"):
            if len(f) < 1:
                continue
            ssh_scp.scp.get(remote + f, local + f)

except ImportError:
    pass


IS_DOS_REGEX = re.compile(r".*?(?<!^/home)\/dos$")
IS_SC_REGEX = re.compile(r".*?(?<!^/home)\/sc$")
IS_WAV_REGEX = re.compile(r".*?(?<!^/home)\/wav$")
GET_OPT_DIR_REGEX = re.compile(r"\/(dos|sc|wav)$")


def classify_job_dir(job_dir: str) -> Literal["dos", "sc", "wav", "opt"]:
    """Returns the type of job this is based on the ending directory name.

    Aka if job_dir ends in /dos then this would return "dos" while if it ended in /sc
    this would return "sc", and if it ended in /wav returns "wav".
    Finally if it does not match any of the following returns "opt"
    """
    if IS_DOS_REGEX.match(str(os.path.normpath(job_dir))):
        return "dos"
    elif IS_SC_REGEX.match(str(os.path.normpath(job_dir))):
        return "sc"
    elif IS_WAV_REGEX.match(str(os.path.normpath(job_dir))):
        return "wav"
    else:
        return "opt"


def get_opt_dir(job_dir: str) -> str:
    """Replaces the dos sc and wav's that could be in a directory with nothing  to turn them into opt jobs"""
    return GET_OPT_DIR_REGEX.sub("", str(job_dir))


def archive_converged(home: str) -> None:
    """renames converged_jobs.dat to archive_converged.dat
    Args:
      home (str): the path to the users home directory
    """
    logger = logging.getLogger()
    logger.warn("archive converged is deperciated, and will be reimplemented shortly")
    shutil.move(
        os.path.join(home, "converged_jobs.dat"),
        os.path.join(home, "archive_converged.dat"),
    )


def reset_converged(home: str) -> None:
    """Moves all converged jobs from converged_jobs.dat to unconverged_jobs.dat

    Depriacted
    Args:
      home (str): the path to the users home directory"""
    logger = logging.getLogger()
    logger.warn("reset converged is deperciated, and will be reimplemented shortly")
    with open(os.path.join(home, "unconverged_jobs.dat"), "a") as f:
        subprocess.call(
            ["grep", "-e", r"^\/", os.path.join(home, "converged_jobs.dat")],
            stdout=f,
            stderr=subprocess.STDOUT,
        )
    os.remove(os.path.join(home, "converged_jobs.dat"))
