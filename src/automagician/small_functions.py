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

    def scp_get_dir(remote: str, local: str, ssh_scp: object) -> None:
        pass


IS_DOS_REGEX = re.compile(r".*?(?<!^/home)\/dos$")
IS_SC_REGEX = re.compile(r".*?(?<!^/home)\/sc$")
IS_WAV_REGEX = re.compile(r".*?(?<!^/home)\/wav$")
GET_OPT_DIR_REGEX = re.compile(r"\/(dos|sc|wav)$")


def classify_job_dir(job_dir: str) -> Literal["dos", "sc", "wav", "opt"]:
    """Classify a job directory as ``"dos"``, ``"sc"``, ``"wav"``, or ``"opt"``.

    The path is normalised with ``os.path.normpath`` before matching, so
    trailing slashes do not affect the result.  Classification is based solely
    on the final path component: ``/dos`` → ``"dos"``, ``/sc`` → ``"sc"``,
    ``/wav`` → ``"wav"``, anything else → ``"opt"``.

    The leading ``/home`` segment is exempted so that directories whose root
    happens to share a name with a reserved component are not misclassified.

    Args:
        job_dir: Path to the job directory (absolute or relative).

    Returns:
        One of ``"dos"``, ``"sc"``, ``"wav"``, or ``"opt"``.
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
    """Strip a trailing ``/dos``, ``/sc``, or ``/wav`` segment to recover the opt directory.

    Uses a simple regex substitution and does **not** call ``os.path.normpath``;
    callers must pass already-normalised paths (no trailing slashes).

    Args:
        job_dir: Path to a dos, sc, wav, or opt job directory.

    Returns:
        The path with any trailing ``/dos``, ``/sc``, or ``/wav`` removed.
        Returns ``job_dir`` unchanged if none of those suffixes are present.
    """
    return GET_OPT_DIR_REGEX.sub("", str(job_dir))


def archive_converged(home: str) -> None:
    """renames converged_jobs.dat to archive_converged.dat
    Args:
      home (str): the path to the users home directory
    """
    logger = logging.getLogger()
    logger.warning("archive converged is deprecated, and will be reimplemented shortly")
    shutil.move(
        os.path.join(home, "converged_jobs.dat"),
        os.path.join(home, "archive_converged.dat"),
    )


def reset_converged(home: str) -> None:
    """Moves all converged jobs from converged_jobs.dat to unconverged_jobs.dat

    Deprecated
    Args:
      home (str): the path to the users home directory"""
    logger = logging.getLogger()
    logger.warning("reset converged is deprecated, and will be reimplemented shortly")
    with open(os.path.join(home, "unconverged_jobs.dat"), "a") as f:
        subprocess.call(
            ["grep", "-e", r"^\/", os.path.join(home, "converged_jobs.dat")],
            stdout=f,
            stderr=subprocess.STDOUT,
        )
    os.remove(os.path.join(home, "converged_jobs.dat"))
