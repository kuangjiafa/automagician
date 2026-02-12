import logging
import os
import shutil
import subprocess


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
