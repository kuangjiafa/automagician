# pylint: disable=cyclic-import
import logging
import os
import shutil
import subprocess
import time

import automagician.constants as constants


def wrap_up(job_directory: str) -> None:
    """Wraps up a job by using vfin.pl. Places the results in a "run" directory

        If the job already had a completed run and thus had run0, then run1
        would be created, and the job would be wrapped up there

        If the job did not have a completed run then the job would be wrapped
        up into run0

        If the job had a run10 directory, but no other directories the job
        would be wrapped up to run11

    Args:
        job_directory: The path of the job directory to wrap up.
    """
    logger = logging.getLogger()
    logger.info("wrapping up job")
    # first find name
    cwd = os.getcwd()
    os.chdir(job_directory)
    try:
        directories = [f.path for f in os.scandir(job_directory) if f.is_dir()]
        runs = [f for f in directories if "run" in f]
        # if no runs, wrap up into run0
        if len(runs) == 0:
            try:
                subprocess.run(
                    [constants.V_FIN_PL_PATH, "run0"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.STDOUT,
                )
            except FileNotFoundError:
                logger.warning("vfin.pl not found, skipping")
            try:
                shutil.move(
                    os.path.join(job_directory, "ll_out"),
                    os.path.join(job_directory, "run0"),
                )
            except FileNotFoundError:
                logger.warning("ll_out not found")
        else:
            # find the largest run
            largest_number = 0
            for run in runs:
                try:
                    number = int(run.partition("run")[2])
                    if number >= largest_number:
                        largest_number = number + 1
                except Exception:
                    continue

            largest_run = f"run{largest_number}"
            try:
                subprocess.run(
                    [constants.V_FIN_PL_PATH, largest_run],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.STDOUT,
                )
            except FileNotFoundError:
                logger.warning("vfin.pl not found, skipping")
            try:
                shutil.move("ll_out", largest_run)
            except FileNotFoundError:
                logger.warning("ll_out not found")
            logger.warning("combine_XDAT_FE disabled due to bugs")
        import automagician.update_job as update_job

        update_job.optimizer_review(job_directory)
    finally:
        os.chdir(cwd)


def give_certificate(job_directory: str) -> int:
    """Creates a convergence certificate in job_directory

    Args:
        job_directory: The directory to place the convergence certificate in

    Return:
        int: 1 if certificate already exists,
             0 if certificate was created
    """
    try:
        open(
            os.path.join(job_directory, constants.CONVERGENCE_CERTIFICATE_NAME), "x"
        )
        return 0
    except FileExistsError:
        return 1


def sc_is_complete(sc_dir: str) -> bool:
    """Returns True if the CHGCAR in sc_dir has not been writen to in 2 minutes or more.

    Args:
        sc_dir: the directory of the DosJob. Should contain a WAVECAR
    Returns:
        False if CHGCAR does not exist in sc_dir
        False if CHGCAR has been writen to in the previous 2 minutes
        True otherwise
    """
    if not os.path.exists(os.path.join(sc_dir, "CHGCAR")):
        return False
    last_modified = os.path.getmtime(os.path.join(sc_dir, "CHGCAR"))
    current_time = time.time()
    return current_time - last_modified > 120  # write stopped more than two minutes ago


def dos_is_complete(dos_dir: str) -> bool:
    """Returns True if the DOSCAR in dos_dir has not been writen to in 2 minutes or more.

    Args:
        dos_dir: the directory of the DosJob. Should contain a WAVECAR
    Returns:
        False if DOSCAR does not exist in dos_dir
        False if DOSCAR has been writen to in the previous 2 minutes
        True otherwise
    """
    if not os.path.exists(os.path.join(dos_dir, "DOSCAR")):
        return False
    last_modified = os.path.getmtime(os.path.join(dos_dir, "DOSCAR"))
    current_time = time.time()
    return current_time - last_modified > 120  # write stopped more than two minutes ago


def wav_is_complete(wav_dir: str) -> bool:
    """Returns True if the WAVECAR in wav_dir has not been writen to in 2 minutes or more.
    Args:
        wav_dir: the directory of the WavJob. Should contain a WAVECAR
    Returns:
        False if WAVECAR does not exist in wav_dir
        False if WAVECAR has been writen to in the previous 2 minutes
        True otherwise
    """
    if not os.path.exists(os.path.join(wav_dir, "WAVECAR")):
        return False
    last_modified = os.path.getmtime(os.path.join(wav_dir, "WAVECAR"))
    current_time = time.time()
    return current_time - last_modified > 120  # write stopped more than two minutes ago
