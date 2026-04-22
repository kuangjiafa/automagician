# pylint: disable=duplicate-code,cyclic-import
# pylint: disable=duplicate-code,cyclic-import
from __future__ import annotations

import datetime
import logging
import os
import re
import subprocess
import traceback
from os.path import exists
from typing import Dict, Optional, TextIO

import automagician.constants as constants
import automagician.finish_job as finish_job
import automagician.small_functions as small_functions
from automagician.classes import DosJob, JobStatus, Machine, OptJob, WavJob


def add_preliminary_results(
    job_directory: str,
    step: int,
    force: float,
    energy: float,
    preliminary_results: TextIO,
) -> None:
    """Append a job's current step, force, and energy to the preliminary results file.

    Args:
        job_directory: Absolute path to the job directory.
        step: Current ionic step number.
        force: Residual force magnitude at the current step.
        energy: Total energy at the current step.
        preliminary_results: Open writable file handle for recording results.
    """
    preliminary_results.write(str(job_directory) + "\n")
    preliminary_results.write(f"     {step}     {force}     {energy}\n")


# generate a permanent error log
def log_error(job_directory: str, home: str) -> None:
    """Writes error messages in the job directory to error_log.dat. Appends

    Args:
      job_directory (str): A path to the directory that contains a job which has an error
      home (str): The home of the user
    Returns:
      None
    Changes:
      Updates error_log.dat, creating it if it dosent exist, and writes the error message, and current time
    Tests
      TODO: Medium priority
        Simple, something not critical"""
    with open(os.path.join(home, "error_log.dat"), "a+") as error_log:
        for error_message in get_error_message(job_directory):
            error_log.write(
                f"{str(datetime.datetime.now())} {job_directory} {error_message} \n"
            )


def get_error_message(job_directory: str) -> list[str]:
    """Gets the error message from ll_out and returns all found
    Args:
      job_directory (str): A path to the directory that contains a job which has an error
    Returns:
      list(str): A list of error messages found. If none were found, returns an empty list.
    Changes:
      Changes current working direcctory to job_directory"""
    messages = []
    try:
        with open(os.path.join(job_directory, "ll_out"), "r") as ll_out:
            for line in ll_out:
                if "ERROR" in line or "error" in line:
                    messages.append(line.strip("| \n"))
    except FileNotFoundError:
        pass
    return messages


def fix_error(
    job_directory: str,
) -> bool:
    """Attempt to automatically fix a known VASP error in ``job_directory``.

    Handles two error types found in ``ll_out``:

    - **ZBRENT**: calls :func:`~automagician.finish_job.wrap_up` to archive the
      current run into ``run<N>/`` and copy CONTCAR over POSCAR, then returns
      ``True`` so the caller can resubmit.  Returns ``False`` if CONTCAR is
      missing or empty.
    - **POTCAR count mismatch**: runs ``sortpos.py`` then ``sogetsoftpbe.py``
      (paths from :mod:`automagician.constants`) to regenerate a compatible
      POTCAR.  Returns ``False`` if the scripts are not found.

    If no recognised error is found, logs a message and returns ``False``.

    Args:
        job_directory: Absolute path to the directory containing the failed job.

    Returns:
        ``True`` if a fix was attempted (resubmission is expected by the caller);
        ``False`` if the error was not recognised or the fix prerequisites were
        missing.
    """
    logger = logging.getLogger()
    error_messages = get_error_message(job_directory)

    for error_message in error_messages:
        if "ZBRENT" in error_message:
            contcar_path = os.path.join(job_directory, "CONTCAR")
            if not os.path.exists(contcar_path) or os.path.getsize(contcar_path) == 0:
                return False

            finish_job.wrap_up(job_directory)
            return True
        elif (
            "number of potentials on File POTCAR incompatible with number"
            in error_message
        ):
            cwd = os.getcwd()
            os.chdir(job_directory)
            try:
                subprocess.call(
                    [constants.SORT_POS_PATH],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.STDOUT,
                )
                subprocess.call(
                    [constants.SO_GET_SOFT_PBE_PATH],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.STDOUT,
                )
            except FileNotFoundError as e:
                logger.error(
                    f"fix_error script not found for job at {job_directory}: {e}"
                )
                return False
            finally:
                os.chdir(cwd)
            return True
    logger.info(f"a fix was not attempted for the job at {job_directory}")
    return False


def update_job_name(subfile: str) -> None:
    """Replaces the ``-J`` / ``--job-name=`` line in the submission script.

    The job name is set to ``AM_<cwd>`` where ``<cwd>`` is the current working
    directory with ``/`` replaced by ``_``. Only the ``-J`` and ``--job-name=``
    (equals-separated) forms are matched; space-separated ``--job-name <name>``
    is not handled.
    """
    script = open(subfile, "r")
    script_lines = script.readlines()
    script.close()
    with open(subfile, "w") as script:
        cwd_str = os.getcwd().replace("/", "_")
        for line in script_lines:
            if "-J" in line or "--job-name=" in line:
                script.write("#SBATCH -J " + "AM_" + cwd_str + "\n")
            else:
                script.write(line)


def set_incar_tags(path: str, tags_dict: Dict[str, Optional[str]]) -> None:
    """Edits the INCAR and writes the dictionary to the INCAR

    If a tag in tags_dict is not in the INCAR, writes the tag to the INCAR alongside the value
    if a tag is in INCAR, but not in tags_dict, leavs the tag unchanged
    If a tag in tags_dict is present in the INCAR, updates the tag to the value present in tags_dict

    Args:
        path: Path to the INCAR file to edit.
        tags_dict: Maps INCAR tag names (left-hand side of ``=``) to their
            desired values. A ``None`` value means the tag has already been
            written and should be skipped.
    """
    with open(path, "r") as read_incar:
        lines = read_incar.readlines()

    for i, line in enumerate(lines):
        if "=" not in line:
            continue
        tag = line.split("=")[0].strip()
        if tag in tags_dict:
            new_val = tags_dict[tag]
            if new_val is not None:
                lines[i] = f"{tag}={new_val}\n"
                tags_dict[tag] = None

    with open(path, "w") as write_incar:
        write_incar.writelines(lines)
        more_lines = []
        for tag, val in tags_dict.items():
            if val is not None:
                more_lines.append(f"{tag}={val}\n")
        write_incar.writelines(more_lines)


def switch_subfile(
    job_dir: str,
    new_sub: str,
    subfile: str,
    machine: Machine,
) -> None:
    """Copy the machine's default submission script into ``job_dir`` as ``new_sub``.

    If the current subfile (``subfile``) is not present in ``job_dir`` the
    function is a no-op.  Otherwise it copies ``new_sub`` from the
    machine-appropriate template archive (FRI/Halifax: ``DEFAULT_SUBFILE_PATH_FRI_HALIFAX``;
    TACC: ``DEFAULT_SUBFILE_PATH_TACC``) into ``job_dir`` and rewrites the
    ``#SBATCH -J`` job-name line via :func:`update_job_name`.

    Args:
        job_dir: Absolute path to the job directory.
        new_sub: Filename of the target submission script to copy in (e.g.
            ``"halifax.sub"``).
        subfile: Filename of the current machine's submission script used as a
            sentinel to confirm an opt job exists here.
        machine: The machine the user is currently logged into; controls which
            template archive is used.
    """
    cwd = os.getcwd()
    try:
        os.chdir(job_dir)

        if not exists(subfile):
            return

        default_subfile_path = (
            constants.DEFAULT_SUBFILE_PATH_FRI_HALIFAX
            if machine < 2
            else constants.DEFAULT_SUBFILE_PATH_TACC
        )

        subprocess.call(["cp", default_subfile_path + "/" + new_sub, new_sub])
        # os.remove(old_sub)
        update_job_name(new_sub)
    finally:
        os.chdir(cwd)


def set_status_for_newly_submitted_job(
    job_dir: str,
    job_machine: Machine,
    dos_jobs: Dict[str, DosJob],
    wav_jobs: Dict[str, WavJob],
    opt_jobs: Dict[str, OptJob],
    error: bool,
) -> None:
    """Update the in-memory status for a job that was just submitted via ``sbatch``.

    Classifies ``job_dir`` as ``"opt"``, ``"sc"``, ``"dos"``, or ``"wav"`` and
    sets the corresponding status field in ``opt_jobs``, ``dos_jobs``, or
    ``wav_jobs`` to ``JobStatus.RUNNING`` (or ``JobStatus.ERROR`` if ``sbatch``
    returned a non-zero exit code).  Also records ``job_machine`` as the machine
    the job was sent to.

    Args:
        job_dir: Absolute path to the submitted job directory.
        job_machine: The machine the job was submitted to.
        dos_jobs: In-memory map of all known DOS jobs.
        wav_jobs: In-memory map of all known WAV jobs.
        opt_jobs: In-memory map of all known optimisation jobs.
        error: When ``True`` the status is set to ``JobStatus.ERROR`` instead of
            ``JobStatus.RUNNING`` (indicates ``sbatch`` failed).
    """
    job_type = small_functions.classify_job_dir(job_dir)
    opt_dir = small_functions.get_opt_dir(job_dir)

    # for now, status -1 is for special jobs that no longer need optimization
    if job_type == "sc":
        if error:
            dos_jobs[opt_dir].sc_status = JobStatus.ERROR
        else:
            dos_jobs[opt_dir].sc_status = JobStatus.RUNNING
        dos_jobs[opt_dir].sc_last_on = job_machine
    elif job_type == "dos":
        if error:
            dos_jobs[opt_dir].dos_status = JobStatus.ERROR
        else:
            dos_jobs[opt_dir].dos_status = JobStatus.RUNNING
        dos_jobs[opt_dir].dos_last_on = job_machine
    elif job_type == "wav":
        if error:
            wav_jobs[opt_dir].wav_status = JobStatus.ERROR
        else:
            wav_jobs[opt_dir].wav_status = JobStatus.RUNNING
        wav_jobs[opt_dir].wav_last_on = job_machine
    else:
        if error:
            opt_jobs[opt_dir].status = JobStatus.ERROR
        else:
            opt_jobs[opt_dir].status = JobStatus.RUNNING
        opt_jobs[opt_dir].last_on = job_machine
