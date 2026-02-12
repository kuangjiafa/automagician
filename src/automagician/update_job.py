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
import automagician.process_job as process_job
from automagician.classes import DosJob, JobStatus, Machine, OptJob, WavJob

try:
    from automagician.classes import SshScp


    def scp_get_dir(remote: str, local: str, ssh_scp: SshScp) -> None:
        """Puts files inside the remote directory to the local directory

        Args:
        remote (str): the directory on the remote machine to transfer files from
        local (str): the directory on the local machine to transfer files to
        """
        for f in ssh_scp.ssh.run(
                "cd " + remote + "; find . -type f | cut -c 2-"
        ).stdout.split("\n"):
            if len(f) < 1:
                continue
            ssh_scp.scp.get(remote + f, local + f)

except ImportError:
    pass


def add_preliminary_results(
        job_directory: str,
        step: int,
        force: float,
        energy: float,
        preliminary_results: TextIO,
) -> None:
    """Adds the job directory, step number, force, and energy to the file in preliminary_results"""
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
    # error_log = open(os.path.join(home, "error_log.dat"), "a+")
    # potentially create an error buffer and write the errors all at once in the end? potentially a bad idea in case of a crash though/not sure if the speedup would be non-negligible
    # for error_message in get_error_message(job_directory):
    #     error_log.write(
    #         f"{str(datetime.datetime.now())} {job_directory} {error_message} \n"
    #     )
    # error_log.close()

    # TODO: verify that this change doesn't
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
      list(str): A list of error messages found. If none were found, contains a single str
      saying "message not found"
    Changes:
      Changes current working direcctory to job_directory"""
    ll_out = open(os.path.join(job_directory, "ll_out"), "r")
    messages = []
    for line in ll_out:
        if ("ERROR" in line) or ("error" in line):
            messages.append(line.strip("| \n"))
    # if len(messages) == 0:
    #     messages.append("error message not found!")
    return messages


def fix_error(
        job_directory: str,
) -> bool:
    """Attempts to fix the error in job_direcory. Fixes ZBRINT, and number of potentials incompatable.
    Args:
      job_directory (str): A path to the directory that contains a job which has an error
      home_dir (str): A path that contains the files "/kingRaychardsArsenal/sogetsoftpbe.py" and "/kingRaychardsArsenal/sortpos.py"
    Returns:
      True if a fix was attempted,
    Changes:
      Resubmits the job iff a fix was attempted"""
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
            os.chdir(cwd)
            return True
    logger.info(f"a fix was not attempted for the job at {job_directory}")
    return False


# if CG isn't working, use Damped molecular dynamics
def optimizer_review(job_directory: str) -> None:
    """Returns None
     --- What I think it wants to do below ---
    Goal seems to be to combine XDATCAR and FE

    Changes INCAR by adjusting INBARIAR to the most correct option
    """
    logger = logging.getLogger()
    logger.warning("because bugs related to cmbFE, optimizer review is disabled.")
    return None


def update_job_name(subfile: str) -> None:
    """Replaces lines that contain -N and -J with new lines

    -J is repalced with "#SBATCH -J AM_"<current_working_directory with / replaced by _
    """
    script = open(subfile, "r")
    script_lines = script.readlines()
    script.close()
    with open(subfile, "w") as script:
        for line in script_lines:
            if "-J" in line or "--job-name=" in line:
                script.write(
                    "#SBATCH -J " + "AM_" + os.getcwd().replace("/", "_") + "\n"
                )
            else:
                script.write(line)


def set_incar_tags(path: str, tags_dict: Dict[str, Optional[str]]) -> None:
    """Edits the INCAR and writes the dictionary to the INCAR

    If a tag in tags_dict is not in the INCAR, writes the tag to the INCAR alongside the value
    if a tag is in INCAR, but not in tags_dict, leavs the tag unchanged
    If a tag in tags_dict is present in the INCAR, updates the tag to the value present in tags_dict

    Args:
      path (str): the path to the INCAR
      tags_dict (dict(keys: str vals: str)): A dictionary connecting the diffrent tags of an INCAR to the values,
        keys = left hand side of the = ex
          x = y
          the key is x, while the value is y"""
    logger = logging.getLogger()
    read_incar = open(path, "r")
    lines = read_incar.readlines()
    for i in range(0, len(lines)):
        tag = lines[i].strip().split("=")[0]
        try:
            new_val = tags_dict[tag]
            tags_dict[tag] = None
            lines[i] = tag + "=" + new_val + "\n"  # type: ignore
        except KeyError:
            logger.error("KeyError")
            traceback.print_exc()
            continue

    read_incar.close()

    write_incar = open(path, "w")
    write_incar.writelines(lines)

    more_lines = []
    for tag in tags_dict:
        val = tags_dict[tag]
        if val is not None:
            more_lines.append(tag + "=" + val + "\n")

    write_incar.writelines(more_lines)
    write_incar.close()


def get_opt_dir(job_dir: str) -> str:
    """Replaces the dos sc and wav's that could be in a directory with nothing  to turn them into opt jobs"""
    return re.compile(r"\/(dos|sc|wav)$").sub("", str(job_dir))


def switch_subfile(
        job_dir: str,
        new_sub: str,
        subfile: str,
        machine: Machine,
) -> None:
    """Copies the subfile into new_sub and updates the job_name of new_sub

    if there is not a subfile does nothing
    Args:
        job_dir: the job directory
        new_sub:
        subfile: The name of the subfile
        machine:"""
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


def set_status_for_newly_submitted_job(
        job_dir: str,
        job_machine: Machine,
        dos_jobs: Dict[str, DosJob],
        wav_jobs: Dict[str, WavJob],
        opt_jobs: Dict[str, OptJob],
        error: bool,
) -> None:
    """Sets the job status to that of special jobs that no longer need to be optoomised


    job_dir - the directory that the job is found in

    job_machine - the machine the job is running on

    """
    job_type = process_job.classify_job_dir(job_dir)
    opt_dir = get_opt_dir(job_dir)

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
