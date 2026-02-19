import logging
import os
import shutil
from typing import List

import automagician.machine as machine_file
import automagician.update_job as update_job
from automagician.classes import JobLimitError, Machine


def add_to_sub_queue(
        job_directory: str,
        continue_past_limit: bool,
        limit: int,
        sub_queue: List[str],
        machine: Machine,
        hit_limit: bool,
) -> bool:
    """Adds job_directoy to sub_queue. and updates the job name

    job_directory MUST have a trailing '/'

    Throws JobLimitError if the limit would be hit

    If hit_limit is true, is a no-op

    If submitting would go past the limit, raise a JobLimitError

    Args:
        job_directory: The directory that should be submitted
        continue_past_limit: Says to raise a JobLimitError when submitting past
            the limit
        limit: The amount of jobs that automagician is allowed to submit
        sub_queue: A list showing the jobs that will be submitted
        machine: The machine to submit the jobs on
        hit_limit: If the limit has already been hit
    Returns:
        True if the job being submitted would hit the limit, False otherwise
    Raises:
        JobLimitError: If submitting this job would cause the limit to be
            breached, and continue_past_limit is false
    """
    if hit_limit:
        return True
    subfile = machine_file.get_subfile(machine)
    update_job.update_job_name(os.path.join(job_directory, subfile))
    sub_queue.append(job_directory)

    if len(sub_queue) >= limit:
        if continue_past_limit:
            return True
        else:
            raise JobLimitError()
    return False


def create_dos_from_sc(
        job_directory: str,
        continue_past_limit: bool,
        limit: int,
        sub_queue: List[str],
        machine: Machine,
        hit_limit: bool,
) -> None:
    """Creates a properly formed dos directory from sc, setting up INCAR to be
    correct, and then submits the job

    The INCAR has the fields ICHARGE and LORBIT both set to 11
    Args:
        job_directory: The directory that should be submitted
        continue_past_limit: Says to raise a JobLimitError when submitting past
            the limit
        limit: The ammount of jobs that automagician is allowed to submit
        sub_queue: A list showing the jobs that will be submitted
        machine: The machine to submit the jobs on
        hit_limit: If the limit has already been hit
    Raises:
        JobLimitError: If submitting this job would cause the limit to be
            breached, and continue_past_limit is false
    """
    subfile = machine_file.get_subfile(machine)
    dos_dir = os.path.normpath(os.path.join(job_directory, "../dos"))
    # os.mkdir(dos_dir)
    # shutil.copy(os.path.join(job_directory, subfile), dos_dir)
    # shutil.copy(os.path.join(job_directory, "KPOINTS"), dos_dir)
    # shutil.copy(os.path.join(job_directory, "POTCAR"), dos_dir)
    # shutil.copy(os.path.join(job_directory, "INCAR"), dos_dir)
    # shutil.copy(os.path.join(job_directory, "CHGCAR"), dos_dir)

    # copy over the inputs
    copy_inputs(subfile, job_directory, dos_dir)

    if os.path.exists(os.path.join(job_directory, "CONTCAR")):
        shutil.copy(os.path.join(job_directory, "CONTCAR"), dos_dir)
    else:
        shutil.copy(os.path.join(job_directory, "POSCAR"), dos_dir)

    update_job.set_incar_tags(
        os.path.join(dos_dir, "INCAR"), {"ICHARGE": "11", "LORBIT": "11"}
    )

    add_to_sub_queue(
        job_directory=dos_dir,
        continue_past_limit=continue_past_limit,
        limit=limit,
        sub_queue=sub_queue,
        machine=machine,
        hit_limit=hit_limit,
    )


def copy_inputs(subfile,
                job_directory: str,
                directory: str) -> None:
    os.mkdir(directory)
    shutil.copy(os.path.join(job_directory, subfile), directory)
    shutil.copy(os.path.join(job_directory, "KPOINTS"), directory)
    shutil.copy(os.path.join(job_directory, "POTCAR"), directory)
    shutil.copy(os.path.join(job_directory, "INCAR"), directory)
    if os.path.exists(os.path.join(job_directory, "CHGCAR")):
        shutil.copy(os.path.join(job_directory, "CHGCAR"), directory)
    if os.path.exists(os.path.join(job_directory, "CONTCAR")):
        shutil.copy(os.path.join(job_directory, "CONTCAR"), directory)
    else:
        shutil.copy(os.path.join(job_directory, "POSCAR"), directory)

# Create a self-consistent calculation to get WAVECAR for later use
def create_wav(
        job_directory: str,
        continue_past_limit: bool,
        limit: int,
        sub_queue: List[str],
        machine: Machine,
        hit_limit: bool,
) -> None:
    """Wakes a WAV directory, and copies INCAR, KPOINTS, POTCAR, and
    CONTCAR, or POSCAR if CONTCAR does not exist to this new directory

    Sets up the INCAR to be that of a WAV calculation. Submits the job"""
    subfile = machine_file.get_subfile(machine)
    wav_dir = os.path.normpath(os.path.join(job_directory, "../wav"))
    # copy over the inputs
    copy_inputs(subfile, job_directory, wav_dir)
    update_job.set_incar_tags(
        os.path.join(wav_dir, "INCAR"), {"IBRION": "-1", "LWAVE": ".TRUE.", "NSW": "0"}
    )

    add_to_sub_queue(
        job_directory=wav_dir,
        continue_past_limit=continue_past_limit,
        limit=limit,
        sub_queue=sub_queue,
        machine=machine,
        hit_limit=hit_limit,
    )


def create_sc(
        job_directory: str,
        continue_past_limit: bool,
        limit: int,
        sub_queue: List[str],
        machine: Machine,
        hit_limit: bool,
) -> None:
    """Creates an SC directory and sets INCAR. Submits the job

    IBRION = -1
    LCHARGE  = .TRUE.
    NSW = 0
    Args:
      job_directory (str): the path to the directory the job is located in
    Changes:
      Submits the job in the job_directory
      Creates a sc subdirectory inside job directory that is set up for a job"""
    logger = logging.getLogger()
    subfile = machine_file.get_subfile(machine)
    logger.debug("creating sc directory")
    sc_dir = os.path.normpath(os.path.join(job_directory, "../sc"))

    # copy kpoints, incar, potcar, subfile over
    copy_inputs(subfile, job_directory, sc_dir)

    update_job.set_incar_tags(
        os.path.join(sc_dir, "INCAR"), {"IBRION": "-1", "LCHARGE": ".TRUE.", "NSW": "0"}
    )

    add_to_sub_queue(
        job_directory=sc_dir,
        continue_past_limit=continue_past_limit,
        limit=limit,
        sub_queue=sub_queue,
        machine=machine,
        hit_limit=hit_limit,
    )
