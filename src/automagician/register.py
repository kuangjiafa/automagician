import logging
import os
import re
from typing import Dict, List, TextIO

import automagician.machine as machine_file
import automagician.process_job as process_job
from automagician.classes import DosJob, JobStatus, Machine, OptJob, SSHConfig, WavJob


def register(
        opt_jobs: Dict[str, OptJob],
        dos_jobs: Dict[str, DosJob],
        wav_jobs: Dict[str, WavJob],
        machine: Machine,
        clear_certificate: bool,
        home_dir: str,
        ssh_config: SSHConfig,
        preliminary_results: TextIO,
        continue_past_limit: bool,
        limit: int,
        sub_queue: List[str],
        hit_limit: bool,
) -> None:
    """Adds jobs to opt_jobs, dos_jobs, and wav_jobs, and their associated queues.

    This is based on the current working directory

    ini, fin, dos, wav, and sc are reserved

    Processes the queues

    Args:
      None
    Returns:
      None
    Changes:
      Submits the jobs if a run finished, but they were not optomized
      Updates prelimanary results
      Finds NEB, and later calls turtleMagician on them
    """
    logger = logging.getLogger()
    # calc_files = ["POSCAR","POTCAR","INCAR","KPOINTS",subfile]
    # neb_dirs = [re.compile(".*?[Iini]", ".*?[Fin]", ".*?[Band]")]
    # ini, fin, dos, wav, sc, are now reserved directory names. these cannot be a substring of a directory
    subfile = machine_file.get_subfile(machine)
    NEB_paths_arr = []
    opt_queue = []
    dos_queue = []
    wav_queue = []
    for job_dir, subdirs, files in os.walk(os.getcwd(), followlinks=True):
        if exclude_regex(job_dir):
            continue

        job_dir = job_dir.strip("\n")
        logger.info(
            "Registrator looking at " + "\x1b[0;49;34m" + job_dir + "\x1b[0m"
        )  # Should show directory in blue text

        has_dos = False
        has_wav = False
        exclude = False
        if os.path.exists(os.path.join(job_dir, "automagic_note")):
            with open(os.path.join(job_dir, "automagic_note"), "r") as f:
                for line in f:
                    if line == "dos\n":
                        has_dos = True
                        break
                    elif line == "wav\n":
                        has_wav = True
                        break
                    elif line == "exclude\n":
                        exclude = True
                        break

        has_opt = process_job.check_has_opt(job_dir, subfile)
        if not has_opt:
            continue

        dirs_lowercase = {item.lower() for item in subdirs}
        if (
                ("band" in dirs_lowercase)
                and ("ini" in dirs_lowercase)
                and ("fin" in dirs_lowercase)
        ):
            logger.debug("Found a NEB job bundle")
            NEB_paths_arr.append(job_dir)
            logger.info(f"NEB located at {job_dir}")
            continue  # skip any further action for this root directory

        if not exclude:
            opt_queue.append(job_dir)
            if job_dir not in opt_jobs:
                opt_jobs[job_dir] = OptJob(JobStatus.INCOMPLETE, machine, machine)
        if has_dos and not exclude:
            dos_queue.append(job_dir)
            if job_dir not in dos_jobs:
                dos_jobs[job_dir] = DosJob(
                    -1, JobStatus.INCOMPLETE, JobStatus.INCOMPLETE, machine, machine
                )
        if has_wav and not exclude:
            wav_queue.append(job_dir)
            if job_dir not in wav_jobs:
                wav_jobs[job_dir] = WavJob(-1, JobStatus.INCOMPLETE, machine)
    process_queue(
        opt_queue=opt_queue,
        dos_queue=dos_queue,
        wav_queue=wav_queue,
        machine=machine,
        opt_jobs=opt_jobs,
        dos_jobs=dos_jobs,
        wav_jobs=wav_jobs,
        clear_certificate=clear_certificate,
        home_dir=home_dir,
        ssh_config=ssh_config,
        preliminary_results=preliminary_results,
        continue_past_limit=continue_past_limit,
        limit=limit,
        sub_queue=sub_queue,
        hit_limit=hit_limit,
    )


def exclude_regex(job_dir: str) -> bool:
    regex = r".*?(?<!^/home)((/run\d*)|(/dos)|(/sc)|(/[Ii]ni)|(/[Ff]in)|(/wav))"
    return bool(re.match(regex, job_dir))


def process_queue(
        opt_queue: List[str],
        dos_queue: List[str],
        wav_queue: List[str],
        machine: Machine,
        opt_jobs: Dict[str, OptJob],
        dos_jobs: Dict[str, DosJob],
        wav_jobs: Dict[str, WavJob],
        clear_certificate: bool,
        home_dir: str,
        ssh_config: SSHConfig,
        preliminary_results: TextIO,
        continue_past_limit: bool,
        limit: int,
        sub_queue: List[str],
        hit_limit: bool,
) -> None:
    """Processes the jobs in each of the quenes, updates opt jobs if the job was no longer found in the correct directory

    Args:
    Returns:
    Changes:"""
    logger = logging.getLogger()
    logger.debug(f"opt_queue is {opt_queue}")
    for job_dir in opt_queue:
        if os.path.exists(job_dir):
            process_job.process_opt(
                job_directory=job_dir,
                machine=machine,
                opt_jobs=opt_jobs,
                clear_certificate=clear_certificate,
                home_dir=home_dir,
                ssh_config=ssh_config,
                preliminary_results=preliminary_results,
                continue_past_limit=continue_past_limit,
                limit=limit,
                sub_queue=sub_queue,
                hit_limit=hit_limit,
            )
        else:
            logger.warning(f"job is no longer found at {job_dir}")
            old_opt = opt_jobs[job_dir]
            old_opt.status = JobStatus.NOT_FOUND

    for job_dir in dos_queue:
        process_job.process_dos(
            job_directory=job_dir,
            opt_jobs=opt_jobs,
            dos_jobs=dos_jobs,
            continue_past_limit=continue_past_limit,
            limit=limit,
            sub_queue=sub_queue,
            machine=machine,
            hit_limit=hit_limit,
        )

    for job_dir in wav_queue:
        process_job.process_wav(
            job_directory=job_dir,
            opt_jobs=opt_jobs,
            wav_jobs=wav_jobs,
            continue_past_limit=continue_past_limit,
            limit=limit,
            sub_queue=sub_queue,
            machine=machine,
            hit_limit=hit_limit,
        )
