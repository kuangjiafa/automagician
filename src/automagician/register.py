# pylint: disable=duplicate-code,cyclic-import
# pylint: disable=duplicate-code
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
    """Walk the current directory tree, register discovered jobs, then process them.

    Scans ``os.getcwd()`` recursively (following symlinks). For each directory
    that contains the five required VASP input files (POSCAR, POTCAR, INCAR,
    KPOINTS, and the machine-specific subfile), the directory is classified:

    - If it contains ``band/``, ``ini/``, and ``fin/`` subdirectories it is
      treated as a NEB bundle and logged but otherwise skipped.
    - If an ``automagic_note`` file is present with ``dos``, ``wav``, or
      ``exclude`` on the first line, the corresponding flag is set.
    - Otherwise the directory is added to ``opt_jobs`` (if not already known)
      and enqueued for processing.

    After walking, :func:`process_queue` is called to run
    :func:`~automagician.process_job.process_opt` / ``process_dos`` /
    ``process_wav`` on each queued directory.

    Reserved directory name fragments (case-insensitive where applicable):
    ``run*``, ``dos``, ``sc``, ``ini``/``Ini``, ``fin``/``Fin``, ``wav``.

    Args:
        opt_jobs: In-memory map of known optimisation jobs, keyed by absolute
            directory path.  New jobs are inserted with ``JobStatus.INCOMPLETE``.
        dos_jobs: In-memory map of known DOS jobs.  New entries are created when
            an ``automagic_note`` file marks a directory with ``dos``.
        wav_jobs: In-memory map of known WAV jobs.  New entries are created when
            an ``automagic_note`` file marks a directory with ``wav``.
        machine: The machine the process is currently running on.
        clear_certificate: When ``True``, an existing ``convergence_certificate``
            file is removed before convergence is re-evaluated.
        home_dir: Absolute path to the user's home (or ``$WORK/..`` on TACC);
            used when forwarding to :func:`process_queue`.
        ssh_config: SSH/SCP connection config; ``config == "NoSSH"`` when fabric
            is unavailable or ``--balance`` was not set.
        preliminary_results: Open writable file handle for recording intermediate
            energy/force results.
        continue_past_limit: When ``True``, processing continues after the
            submission limit is reached instead of raising
            :class:`~automagician.classes.JobLimitError`.
        limit: Maximum number of jobs that may be in the submission queue at once.
        sub_queue: Accumulator list of job directories to be submitted via
            ``sbatch`` at the end of the run.
        hit_limit: Whether the submission limit has already been reached before
            this call.
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


_EXCLUDE_REGEX = re.compile(
    r".*?(?<!^/home)((/run\d*)|(/dos)|(/sc)|(/[Ii]ni)|(/[Ff]in)|(/wav))"
)


def exclude_regex(job_dir: str) -> bool:
    """Return ``True`` if ``job_dir`` matches a reserved subdirectory pattern.

    Directories whose path ends with ``/run<N>``, ``/dos``, ``/sc``, ``/ini``,
    ``/Ini``, ``/fin``, ``/Fin``, or ``/wav`` are excluded from registration.
    The leading ``/home`` segment is exempted so that paths whose first component
    happens to be one of the reserved names are not incorrectly filtered.

    Args:
        job_dir: Absolute path of the candidate directory.

    Returns:
        ``True`` if the directory should be skipped; ``False`` otherwise.
    """
    return bool(_EXCLUDE_REGEX.match(job_dir))


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
    """Process registered job queues by calling the appropriate ``process_*`` function.

    Iterates over ``opt_queue``, ``dos_queue``, and ``wav_queue`` in order.
    For each opt directory, calls :func:`~automagician.process_job.process_opt`;
    if the directory no longer exists on disk its status is set to
    ``JobStatus.NOT_FOUND``.  DOS and WAV directories are forwarded to
    :func:`~automagician.process_job.process_dos` and
    :func:`~automagician.process_job.process_wav` respectively.

    Args:
        opt_queue: Ordered list of opt job directories discovered during the
            current registration walk.
        dos_queue: Ordered list of opt job directories that have a ``dos`` note.
        wav_queue: Ordered list of opt job directories that have a ``wav`` note.
        machine: The machine the process is currently running on.
        opt_jobs: In-memory map of all known optimisation jobs.
        dos_jobs: In-memory map of all known DOS jobs.
        wav_jobs: In-memory map of all known WAV jobs.
        clear_certificate: When ``True``, removes any existing convergence
            certificate before re-evaluating convergence.
        home_dir: Absolute path to the user's home directory (or ``$WORK/..``
            on TACC).
        ssh_config: SSH/SCP connection configuration.
        preliminary_results: Open writable file handle for intermediate results.
        continue_past_limit: When ``True``, does not raise on hitting the
            submission limit.
        limit: Maximum number of jobs that may be queued at once.
        sub_queue: Accumulator list of job directories pending ``sbatch``
            submission.
        hit_limit: Whether the submission limit was already reached.
    """
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
