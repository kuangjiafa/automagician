from __future__ import annotations

import logging
import os
import re
import shlex
import shutil
import subprocess
import traceback
from dataclasses import dataclass
from os.path import exists
from typing import TYPE_CHECKING, Dict, List, Optional, TextIO, Tuple, cast

import automagician.constants as constants
import automagician.create_job as create_job
import automagician.finish_job as finish_job
import automagician.machine as machine_file
import automagician.small_functions as small_functions
import automagician.update_job as update_job
from automagician.classes import (DosJob, GoneJob, JobStatus, Machine, OptJob,
                                  SSHConfig, WavJob)

if TYPE_CHECKING:
    from automagician.classes import SshScp
    from automagician.database import Database

try:
    from automagician.classes import SshScp  # noqa: F811

    def scp_get_dir(remote: str, local: str, ssh_scp: SshScp) -> None:
        """Puts files inside the remote directory to the local directory

        Args:
            remote: the directory on the remote machine to transfer files from
            local: the directory on the local machine to transfer files to
        """
        for f in ssh_scp.ssh.run(
            "cd " + remote + "; find . -type f | cut -c 2-"
        ).stdout.split("\n"):
            if len(f) < 1:
                continue
            ssh_scp.scp.get(remote + f, local + f)

except ImportError:
    pass


@dataclass
class SacctRecord:
    """Accounting information for a completed Slurm job, as returned by sacct."""

    job_id: str
    state: str  # e.g. COMPLETED, FAILED, TIMEOUT, NODE_FAIL, OUT_OF_MEMORY
    exit_code: str  # format "code:signal", e.g. "1:0"
    derived_exit_code: str  # highest exit code across all job steps
    failed_node: str  # node whose failure caused termination, if any
    max_rss: str  # peak resident set size (may be empty when using -X)
    elapsed: str  # wall time in [DD-]HH:MM:SS


# Slurm terminal states that indicate a cluster- or resource-level failure
# rather than a normal job completion.
_SLURM_ERROR_STATES = frozenset(
    {"FAILED", "TIMEOUT", "NODE_FAIL", "OUT_OF_MEMORY", "BOOT_FAIL", "PREEMPTED"}
)


def process_opt(
    job_directory: str,
    machine: Machine,
    opt_jobs: Dict[str, OptJob],
    clear_certificate: bool,
    home_dir: str,
    ssh_config: SSHConfig,
    preliminary_results: TextIO,
    continue_past_limit: bool,
    limit: int,
    sub_queue: List[str],
    hit_limit: bool,
) -> None:
    """Processes an opt job, checking to see if it has the required files, and is running

        If the job is running writes to preliminary results

        Updates machine last on for the particular OptJob in opt_jobs

    Args:
        job_directory: The directory the job can be found on
        machine: The machine that the user is currently logged on into
        opt_jobs: A set of every opt_job known
        clear_certificate: If true, will remove a convergence certificate if
            it exists for this job, and will calculate its convergence
            normally
        home_dir: The home directory of the job
        ssh_config: A config to remote into other machines
        preliminary_results: A file open that currently has write permissions
            new results will be writen to this
        continue_past_limit: Determines if hitting the limit will raise a
            JobLimitError, or not
        limit: How many jobs can currently be submitted at 1 time
        sub_queue: A list of all jobs to be sibmitted
        hit_limit: If the limit has already been set
    Throws:
        JobLimitError: If the job limit was hit, and continue_past_limit is not
        set
    """
    logger = logging.getLogger()
    subfile = machine_file.get_subfile(machine)
    logger.debug(f"process_opt {job_directory}")
    if machine < 2 and ssh_config.config != "NoSSH":
        if opt_jobs[job_directory].last_on == 1 - machine:
            logger.debug("scping from other machine")
            try:
                shutil.rmtree(job_directory)
                # mypy thinks config could be "NoSSH" here, but we checked above.
                # However, scp_get_dir expects SshScp, which is not imported here to avoid cycles.
                # Casting or ignoring for now as we know it's safe at runtime.
                machine_file.scp_get_dir(
                    home_dir + constants.AUTOMAGIC_REMOTE_DIR + job_directory,
                    job_directory,
                    ssh_config.config,
                )
            except Exception as e:
                logger.error(
                    f"Exception {e} occoured while processing an opt job in {job_directory}"
                )
                traceback.print_exc()
            opt_jobs[job_directory].last_on = machine

    if not check_has_opt(job_directory, subfile):
        logger.warning(f"No opt files found in {job_directory}!")
        return
    logger.debug(f"Found opt files in {job_directory}")

    if clear_certificate and os.path.exists(
        os.path.join(job_directory, constants.CONVERGENCE_CERTIFICATE_NAME)
    ):
        os.remove(os.path.join(job_directory, constants.CONVERGENCE_CERTIFICATE_NAME))

    is_converged = False
    is_running = False
    try:
        logger.debug(f"checking if job in {job_directory} is running")
        is_running = opt_jobs[job_directory].status == JobStatus.RUNNING
    except KeyError as e:
        logger.warning(f"is_running KeyError {e}")
        return

    if is_running:
        logger.debug(f"job in {job_directory} is running, do nothing")
        step, force, energy = get_residueSFE(job_directory)
        import automagician.update_job as update_job

        update_job.add_preliminary_results(
            job_directory, step, force, energy, preliminary_results
        )
        return
    if not os.path.exists(os.path.join(job_directory, "ll_out")):
        process_unconverged(
            job_directory=job_directory,
            opt_jobs=opt_jobs,
            continue_past_limit=continue_past_limit,
            limit=limit,
            sub_queue=sub_queue,
            machine=machine,
            hit_limit=hit_limit,
            preliminary_results=preliminary_results,
        )
        return
    error_fixed = False
    if check_error(job_directory):
        logger.warning(f"job in {job_directory} failed!")
        opt_jobs[job_directory].status = JobStatus.ERROR
        import automagician.update_job as update_job

        update_job.log_error(job_directory, home_dir)
        error_fixed = update_job.fix_error(
            job_directory=job_directory,
        )

    logger.debug(f"Determining convergence of job in {job_directory}")
    is_converged = determine_convergence(job_directory)
    if is_converged and not error_fixed:
        process_converged(job_directory, opt_jobs)
    else:
        process_unconverged(
            job_directory=job_directory,
            opt_jobs=opt_jobs,
            continue_past_limit=continue_past_limit,
            limit=limit,
            sub_queue=sub_queue,
            machine=machine,
            hit_limit=hit_limit,
            preliminary_results=preliminary_results,
        )


def get_residueSFE(job_directory: str) -> Tuple[int, float, float]:
    """Currently returns a tuple of 3 zeroes

    Goal seems to be to return the step, force, and total energy of the final step in a tuple
    """
    # this function cannot be used before cmbFE can be created correctly
    return 0, 0.0, 0.0


def check_error(job_directory: str) -> bool:
    """Returns True if this job reported an error, false otherwise

    Args:
      job_directory (str): The directory the job can be found on
    Returns:
      True iff ll_out shows an error, false otherwise"""
    logger = logging.getLogger()
    lloutpath = os.path.join(job_directory, "ll_out")

    error_found = False
    try:
        with open(lloutpath, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if "I REFUSE TO CONTINUE WITH THIS SICK JOB" in line:
                    error_found = True
                    break
    except OSError:
        return False

    if error_found:
        logger.warning(f"The job in {job_directory} reported an error!")
        return True
    else:
        return False


# This assumes that all converged calculations do not wrap up its last run
def determine_convergence(job_directory: str) -> bool:
    """Returns if this job has converged, works for all jobs including bulk relaxation.

    If no convergence certificate exists and both CONTCAR and ll_out are present,
    runs ``vef.pl``, which creates or updates ``fe.dat``.

    Args:
        job_directory (str): A path to the job directory. NO TRAILING SLASHES

    Returns:
        bool: True if the job was converged, False otherise
            NOTE: if a convergence certificate exists this always returns True
            This also uses the correct test of convergence for box convergence
    """
    # default
    # No CONTCAR and no ll_out is not converged
    logger = logging.getLogger()

    if os.path.exists(
        os.path.join(job_directory, constants.CONVERGENCE_CERTIFICATE_NAME)
    ):
        return True
    if not os.path.exists(os.path.join(job_directory, "CONTCAR")) or not os.path.exists(
        os.path.join(job_directory, "ll_out")
    ):
        return False
    # use ll_out to determine convergence
    logger.debug("running vef.pl")
    cwd = os.getcwd()
    os.chdir(job_directory)
    try:
        subprocess.call(["vef.pl"], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    finally:
        os.chdir(cwd)
    if not grep_ll_out_convergence(os.path.join(job_directory, "ll_out")):
        return False
    if is_isif3(job_directory):
        logger.debug(f"job in {job_directory} is a bulk relaxation job")
        return determine_box_convergence(job_directory)
    return True


def determine_box_convergence(job_directory: str) -> bool:
    """Returns true if box relaxation completed, false otherwise

    Args:
      job_directory (str): The directory of the job with the box convergence run
    Returns:
      bool: True iff there is exactly 1 line in the box relaxation"""
    logger = logging.getLogger()
    logger.debug(f"determining box convergence for {job_directory}")
    fedatname = os.path.join(job_directory, "fe.dat")
    line_number = 0
    with open(fedatname, "r") as fedat:
        line_number = len(fedat.readlines())
    if line_number == 0:
        logger.warning(f"The calculation in {job_directory} needs attention")
        return False
    elif line_number == 1:
        logger.debug(f"box relaxation finished for job in {job_directory}")
        return True
    else:
        logger.debug(f"number of lines in fe.dat more than 1 for {job_directory}")
        return False


# Determine if this job needs to be treated differently
def is_isif3(job_directory: str) -> bool:
    """Returns if the INCAR present in job_directory has the tag ISIF = 3

    Args:
      job_directory (str): The directory with the potential box relaxion job
    Returns:
      True if the INCAR has ISIF = 3 (whitespace ignored), false otherwise
    """
    isif3regex = re.compile(r"ISIF\s*=\s*3")
    with open(os.path.join(job_directory, "INCAR"), "r") as f:
        for line in f:
            if isif3regex.match(line):
                return True
    return False


def grep_ll_out_convergence(ll_out: str) -> bool:
    """Looks at ll_out to see if the reuqired accuracy has been met

    Args:
      ll_out (str): A path to ll.out to check accuracy
    Returns:
      bool: True iff the energy minimization was stopped due to required accuracy being met
      False otherwise"""
    try:
        with open(ll_out, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if (
                    "reached required accuracy - stopping structural energy minimisation"
                    in line
                ):
                    return True
    except OSError:
        return False
    return False


def process_converged(job_directory: str, opt_jobs: Dict[str, OptJob]) -> None:
    """creates a convergence certificate, and sets the job status to converged

    This would combine XCATCAR and FE if that was working

    Args:
        job_directory: A path to the job directory with the converged
            optomization job.
        opt_jobs: A collection of every optomization job
    """

    logger = logging.getLogger()
    logger.debug(f"optimization converged! {job_directory}")
    finish_job.give_certificate(job_directory)
    opt_jobs[
        job_directory
    ].status = JobStatus.CONVERGED  # 0 -> status 0 means converged


def process_unconverged(
    job_directory: str,
    opt_jobs: Dict[str, OptJob],
    continue_past_limit: bool,
    limit: int,
    sub_queue: List[str],
    machine: Machine,
    hit_limit: bool,
    preliminary_results: TextIO,
) -> None:
    """Adds the final values of the job to the preliminary_results file then resbumits

    If CONTCAR does not exist, or is of size 0, only resubmits, same if OUTCAR
        does not exist.

    Args:
        job_directory: A path to the directory with the unconverged job.
        opt_jobs: A collection of every optomization job known.
        continue_past_limit: Setting to true prevents a JobLimitError being
            raised when hitting the limit.
        limit: How many jobs to sumit at once.
        sub_queue: A list of jobs to be submitted.
        machine: The machine that the user is currently on.
        hit_limit: If the limit has been hit.
        preliminary_results: A openend file that is writable, used to note down
            preliminary results.
    Throws:
        JobLimitError: if submitting this job would hit the limit, and
            continue_past_limit is not set.
    """
    # First check if this is recorded as unconverged
    logger = logging.getLogger()
    logger.debug(f"processing unconverged job at {job_directory}")

    opt_jobs[
        job_directory
    ].status = JobStatus.INCOMPLETE  # 1 -> status 1 means unconverged

    # is CONTCAR empty? empty file have size 0. if not empty, wrap up if job is not running
    contcar_path = os.path.join(job_directory, "CONTCAR")
    outcar_path = os.path.join(job_directory, "OUTCAR")
    if not os.path.exists(contcar_path) or not os.path.exists(outcar_path):
        logger.debug("contcar or outcar is missing -> resubmit")
    elif os.path.getsize(contcar_path) != 0:
        logger.debug("contcar exists -> wrap up")
        finish_job.wrap_up(job_directory)
        step, force, energy = get_residueSFE(job_directory)
        import automagician.update_job as update_job

        update_job.add_preliminary_results(
            job_directory, step, force, energy, preliminary_results
        )

    create_job.add_to_sub_queue(
        job_directory=job_directory,
        continue_past_limit=continue_past_limit,
        limit=limit,
        sub_queue=sub_queue,
        machine=machine,
        hit_limit=hit_limit,
    )


def process_dos(
    job_directory: str,
    opt_jobs: Dict[str, OptJob],
    dos_jobs: Dict[str, DosJob],
    continue_past_limit: bool,
    limit: int,
    sub_queue: List[str],
    machine: Machine,
    hit_limit: bool,
) -> None:
    """Processes a DOS job and updates its status fields.

    If SC or DOS is complete, the corresponding status is set to
    ``JobStatus.CONVERGED``. If there is an error, the status is set to
    ``JobStatus.ERROR``.

    If the SC directory has an error, ``dos_status`` is set to
    ``JobStatus.INCOMPLETE``. If the DOS directory does not exist yet, it is
    created and the status is set to ``JobStatus.RUNNING``. If no SC directory
    exists, one is created and ``sc_status`` is set to ``JobStatus.RUNNING``.

    Args:
        job_directory: the path to the directory the job is located in
        opt_jobs: A collection of all optimisation jobs known by automagician.
        dos_jobs: A collection of all DOS jobs known by automagician.
        continue_past_limit: When ``True``, does not raise on hitting the limit.
        limit: Maximum number of jobs that may be queued at once.
        sub_queue: Accumulator list of directories pending submission.
        machine: The machine the user is currently logged into.
        hit_limit: Whether the submission limit has already been reached.
    Changes:
    """
    logger = logging.getLogger()
    logger.debug("process_dos " + job_directory)

    if job_directory not in opt_jobs:
        logger.warning(f"No OptJob found in {job_directory}.")
        return

    if (
        opt_jobs[job_directory].status != JobStatus.CONVERGED
    ):  # make parent converge first
        return

    sc_dir = os.path.join(job_directory, "sc")
    if os.path.isdir(sc_dir):
        if finish_job.sc_is_complete(sc_dir):
            dos_dir = os.path.join(job_directory, "dos")
            dos_jobs[job_directory].sc_status = JobStatus.CONVERGED
            if os.path.isdir(dos_dir):
                if job_directory not in dos_jobs:
                    logger.warning(f"No DosJob found in {job_directory}.")
                    return
                if finish_job.dos_is_complete(dos_dir):
                    dos_jobs[job_directory].dos_status = JobStatus.CONVERGED
                elif check_error(dos_dir):
                    dos_jobs[job_directory].dos_status = JobStatus.ERROR
            else:
                create_job.create_dos_from_sc(
                    job_directory=job_directory,
                    continue_past_limit=continue_past_limit,
                    limit=limit,
                    sub_queue=sub_queue,
                    machine=machine,
                    hit_limit=hit_limit,
                )
                if job_directory not in dos_jobs:
                    dos_jobs[job_directory] = DosJob(
                        -1,
                        JobStatus.CONVERGED,
                        JobStatus.RUNNING,
                        opt_jobs[job_directory].last_on,
                        machine,
                    )
                else:
                    dos_jobs[job_directory].sc_status = JobStatus.CONVERGED
                    dos_jobs[job_directory].dos_status = JobStatus.RUNNING
        elif check_error(sc_dir):
            dos_jobs[job_directory].sc_status = JobStatus.ERROR
            dos_jobs[job_directory].dos_status = JobStatus.INCOMPLETE

    else:
        logger.debug("no sc_dir -> create_sc")
        create_job.create_sc(
            job_directory=job_directory,
            continue_past_limit=continue_past_limit,
            limit=limit,
            sub_queue=sub_queue,
            machine=machine,
            hit_limit=hit_limit,
        )
        dos_jobs[job_directory].sc_status = JobStatus.RUNNING
        dos_jobs[job_directory].dos_status = JobStatus.INCOMPLETE


def process_wav(
    job_directory: str,
    opt_jobs: Dict[str, OptJob],
    wav_jobs: Dict[str, WavJob],
    continue_past_limit: bool,
    limit: int,
    sub_queue: List[str],
    machine: Machine,
    hit_limit: bool,
) -> None:
    """Processes a wav job, creating the wav directory and submitting if needed.

    Waits for the parent optimisation job to converge before acting.
    If the ``wav/`` subdirectory already exists, checks whether the WAVECAR
    calculation is complete (``JobStatus.CONVERGED``) or errored
    (``JobStatus.ERROR``).  If the directory does not exist yet, creates it via
    :func:`~automagician.create_job.create_wav` and marks the job as
    ``JobStatus.RUNNING``.

    Args:
        job_directory: Path to the optimisation job directory.
        opt_jobs: All known optimisation jobs.
        wav_jobs: All known wav jobs.
        continue_past_limit: When ``True``, submission continues even after the
            job limit is reached instead of raising :class:`~automagician.classes.JobLimitError`.
        limit: Maximum number of jobs that may be queued at once.
        sub_queue: Accumulator list of directories pending submission.
        machine: The machine the user is currently logged into.
        hit_limit: Whether the submission limit has already been reached.
    """
    logger = logging.getLogger()
    logger.debug(f"process_wav in {job_directory}")

    if (
        opt_jobs[job_directory].status != JobStatus.CONVERGED
    ):  # make parent converge first
        return

    wav_dir = os.path.join(job_directory, "wav")
    if os.path.isdir(wav_dir):
        if finish_job.wav_is_complete(wav_dir):
            # wav_jobs[job_directory].wav_status = 0
            wav_jobs[job_directory].wav_status = JobStatus.CONVERGED
        elif check_error(wav_dir):
            # wav_jobs[job_directory].wav_status = 0
            # TODO: Check that this is the intended behavior.
            wav_jobs[job_directory].wav_status = JobStatus.ERROR

    else:
        logger.debug("no wav_dir -> create_wav")
        create_job.create_wav(
            job_directory=job_directory,
            continue_past_limit=continue_past_limit,
            limit=limit,
            sub_queue=sub_queue,
            machine=machine,
            hit_limit=hit_limit,
        )
        wav_jobs[job_directory].wav_status = JobStatus.RUNNING

        # wav_jobs[job_directory].wav_status = -1


def query_sacct(dirs: List[str]) -> Dict[str, SacctRecord]:
    """Queries sacct for recent jobs matching the given working directories.

    Runs sacct for the current user over the past 7 days and matches records
    by working directory. For directories with multiple recent jobs the
    last-seen record wins (sacct returns records in submission order).

    Uses ``-X`` (``--allocations``) so each job produces exactly one row.
    MaxRSS is tracked at the step level and will be empty in this output;
    the field is included for forward-compatibility if that changes.

    Args:
        dirs: Working directory paths to look up in the accounting database.

    Returns:
        Dict mapping working directory to its most recent SacctRecord.
        Directories not found in sacct and systems without sacct are omitted.
    """
    if not dirs:
        return {}

    logger = logging.getLogger()
    try:
        result = subprocess.run(
            [
                "sacct",
                "--user", os.environ["USER"],
                "--starttime", "now-7days",
                "--format", "JobID,State,WorkDir,ExitCode,DerivedExitCode,FailedNode,MaxRSS,Elapsed",
                "--parsable2",
                "--noheader",
                "-X",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except FileNotFoundError:
        logger.debug("sacct not available on this system")
        return {}
    except subprocess.TimeoutExpired:
        logger.warning("sacct query timed out after 30 seconds")
        return {}

    if result.returncode != 0:
        logger.warning(f"sacct exited with code {result.returncode}: {result.stderr.strip()}")
        return {}

    dir_set = set(dirs)
    records: Dict[str, SacctRecord] = {}

    for line in result.stdout.splitlines():
        parts = line.split("|")
        if len(parts) < 8:
            continue
        job_id, state, work_dir, exit_code, derived_exit_code, failed_node, max_rss, elapsed = (
            parts[:8]
        )
        # CANCELLED states include a "by <uid>" suffix; strip it.
        state = state.split()[0] if state else state
        if work_dir in dir_set:
            records[work_dir] = SacctRecord(
                job_id=job_id,
                state=state,
                exit_code=exit_code,
                derived_exit_code=derived_exit_code,
                failed_node=failed_node,
                max_rss=max_rss,
                elapsed=elapsed,
            )

    return records


def _log_sacct_failures(dirs: List[str]) -> None:
    """Queries sacct for dirs that recently left the queue and logs Slurm-level failures.

    Only logs directories whose sacct state is in ``_SLURM_ERROR_STATES``.
    Silently skips directories not found in sacct (e.g. jobs still pending,
    or clusters where sacct/slurmdbd is not configured).

    Args:
        dirs: Working directory paths for jobs that were RUNNING but are no
            longer visible in squeue.
    """
    logger = logging.getLogger()
    records = query_sacct(dirs)
    for work_dir, record in records.items():
        if record.state in _SLURM_ERROR_STATES:
            details: List[str] = [
                f"state={record.state}",
                f"exit_code={record.exit_code}",
                f"elapsed={record.elapsed}",
            ]
            if record.failed_node:
                details.append(f"failed_node={record.failed_node}")
            if record.max_rss and record.max_rss not in ("", "0"):
                details.append(f"max_rss={record.max_rss}")
            logger.warning(
                f"sacct: job in {work_dir} (id={record.job_id}) terminated with "
                + ", ".join(details)
            )


def _get_submitted_jobs_slurm(
    machine: Machine,
    opt_jobs: Dict[str, OptJob],
    dos_jobs: Dict[str, DosJob],
    wav_jobs: Dict[str, WavJob],
) -> None:
    """Gets all currently running jobs and adds them to opt_jobs, dos_jobs, wav_jobs

        If a job is found and does not exist in opt_jobs, dos_jobs, or wav_jobs
        then said job is added to the appropate collection with the JobStatus
        fields set to either Running or Error depending on if the job is
        has detected an error.

        If a job exists in opt_jobs, dos_jobs, or wav_jobs then said jobs status
        is updated to either JobStatus.Running or JobStatus.Error depending
        on if the job reported an error.

        If a job reported an error the job is cancelled, and the error is logged
        so the user can look into the error

    Args:
        machine: The machine the user is currenty logged into
        opt_jobs: The collection of all opt jobs known by automagican
        dos_jobs: The collection of all dos jobs known by automagican
        wav_jobs: The collection of all wav jobs known by automagican
    """
    logger = logging.getLogger()
    try:
        all_jobs = subprocess.check_output(
            ["squeue", "-u", os.environ["USER"], "-o", "%A %t %Z"],
            stderr=subprocess.STDOUT,
            text=True,
        ).splitlines()
    except subprocess.CalledProcessError as e:
        output = e.output if isinstance(e.output, str) else e.output.decode(errors="replace")
        logger.error(f"squeue command failed (exit {e.returncode}): {output}")
        raise RuntimeError(
            f"Unable to query submitted jobs with squeue (exit {e.returncode})"
        ) from e
    except FileNotFoundError as e:
        logger.error("squeue not found; is SLURM available on this machine?")
        raise RuntimeError(
            "Unable to query submitted jobs because squeue is not available"
        ) from e
    for job in all_jobs[1:]:
        job_arr = job.split()
        if len(job_arr) < 3:
            logger.debug(f"skipping malformed squeue line: {job!r}")
            continue
        job_id = job_arr[0]
        job_sstatus = job_arr[1]  # slurm's status code
        job_dir = job_arr[2]

        job_status = JobStatus.RUNNING

        if job_sstatus in ["BF", "CA", "F", "NF", "OOM", "TO"]:
            logger.warning(
                f"job id={job_id}, dir={job_dir} is in error with status={job_sstatus}"
            )
            subprocess.call(["scancel", job_id])
            job_status = JobStatus.ERROR
        else:
            job_status = JobStatus.RUNNING

        job_type = small_functions.classify_job_dir(job_dir)
        if job_type in ["dos", "sc"]:
            opt_dir = small_functions.get_opt_dir(job_dir)
            if opt_dir not in dos_jobs:
                dos_jobs[opt_dir] = DosJob(
                    opt_id=-1,
                    sc_status=job_status,
                    dos_status=job_status,
                    sc_last_on=machine,
                    dos_last_on=machine,
                )

            if job_type == "dos":
                dos_jobs[opt_dir].dos_status = job_status
                dos_jobs[opt_dir].dos_last_on = machine
            else:
                dos_jobs[opt_dir].sc_status = job_status
                dos_jobs[opt_dir].sc_last_on = machine
        elif job_type == "wav":
            opt_dir = small_functions.get_opt_dir(job_dir)
            if opt_dir not in wav_jobs:
                wav_jobs[opt_dir] = WavJob(
                    opt_id=-1, wav_status=job_status, wav_last_on=machine
                )
            wav_jobs[opt_dir].wav_status = job_status
            wav_jobs[opt_dir].wav_last_on = machine
        else:
            if job_dir not in opt_jobs:
                opt_jobs[job_dir] = OptJob(
                    status=job_status, home_machine=machine, last_on=machine
                )
            else:
                opt_jobs[job_dir].status = job_status
                opt_jobs[job_dir].last_on = machine


def get_submitted_jobs(
    machine: Machine,
    opt_jobs: Dict[str, OptJob],
    dos_jobs: Dict[str, DosJob],
    wav_jobs: Dict[str, WavJob],
    tacc_queue_sizes: List[int],
) -> None:
    """Ensures only jobs that are actually running have JobStatus.Running set

        Every job that has JobStatus.Running set is reset to JobStatus.Incomplete

        Then we look at the list of jobs in the queue by the user.

        If a job is found and does not exist in opt_jobs, dos_jobs, or wav_jobs
        then said job is added to the appropate collection with the JobStatus
        fields set to either Running or Error depending on if the job is
        has detected an error.

        If a job exists in opt_jobs, dos_jobs, or wav_jobs then said jobs status
        is updated to either JobStatus.Running or JobStatus.Error depending
        on if the job reported an error.

        If a job reported an error the job is cancelled, and the error is logged
        so the user can look into the error

    Args:
        machine: The machine the user is currenty logged into
        opt_jobs: The collection of all opt jobs known by automagican
        dos_jobs: The collection of all dos jobs known by automagican
        wav_jobs: The collection of all wav jobs known by automagican
        tacc_queue_sizes: Shows howmany jobs this user has submitted to TACC
    """
    if machine in [0, 1]:  # fri
        # Snapshot which working dirs were RUNNING before we reset them.
        # For dos/wav the working dir is a subdirectory of the opt dir.
        running_working_dirs: Dict[str, Tuple[str, str]] = {}  # workdir -> (dict_key, job_type)
        for d, j in opt_jobs.items():
            if j.status == JobStatus.RUNNING:
                running_working_dirs[d] = (d, "opt")
        for d, j in dos_jobs.items():
            if j.sc_status == JobStatus.RUNNING:
                running_working_dirs[os.path.join(d, "sc")] = (d, "sc")
            if j.dos_status == JobStatus.RUNNING:
                running_working_dirs[os.path.join(d, "dos")] = (d, "dos")
        for d, j in wav_jobs.items():
            if j.wav_status == JobStatus.RUNNING:
                running_working_dirs[os.path.join(d, "wav")] = (d, "wav")

        for job_dir in opt_jobs:
            if opt_jobs[job_dir].status == JobStatus.RUNNING:
                opt_jobs[job_dir].status = JobStatus.INCOMPLETE
        for job_dir in dos_jobs:
            if dos_jobs[job_dir].sc_status == JobStatus.RUNNING:
                dos_jobs[job_dir].sc_status = JobStatus.INCOMPLETE
            if dos_jobs[job_dir].dos_status == JobStatus.RUNNING:
                dos_jobs[job_dir].dos_status = JobStatus.INCOMPLETE
        for job_dir in wav_jobs:
            if wav_jobs[job_dir].wav_status == JobStatus.RUNNING:
                wav_jobs[job_dir].wav_status = JobStatus.INCOMPLETE
        _get_submitted_jobs_slurm(machine, opt_jobs, dos_jobs, wav_jobs)

        # Find dirs that were RUNNING but are no longer in squeue (disappeared).
        # Query sacct to surface any Slurm-level failure reasons for these jobs.
        if running_working_dirs:
            disappeared: List[str] = []
            for work_dir, (key, jtype) in running_working_dirs.items():
                still_running: bool
                if jtype == "opt":
                    still_running = key in opt_jobs and opt_jobs[key].status == JobStatus.RUNNING
                elif jtype == "sc":
                    still_running = key in dos_jobs and dos_jobs[key].sc_status == JobStatus.RUNNING
                elif jtype == "dos":
                    still_running = key in dos_jobs and dos_jobs[key].dos_status == JobStatus.RUNNING
                else:  # wav
                    still_running = key in wav_jobs and wav_jobs[key].wav_status == JobStatus.RUNNING
                if not still_running:
                    disappeared.append(work_dir)
            _log_sacct_failures(disappeared)
    else:  # tacc
        for job_dir in opt_jobs:
            if opt_jobs[job_dir].status == JobStatus.RUNNING:
                tacc_queue_sizes[opt_jobs[job_dir].last_on - 2] = (
                    tacc_queue_sizes[opt_jobs[job_dir].last_on - 2] + 1
                )
                if opt_jobs[job_dir].last_on == machine:
                    opt_jobs[job_dir].status = JobStatus.INCOMPLETE
        for job_dir in dos_jobs:
            if dos_jobs[job_dir].sc_status == JobStatus.RUNNING:
                tacc_queue_sizes[dos_jobs[job_dir].sc_last_on - 2] = (
                    tacc_queue_sizes[dos_jobs[job_dir].sc_last_on - 2] + 1
                )
                if dos_jobs[job_dir].sc_last_on == machine:
                    dos_jobs[job_dir].sc_status = JobStatus.INCOMPLETE
            if dos_jobs[job_dir].dos_status == JobStatus.RUNNING:
                tacc_queue_sizes[dos_jobs[job_dir].dos_last_on - 2] = (
                    tacc_queue_sizes[dos_jobs[job_dir].dos_last_on - 2] + 1
                )
                if dos_jobs[job_dir].dos_last_on == machine:
                    dos_jobs[job_dir].dos_status = JobStatus.INCOMPLETE
        for job_dir in wav_jobs:
            if wav_jobs[job_dir].wav_status == JobStatus.RUNNING:
                tacc_queue_sizes[wav_jobs[job_dir].wav_last_on - 2] = (
                    tacc_queue_sizes[wav_jobs[job_dir].wav_last_on - 2] + 1
                )
                if wav_jobs[job_dir].wav_last_on == machine:
                    wav_jobs[job_dir].wav_status = JobStatus.INCOMPLETE
        _get_submitted_jobs_slurm(machine, opt_jobs, dos_jobs, wav_jobs)


def gone_job_check(
    database: Database,
    opt_jobs: Dict[str, OptJob],
) -> Dict[str, GoneJob]:
    """Checks optomization jobs and turns them into gone jobs if they do not exist

    A gone job is a job that's directory is not found

    Updates the database of gone_jobs with these new jobs
    Deletes the gone jobs from the opt_jobs table
    """
    logger = logging.getLogger()
    my_opt_jobs = database.get_opt_jobs()
    logger.info(f"COUNT OF OPT_JOBS: {len(my_opt_jobs)}")
    # for direc in db.execute('select dir from opt_jobs where status = 1'):
    gone_jobs_list: List[GoneJob] = []
    for job_dir in my_opt_jobs:
        current_opt_job = my_opt_jobs[job_dir]
        if current_opt_job.status != JobStatus.INCOMPLETE:
            continue
        if not exists(job_dir):
            logger.warning(f"{job_dir} no longer exists!")
            logger.warning(f"direc is {job_dir}")
            gone_jobs_list.append(
                GoneJob(
                    old_dir=job_dir,
                    status=current_opt_job.status,
                    home_machine=current_opt_job.home_machine,
                    last_on=current_opt_job.last_on,
                )
            )

    for j in gone_jobs_list:
        logger.info(f"Job to delete: {j.old_dir}")
        opt_jobs.pop(j.old_dir)

    database.add_gone_jobs_to_db(gone_jobs_list, commit=False)
    database.db.executemany(
        "delete from opt_jobs where dir = (?)",
        [(j.old_dir,) for j in gone_jobs_list],
    )
    database.db.connection.commit()
    return database.get_gone_jobs()


def check_has_opt(job_path: str, subfile: str) -> bool:
    """
    Checks if the path provides the necessary files for an optimization job
    Args:
        job_path (str): a path-like object where the job is located.
        subfile (str): the name of the subfile.
    Returns:
        bool: True if the list contains all of POSCAR, POTCAR, INCAR, KPOINTS,
        and the subfile.
    """
    files = os.listdir(job_path)
    calc_files = ["POSCAR", "POTCAR", "INCAR", "KPOINTS", subfile]
    for target_file in calc_files:
        if target_file not in files:
            return False
    return True


def submit_queue(
    machine: Machine,
    balance: bool,
    ssh_config: SSHConfig,
    sub_queue: List[str],
    home: str,
    tacc_queue_sizes: List[int],
    opt_jobs: Dict[str, OptJob],
    dos_jobs: Dict[str, DosJob],
    wav_jobs: Dict[str, WavJob],
    database: Database,
    limit: bool,
) -> None:
    """Submits the jobs to the queue of the machine

    When submitting to fri-halifax attempts to balance files based on how many jobs are in the queue

    When submitting to tacc  tires to determine if it will hit the limit then submits the jobs
    """
    logger = logging.getLogger()
    if len(sub_queue) >= limit:
        logger.warning(
            f"Hit limit of {limit}, Will not submit any jobs. Submission queue was {len(sub_queue)} jobs in size"
        )
        return
    subfile = machine_file.get_subfile(machine)
    logger.debug("starting queue submit")
    cwd = os.getcwd()
    try:
        if machine is Machine.FRI or machine is Machine.HALIFAX:  # fri-halifax
            other_subfile = machine_file.get_subfile(Machine(1 - machine))

            this_machine_job_count = len(
                str(subprocess.run(["squeue"], capture_output=True).stdout).split(r"\n")
            )
            other_machine_job_count = 0
            if ssh_config.config == "NoSSH":
                other_machine_job_count = 0
            elif isinstance(ssh_config.config, SshScp):
                other_squeue_output = ssh_config.config.ssh.run("squeue", hide=True).stdout
                other_machine_job_count = len(str(other_squeue_output).splitlines())
            diff_in_size = this_machine_job_count - other_machine_job_count
            num_to_sub = len(sub_queue)
            num_to_sub_there = num_to_sub / 2 + diff_in_size

            if not balance:
                num_to_sub_there = 0
            elif ssh_config.config == "NoSSH":
                num_to_sub_there = 0
            elif num_to_sub_there < 0:
                num_to_sub_there = 0
            elif num_to_sub_there > num_to_sub:
                num_to_sub_there = num_to_sub

            logger.debug(
                f"num to sub here is {str(num_to_sub - num_to_sub_there)} , num to sub there is {str(num_to_sub_there)}"
            )

            sub_queue_index = 0
            while sub_queue_index < num_to_sub_there:
                job_dir = sub_queue[sub_queue_index]
                update_job.switch_subfile(job_dir, other_subfile, subfile, machine)
                new_loc = home + constants.AUTOMAGIC_REMOTE_DIR + job_dir
                machine_file.scp_put_dir(job_dir, new_loc, ssh_config)
                # ssh_config is verified to be SshScp via match/if logic above
                cast("SshScp", ssh_config.config).ssh.run(
                    "cd "
                    + shlex.quote(new_loc)
                    + " && sbatch "
                    + shlex.quote(other_subfile)
                )
                update_job.set_status_for_newly_submitted_job(
                    job_dir, Machine(1 - machine), dos_jobs, wav_jobs, opt_jobs, False
                )
                sub_queue_index = sub_queue_index + 1

            while sub_queue_index < num_to_sub:
                job_dir = sub_queue[sub_queue_index]
                os.chdir(job_dir)
                sbatch_process = subprocess.run(
                    ["sbatch", os.path.join(job_dir, subfile)]
                )
                print(sbatch_process)
                print(sbatch_process.returncode)
                if sbatch_process.returncode != 0:
                    logger.warning(
                        f"sbatch exited with error code {sbatch_process.returncode} for the job in {job_dir}. "
                    )

                update_job.set_status_for_newly_submitted_job(
                    job_dir,
                    machine,
                    dos_jobs,
                    wav_jobs,
                    opt_jobs,
                    sbatch_process.returncode != 0,
                )
                sub_queue_index = sub_queue_index + 1

        else:  # tacc
            num_to_sub = len(sub_queue)
            logger.debug("num to submit is " + str(num_to_sub))
            num_can_sub = [0, 0, 0]
            total_free_spaces = 0
            num_will_sub = [0, 0, 0]
            # will_hit_limit = False

            for i in range(0, 3):
                num_can_sub[i] = constants.TACC_QUEUE_MAXES[i] - tacc_queue_sizes[i]
                total_free_spaces = total_free_spaces + num_can_sub[i]

            if not balance:
                total_free_spaces = num_can_sub[0]
                num_can_sub[1] = 0
                num_can_sub[2] = 0

            if total_free_spaces < num_to_sub:
                num_will_sub = num_can_sub
                # will_hit_limit = True
            else:
                for i in range(0, 3):
                    if total_free_spaces == 0:
                        continue
                    num_will_sub[i] = round(
                        num_can_sub[i] * num_to_sub / total_free_spaces
                    )
                    num_to_sub = num_to_sub - num_will_sub[i]
                    total_free_spaces = total_free_spaces - num_can_sub[i]

            sub_queue_index = 0
            for i in range(0, 3):
                for _ in range(0, num_will_sub[i]):
                    job_dir = sub_queue[sub_queue_index]
                    os.chdir(job_dir)
                    if i + 2 == machine:
                        subprocess.call(
                            ["sbatch", machine_file.get_subfile(Machine(i + 2))]
                        )
                    else:
                        update_job.switch_subfile(
                            job_dir,
                            machine_file.get_subfile(Machine(i + 2)),
                            subfile,
                            machine,
                        )
                        add_to_insta_submit(
                            job_dir,
                            machine_file.get_machine_name(Machine(i + 2)),
                            database,
                        )
                    update_job.set_status_for_newly_submitted_job(
                        job_dir, Machine(i + 2), dos_jobs, wav_jobs, opt_jobs, False
                    )
                    sub_queue_index = sub_queue_index + 1
    finally:
        os.chdir(cwd)


def add_to_insta_submit(job_dir: str, machine: str, database: Database) -> None:
    """Adds the jobs in job_dir into insta_submit

    Does not commit changes to the DB
    """
    database.db.execute("insert into insta_submit values (?, ?)", (job_dir, machine))
