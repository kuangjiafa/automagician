#!/usr/bin/env python

from __future__ import annotations

from __future__ import annotations

import argparse
import logging
import os
import sys
import traceback

import automagician.constants as constants
import automagician.machine as machine_file
import automagician.process_job as process_job
import automagician.register as register
import automagician.small_functions as small_functions
from automagician.classes import JobLimitError, JobStatus
from automagician.database import Database

# def constants_check(is_silent: bool, is_verbose: bool) -> logging.Logger:
#  """Checks the existence of files required, such as a working subfile am.sub
#  """
#  home = os.path.expanduser("~")
#  file_path = os.path.join(home,"am.sub")
#  if not os.path.isfile(file_path):
#    print("The slurm subfile must be in home directory and named 'am.sub'!")
#    exit()


def set_up_logger(is_silent: bool, is_verbose: bool) -> logging.Logger:
    """Creates a logger and sets it up as the root logger

        The logger will log its output to standard output, and will prefix the
        log messages with the log level name followed by the message

        Note: Since this logger is set up to be the root logger this logger can
        be accessed by using logging.getLogger(), and logs can be made by simply
        calling logging.<log level>

    Args:
        is_silent: If set will set logging level to warn, meaning only warnings
            and errors will be printed
        is_verbose: If set will set logging level to debug, meaning every
            log is shown. Overrides is_silent
    Returns:
        The logger that is set up"""
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    if is_silent:
        root_logger.setLevel(logging.WARN)
        handler.setLevel(logging.WARN)
    if is_verbose:
        root_logger.setLevel(logging.DEBUG)
        handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter("%(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

    return root_logger


def set_up_parser() -> argparse.ArgumentParser:
    """Creates an argparse parser to help parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="helps in making it easier to work with jobs for automagician"
    )

    parser.add_argument(
        "-r",
        "--register",
        action="store_true",
        dest="register",
        default=False,
        help="Process and add all calculations contained in the current directory and all its subdirectories to known_jobs.dat",
    )
    parser.add_argument(
        "-p",
        "--process",
        action="store_true",
        dest="process",
        default=False,
        help="Check and re-process every job in unconverged_jobs.dat",
    )  # working
    parser.add_argument(
        "-s",
        "--silent",
        action="store_true",
        dest="silent",
        default=False,
        help="supress output",
    )  # working
    parser.add_argument(
        "--rsc",
        "--reset_converged",
        action="store_true",
        dest="reset_converged",
        default=False,
        help="move all converged jobs back into unconverged ones",
    )  # working
    parser.add_argument(
        "--rsa",
        "--reset_all",
        action="store_true",
        dest="reset_all",
        default=False,
        help="visit all jobs recorded in known_jobs.dat, add it to unconverged or converged accordingly if missing",
    )  # working
    parser.add_argument(
        "--cc",
        "--clear_certificate",
        action="store_true",
        dest="clear_certificate",
        default=False,
        help="Remove the certificate given to converged jobs.",
    )  # working
    parser.add_argument(
        "--ac",
        "--archive_converged",
        action="store_true",
        dest="archive_converged",
        default=False,
        help="Permanently log converged jobs",
    )  # working
    parser.add_argument(
        "--cpl",
        "--continue_past_limit",
        action="store_true",
        dest="continue_past_limit",
        default=False,
        help="Once the job limit is hit, continue processing directory but don't submit",
    )
    parser.add_argument(
        "-l",
        "--limit",
        action="store",
        dest="limit",
        type=int,
        default=99999,
        help="Stop this script if more than a number of jobs are in queue.",
    )  # working
    parser.add_argument(
        "-b",
        "--balance",
        action="store_true",
        dest="balance",
        default=False,
        help="Balance jobs between other machines",
    )  # working
    parser.add_argument(
        "--rcmb",
        action="store_true",
        dest="rcmb_flag",
        default=False,
        help="Simply recreate cmbFE.dat and cmbXDATCAR at current working directory",
    )  # not fully implemented
    parser.add_argument(
        "--dbplaintext",
        action="store_true",
        dest="dbplaintext_flag",
        default=False,
        help="Print all job status into a readable text file after finishing other commands",
    )  # not fully implemented
    parser.add_argument(
        "--dbcheck",
        action="store_true",
        dest="dbcheck_flag",
        default=False,
        help="Check interaction with database without submitting any jobs",
    )  # not fully implemented
    parser.add_argument(
        "--rjs",
        action="store_true",
        dest="resetjobstatus_flag",
        default=False,
        help="Reset all job statuses from converged to unconverged",
    )  # not fully implemented
    parser.add_argument(
        "--db_debug",
        action="store_true",
        dest="db_debug_flag",
        default=False,
        help="Used for debugging job directories",
    )  # not fully implemented
    parser.add_argument(
        "--delpwd",
        action="store_true",
        dest="delpwd_flag",
        default=False,
        help="Remove the present working directory from database",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        dest="verbose",
        default=False,
        help="Increase logging level, conflicts, and overwrites --silent",
    )
    return parser


def main() -> None:
    """A wrapper around main that sets up the parser and sends in an args array"""
    parser = set_up_parser()
    args = parser.parse_args()
    main_wrapper(args)


def main_wrapper(args: argparse.Namespace) -> None:
    """The entry point of the program

    Args:
        args: The parsed arguments from the CommandLine"""
    sub_queue: list[str] = []
    set_up_logger(args.silent, args.verbose)
    logger = logging.getLogger()
    try:
        machine = machine_file.get_machine_number()
        home = (
            os.environ["HOME"]
            if machine < 2
            else os.path.normpath(os.path.join(os.environ["WORK"], ".."))
        )
        ssh_config = machine_file.ssh_scp_init(machine, home, args.balance, logger)
        logger.debug(f"ssh_config is {str(ssh_config.config)}")
        machine_file.write_lockfile(ssh_config, machine)
        database = Database(os.path.join(home, constants.DB_NAME))
        opt_jobs = database.get_opt_jobs()
        dos_jobs = database.get_dos_jobs()
        wav_jobs = database.get_wav_jobs()
        tacc_queue_sizes = [0, 0, 0]
        process_job.get_submitted_jobs(
            machine,
            opt_jobs,
            dos_jobs,
            wav_jobs,
            tacc_queue_sizes,
        )
        preliminary_results = open(
            os.path.join(home, constants.PRELIMINARY_RESULTS_NAME), "w"
        )
        process_job.gone_job_check(database, opt_jobs)

        hit_limit = False
        try:
            if args.reset_converged:
                logger.warning("Reset converged is not working with sql database")
                small_functions.reset_converged(home)
            if args.archive_converged:
                logger.warning("Archive converged is not working with sql database")
                small_functions.archive_converged(home)
            if args.resetjobstatus_flag:
                database.reset_job_status()
            if args.register:
                logger.info("Registering all jobs in the current directory")
                register.register(
                    opt_jobs=opt_jobs,
                    dos_jobs=dos_jobs,
                    wav_jobs=wav_jobs,
                    machine=machine,
                    clear_certificate=args.clear_certificate,
                    home_dir=home,
                    ssh_config=ssh_config,
                    preliminary_results=preliminary_results,
                    continue_past_limit=args.continue_past_limit,
                    limit=args.limit,
                    sub_queue=sub_queue,
                    hit_limit=hit_limit,
                )
            if args.process:
                logger.info("Processing all unconverged optimization jobs")
                for direc in database.db.execute(
                    "select dir from opt_jobs where status = ?",
                    str(JobStatus.INCOMPLETE.value),
                ):
                    logger.info(f"inspecting recorded job: {direc[0]}")
                    if args.db_debug_flag:
                        continue
                    else:
                        if not os.path.exists(direc[0]):
                            logger.warning(f"{direc[0]} no longer exists!")
                            continue
                        else:
                            process_job.process_opt(
                                job_directory=direc[0],
                                machine=machine,
                                ssh_config=ssh_config,
                                opt_jobs=opt_jobs,
                                clear_certificate=args.clear_certificate,
                                home_dir=home,
                                preliminary_results=preliminary_results,
                                continue_past_limit=args.continue_past_limit,
                                limit=args.limit,
                                sub_queue=sub_queue,
                                hit_limit=hit_limit,
                            )

        except JobLimitError:
            logger.warning("JobLimitError")
            pass
        except Exception as e:
            logger.error(f"error: {e} cannot continue processing")
            logger.error(traceback.format_exc())
        finally:
            logger.info("done with command-specific stuff, time to submit!")
            process_job.submit_queue(
                machine=machine,
                balance=args.balance,
                ssh_config=ssh_config,
                sub_queue=sub_queue,
                home=home,
                tacc_queue_sizes=tacc_queue_sizes,
                opt_jobs=opt_jobs,
                dos_jobs=dos_jobs,
                wav_jobs=wav_jobs,
                database=database,
                limit=args.limit,
            )
            database.write_job_statuses(
                opt_jobs=opt_jobs,
                dos_jobs=dos_jobs,
                wav_jobs=wav_jobs,
            )
            if args.delpwd_flag:
                database.delpwd(os.getcwd())
            if args.dbplaintext_flag:
                logger.info(f"Writing plain text db to {constants.PLAIN_TEXT_DB_NAME}")
                database.write_plain_text_db(
                    os.path.join(home, constants.PLAIN_TEXT_DB_NAME)
                )
            preliminary_results.close()
            database.db.close()
            machine_file.automagic_exit(machine, ssh_config)
    except Exception:
        if (
            sys.exc_info()[0] is not None and sys.exc_info()[0].__name__ == "SystemExit"  # type: ignore
        ):
            exit()
        logger.error(
            "An error occurred when processing an automagician job. Will wrap up and exit"
        )
        traceback.print_exc()
        process_job.submit_queue(
            machine=machine,
            balance=args.balance,
            ssh_config=ssh_config,
            sub_queue=sub_queue,
            home=home,
            tacc_queue_sizes=tacc_queue_sizes,
            opt_jobs=opt_jobs,
            dos_jobs=dos_jobs,
            wav_jobs=wav_jobs,
            database=database,
            limit=args.limit,
        )
        database.write_job_statuses(
            opt_jobs=opt_jobs,
            dos_jobs=dos_jobs,
            wav_jobs=wav_jobs,
        )
        logger.warning(
            "interrupt received, lock released and job statuses written to sql db",
        )
        machine_file.automagic_exit(machine, ssh_config)


if __name__ == "__main__":
    main()
