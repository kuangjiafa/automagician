import logging
import os
import sqlite3
from typing import Dict, Optional

import automagician.update_job as update_job
from automagician.classes import DosJob, GoneJob, JobStatus, Machine, OptJob, WavJob


class Database:
    """A wrapper around a sqlite3 database

    Attributes:
        db: a sqlite3.Cursor object that points to the database. It has the
        tables opt_jobs, dos_jobs, wav_jobs, gone_jobs, and insta_submit.
    """

    db: sqlite3.Cursor

    def __init__(self, path: str):
        """Created a database at path, adding the necessary tables

        Args:
          path: Where the database currently exists or should be placed
        """
        self.db = sqlite3.connect(path).cursor()
        has_opt = False
        has_dos = False
        has_wav = False
        has_gone = False
        has_insta_submit = False
        for table in self.db.execute(
                "select name from sqlite_master where type='table'"
        ):
            if table[0] == "opt_jobs":
                has_opt = True
            elif table[0] == "dos_jobs":
                has_dos = True
            elif table[0] == "wav_jobs":
                has_wav = True
            elif table[0] == "gone_jobs":
                has_gone = True
            elif table[0] == "insta_submit":
                has_insta_submit = True

        if not has_opt:
            self.db.execute(
                "create table opt_jobs (dir text, status int, home_machine int, last_on int)"
            )
        if not has_dos:
            self.db.execute(
                "create table dos_jobs (opt_id int, sc_status int, dos_status int, sc_last_on int, dos_last_on int)"
            )
        if not has_wav:
            self.db.execute(
                "create table wav_jobs (opt_id int, wav_status int, wav_last_on int)"
            )
        if not has_gone:
            self.db.execute(
                "create table gone_jobs (dir text, status int, home_machine int, last_on int)"
            )
        if not has_insta_submit:
            self.db.execute("create table insta_submit (dir text, machine_name text)")

    def get_string_from_db(self, cmd: str) -> str:
        """Executes the command and returns the first result of the query as a string

        Args:
          cmd (str): The command to execute in the database
        Returns:
          str: The results from the database as a string"""
        out = self.db.execute(cmd).fetchone()
        if out is None:
            return ""
        return str(out[0])

    def delpwd(self, cwd: str) -> None:
        """Deletes jobs from the database where the directory equals the given directory

        Args:
            cwd: The directory to remove"""
        logger = logging.getLogger()
        self.db.execute("delete from opt_jobs where dir = '" + cwd + "'")
        self.db.connection.commit()
        logger.info("%s is deleted from opt_jobs", cwd)

    def write_plain_text_db(self, file: str) -> None:
        """Prints out the text DB to the file in file. Overwrites content previously present

        Args:
            file: the path to the file to write the plain text db to"""
        with open(file, "w") as f:
            f.write(self._get_opt_jobs_str())
            f.write(self._get_dos_jobs_str())
            f.write(self._get_wav_jobs_str())

    def get_opt_jobs(self) -> Dict[str, OptJob]:
        """Returns the opt_jobs in this database.

        Returns:
            A dictionary where the keys are the job directoties, and the values
            are the opt jobs associated with said job directory"""
        opt_jobs = {}
        for job in self.db.execute("select * from opt_jobs"):
            opt_jobs[job[0]] = OptJob(
                status=JobStatus(job[1]),
                home_machine=Machine(job[2]),
                last_on=Machine(job[3]),
            )
        return opt_jobs

    def get_dos_jobs(self) -> Dict[str, DosJob]:
        """Returns the dos_jobs in this database.

        Returns:
            A dictionary where the keys are the job directoties, and the values
            are the dos jobs associated with said job directory"""
        dos_jobs = {}
        for job in self.db.execute("select * from dos_jobs").fetchall():
            opt_id = job[0]
            opt_dir = self.get_string_from_db(
                "select dir from opt_jobs where rowid = " + str(opt_id)
            )
            dos_dir = os.path.join(opt_dir, "dos")
            dos_jobs[dos_dir] = DosJob(
                opt_id=job[0],
                sc_status=JobStatus(job[1]),
                dos_status=JobStatus(job[2]),
                sc_last_on=Machine(job[3]),
                dos_last_on=Machine(job[4]),
            )
        return dos_jobs

    def get_wav_jobs(self) -> Dict[str, WavJob]:
        """Returns the wav_jobs in this database.

        Returns:
            A dictionary where the keys are the job directoties, and the values
            are the wav jobs associated with said job directory"""
        wav_jobs = {}
        for job in self.db.execute("select * from wav_jobs").fetchall():
            opt_id = job[0]
            opt_dir = self.get_string_from_db(
                "select dir from opt_jobs where rowid = " + str(opt_id)
            )
            wav_dir = os.path.join(opt_dir, "wav")
            wav_jobs[wav_dir] = WavJob(
                opt_id=job[0], wav_status=JobStatus(job[1]), wav_last_on=Machine(job[2])
            )
        return wav_jobs

    def get_gone_jobs(self) -> Dict[str, GoneJob]:
        """Returns the wav_jobs in this database.

        Returns:
            A dictionary where the keys are the job directories, and the values
            are the gone jobs associated with said job directory"""
        gone_jobs: Dict[str, GoneJob] = {}
        for job in self.db.execute("select * from gone_jobs"):
            gone_jobs[job[0]] = GoneJob(
                old_dir=job[0],
                status=JobStatus(job[1]),
                home_machine=Machine(job[2]),
                last_on=Machine(job[3]),
            )
        return gone_jobs

    def write_job_statuses(
            self,
            opt_jobs: Dict[str, OptJob],
            dos_jobs: Dict[str, DosJob],
            wav_jobs: Dict[str, WavJob],
    ) -> None:
        """Updates the database to include the jobs in opt_jobs, dos_jobs, and wav_jobs

        If a job exists in the database, but is not present here that job is not touched

        Args:
            opt_jobs: A collection of every opt_job known.
            dos_jobs: A collection of every dos_job known.
            wav_jobs: A collection of every wav_job known.
        """
        logger = logging.getLogger()
        for job_dir in opt_jobs:
            self.add_opt_job_to_db(opt_jobs[job_dir], job_dir, commit=False)

        for job_dir in dos_jobs:
            opt_dir = update_job.get_opt_dir(job_dir)
            try:
                self.add_dos_job_to_db(dos_jobs[job_dir], opt_dir, commit=False)
            except ValueError:
                logger.warning(
                    f"no opt job at directory {opt_dir}. Expected as was adding a dos_job"
                )
                continue

        for job_dir in wav_jobs:
            opt_dir = update_job.get_opt_dir(job_dir)
            try:
                self.add_wav_job_to_db(wav_jobs[job_dir], opt_dir, commit=False)
            except ValueError:
                logger.warning(
                    f"no opt job at directory {opt_dir}. Expected as was adding a wav_job"
                )
                continue

        self.db.connection.commit()
        logger.info("automagician.db updated")

    def add_opt_job_to_db(
            self, job_to_add: OptJob, opt_dir: str, commit: bool = True
    ) -> None:
        """Adds (or updates) a opt_job in the database.

        Args:
            job_to_add: The job to add to the database.
            opt_dir: The directory that this job lives in
            commit: Weither to commit the transaction. Committing the transaction
                is slower, but is needed to allow the data to be persisted.
                Turning this off is only recommended if you need to peform a
                lot of database operations and will commit at the end of
                peforming them.
        """
        row_id = self.db.execute(
            "select rowid from opt_jobs where dir = ?", [opt_dir]
        ).fetchone()
        if row_id is not None:
            self.db.execute(
                "update opt_jobs set dir = ?, status = ?, home_machine = ?, last_on = ? where rowid = ?",
                [
                    opt_dir,
                    job_to_add.status.value,
                    job_to_add.home_machine.value,
                    job_to_add.last_on.value,
                    row_id[0],
                ],
            )
        else:
            self.db.execute(
                "insert into opt_jobs values (?, ?, ?, ?)",
                [
                    opt_dir,
                    job_to_add.status.value,
                    job_to_add.home_machine.value,
                    job_to_add.last_on.value,
                ],
            )

        if commit:
            self.db.connection.commit()

    def add_dos_job_to_db(
            self,
            job_to_add: DosJob,
            opt_dir: Optional[str] = None,
            commit: bool = True,
            add_opt_id: bool = True,
    ) -> None:
        """Adds (or updates) a dos_job in the database.

        Args:
            job_to_add: The job to add to the database.
            opt_dir: The opt_dir that the job is connected to. MUST be set if
                job_to_add's opt_id is -1, or otherwise is not a row id of a
                opt_job.
            commit: Weither to commit the transaction. Committing the transaction
                is slower, but is needed to allow the data to be persisted.
                Turning this off is only recommended if you need to peform a
                lot of database operations and will commit at the end of
                peforming them.
            add_opt_id: If set will update job_to_add with the appropate opt_id
        Raises:
            ValueError: If there was not an opt_job at opt_dir"""
        if job_to_add.opt_id == -1:
            if opt_dir is None:
                raise ValueError("dos job must either have an opt_id or an opt_dir")
            opt_job_id = self.db.execute(
                "SELECT rowid from opt_jobs WHERE dir = ?", [opt_dir]
            ).fetchone()
            if opt_job_id is None:
                raise ValueError("There was not an opt_job in the directory")
            if add_opt_id:
                job_to_add.opt_id = opt_job_id[0]
            self.db.execute(
                "insert into dos_jobs values (?, ?, ?, ?, ?)",
                [
                    opt_job_id[0],
                    job_to_add.sc_status.value,
                    job_to_add.dos_status.value,
                    job_to_add.sc_last_on.value,
                    job_to_add.dos_last_on.value,
                ],
            )
        else:
            dos_id = self.db.execute(
                "SELECT rowid from dos_jobs WHERE rowid = ?", [job_to_add.opt_id]
            ).fetchone()
            if dos_id is not None:
                self.db.execute(
                    "update dos_jobs set sc_status = ?, dos_status = ?, sc_last_on = ?, dos_last_on = ? where rowid = ?",
                    [
                        job_to_add.sc_status.value,
                        job_to_add.dos_status.value,
                        job_to_add.sc_last_on.value,
                        job_to_add.dos_last_on.value,
                        dos_id[0],
                    ],
                )
            else:
                self.db.execute(
                    "insert into dos_jobs values (?, ?, ?, ?, ?)",
                    [
                        job_to_add.opt_id,
                        job_to_add.sc_status.value,
                        job_to_add.dos_status.value,
                        job_to_add.sc_last_on.value,
                        job_to_add.dos_last_on.value,
                    ],
                )
        if commit:
            self.db.connection.commit()

    def add_wav_job_to_db(
            self,
            job_to_add: WavJob,
            opt_dir: Optional[str] = None,
            commit: bool = True,
            add_opt_id: bool = True,
    ) -> None:
        """Adds (or updates) a wav_job in the database.

        Args:
            job_to_add: The job to add to the database.
            opt_dir: The opt_dir that the job is connected to. MUST be set if
                job_to_add's opt_id is -1, or otherwise is not a row id of a
                opt_job.
            commit: Weither to commit the transaction. Committing the transaction
                is slower, but is needed to allow the data to be persisted.
                Turning this off is only recommended if you need to peform a
                lot of database operations and will commit at the end of
                peforming them.
            add_opt_id: If set will update job_to_add with the appropate opt_id
        Raises:
            ValueError: If there was not an opt_job at opt_dir"""
        if job_to_add.opt_id == -1:
            if opt_dir is None:
                raise ValueError("dos job must either have an opt_id or an opt_dir")
            opt_job_id = self.db.execute(
                "SELECT rowid from opt_jobs WHERE dir = ?", [opt_dir]
            ).fetchone()
            if opt_job_id is None:
                raise ValueError("There was not an opt_job in the directory")
            if add_opt_id:
                job_to_add.opt_id = opt_job_id[0]
            self.db.execute(
                "insert into wav_jobs values (?, ?, ?)",
                (
                    opt_job_id[0],
                    job_to_add.wav_status.value,
                    job_to_add.wav_last_on.value,
                ),
            )
        else:
            wav_id = self.db.execute(
                "SELECT rowid from wav_jobs WHERE rowid = ?", [job_to_add.opt_id]
            ).fetchone()
            if wav_id is not None:
                self.db.execute(
                    "update wav_jobs set wav_status = ?, wav_last_on = ? where rowid = ?",
                    (
                        job_to_add.wav_status.value,
                        job_to_add.wav_last_on.value,
                        wav_id[0],
                    ),
                )
            else:
                self.db.execute(
                    "insert into wav_jobs values (?, ?, ?)",
                    (
                        job_to_add.opt_id,
                        job_to_add.wav_status.value,
                        job_to_add.wav_last_on.value,
                    ),
                )
        if commit:
            self.db.connection.commit()

    def add_gone_job_to_db(self, job_to_add: GoneJob, commit: bool = True) -> None:
        """Adds (or updates) a gone_job to the database.

        Args:
            job_to_add: The job to add to the database.
            commit: Weither to commit the transaction. Committing the transaction
                is slower, but is needed to allow the data to be persisted.
                Turning this off is only recommended if you need to peform a
                lot of database operations and will commit at the end of
                peforming them."""
        row_id = self.db.execute(
            "select rowid from gone_jobs where dir = ?", [job_to_add.old_dir]
        ).fetchone()
        if row_id is not None:
            self.db.execute(
                "update gone_jobs set dir = ?, status = ?, home_machine = ?, last_on = ? where rowid = ?",
                (
                    job_to_add.old_dir,
                    job_to_add.status.value,
                    job_to_add.home_machine.value,
                    job_to_add.last_on.value,
                    row_id[0],
                ),
            )
        else:
            self.db.execute(
                "insert into gone_jobs values (?, ?, ?, ?)",
                [
                    job_to_add.old_dir,
                    job_to_add.status.value,
                    job_to_add.home_machine.value,
                    job_to_add.last_on.value,
                ],
            )
        if commit:
            self.db.connection.commit()

    def reset_job_status(self) -> None:
        """Sets the status of optimization jobs to 1 which means unconverged"""
        self.db.execute("update opt_jobs set status = ?", (JobStatus.INCOMPLETE.value,))
        self.db.connection.commit()
        logger = logging.getLogger()
        logger.info("Jobs have been updated")

    def _get_opt_jobs_str(self) -> str:
        """Returns the opt jobs section for printing a plain text db"""
        lines = []
        opt_jobs = self.get_opt_jobs()
        if len(opt_jobs) == 0:
            return "NO OPT JOBS FOUND\n"
        lines.append("OPT JOBS\n")
        lines.append("   status    | home machine |    last on    | job dir\n")
        lines.append("-------------|--------------|---------------|--------\n")
        for job_dir in opt_jobs:
            job = opt_jobs[job_dir]
            lines.append(
                f"{job.status.name:13}|{job.home_machine.name:14}|{job.last_on.name:15}|{job_dir}\n"
            )
        return "".join(lines)  # Used to avoid costly string concatenation

    def _get_dos_jobs_str(self) -> str:
        """Returns the dos jobs section for printing a plain text db"""
        lines = []
        dos_jobs = self.get_dos_jobs()
        if len(dos_jobs) == 0:
            return ""
        lines.append("DOS JOBS\n")
        lines.append(
            "  sc status  |  dos status |  sc last on  |  dos last on  | job dir\n"
        )
        lines.append(
            "-------------|-------------|--------------|---------------|--------\n"
        )
        for job_dir in dos_jobs:
            job = dos_jobs[job_dir]
            lines.append(
                f"{job.sc_status.name:13}|{job.dos_status.name:13}|{job.sc_last_on.name:14}|{job.dos_last_on.name:15}|{job_dir}\n"
            )
        return "".join(lines)  # Used to avoid costly string concatenation

    def _get_wav_jobs_str(self) -> str:
        """Returns the wav jobs section for printing a plain text db"""
        lines = []
        wav_jobs = self.get_wav_jobs()
        if len(wav_jobs) == 0:
            return ""
        lines.append("WAV JOBS\n")
        lines.append("  wav status  |  wav last on  | job dir\n")
        lines.append("--------------|---------------|--------\n")
        for job_dir in wav_jobs:
            job = wav_jobs[job_dir]
            lines.append(
                f"{job.wav_status.name:14}|{job.wav_last_on.name:15}|{job_dir}\n"
            )
        return "".join(lines)  # Used to avoid costly string concatenation
