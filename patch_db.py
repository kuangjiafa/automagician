import re

with open('src/automagician/database.py', 'r') as f:
    content = f.read()

# We want to replace write_job_statuses in database.py
# Let's find the write_job_statuses method
method_start = content.find("def write_job_statuses(")

# Let's find the start of the next method
next_method_start = content.find("def add_opt_job_to_db(", method_start)

new_method = """def write_job_statuses(
        self,
        opt_jobs: Dict[str, OptJob],
        dos_jobs: Dict[str, DosJob],
        wav_jobs: Dict[str, WavJob],
    ) -> None:
        \"\"\"Updates the database to include the jobs in opt_jobs, dos_jobs, and wav_jobs

        If a job exists in the database, but is not present here that job is not touched

        Args:
            opt_jobs: A collection of every opt_job known.
            dos_jobs: A collection of every dos_job known.
            wav_jobs: A collection of every wav_job known.
        \"\"\"
        import automagician.small_functions as small_functions

        logger = logging.getLogger()

        # 1. BATCH PROCESS OPT_JOBS
        if opt_jobs:
            existing_opt_rows = {}
            # Chunk the fetch to avoid SQL limits if necessary, but typically all records are fetched
            # Let's just fetch all opt_jobs directories mapping to rowid
            cursor = self.db.execute("SELECT dir, rowid FROM opt_jobs")
            for row in cursor:
                existing_opt_rows[row[0]] = row[1]

            opt_inserts = []
            opt_updates = []

            for job_dir, opt_job in opt_jobs.items():
                if job_dir in existing_opt_rows:
                    opt_updates.append((job_dir, opt_job.status.value, opt_job.home_machine.value, opt_job.last_on.value, existing_opt_rows[job_dir]))
                else:
                    opt_inserts.append((job_dir, opt_job.status.value, opt_job.home_machine.value, opt_job.last_on.value))

            if opt_inserts:
                self.db.executemany("INSERT INTO opt_jobs VALUES (?, ?, ?, ?)", opt_inserts)
            if opt_updates:
                self.db.executemany("UPDATE opt_jobs SET dir = ?, status = ?, home_machine = ?, last_on = ? WHERE rowid = ?", opt_updates)

        # Build a mapping of all opt_dir -> rowid from the database for dos and wav
        # We need this mapping to get the opt_id for dos_jobs and wav_jobs
        # since we might have just inserted new opt_jobs
        opt_dir_to_id = {}
        if dos_jobs or wav_jobs:
            cursor = self.db.execute("SELECT dir, rowid FROM opt_jobs")
            for row in cursor:
                opt_dir_to_id[row[0]] = row[1]

        # 2. BATCH PROCESS DOS_JOBS
        if dos_jobs:
            existing_dos_ids = set()
            cursor = self.db.execute("SELECT opt_id FROM dos_jobs")
            for row in cursor:
                existing_dos_ids.add(row[0])

            dos_inserts = []
            dos_updates = []

            for job_dir, dos_job in dos_jobs.items():
                opt_dir = small_functions.get_opt_dir(job_dir)
                opt_id = opt_dir_to_id.get(opt_dir)

                if opt_id is None:
                    logger.warning(
                        f"no opt job at directory {opt_dir}. Expected as was adding a dos_job"
                    )
                    continue

                if dos_job.opt_id == -1:
                    dos_job.opt_id = opt_id

                # Use the resolved opt_id from database
                resolved_opt_id = dos_job.opt_id

                if resolved_opt_id in existing_dos_ids:
                    dos_updates.append((dos_job.sc_status.value, dos_job.dos_status.value, dos_job.sc_last_on.value, dos_job.dos_last_on.value, resolved_opt_id))
                else:
                    dos_inserts.append((resolved_opt_id, dos_job.sc_status.value, dos_job.dos_status.value, dos_job.sc_last_on.value, dos_job.dos_last_on.value))

            if dos_inserts:
                self.db.executemany("INSERT INTO dos_jobs VALUES (?, ?, ?, ?, ?)", dos_inserts)
            if dos_updates:
                self.db.executemany("UPDATE dos_jobs SET sc_status = ?, dos_status = ?, sc_last_on = ?, dos_last_on = ? WHERE opt_id = ?", dos_updates)

        # 3. BATCH PROCESS WAV_JOBS
        if wav_jobs:
            existing_wav_ids = set()
            cursor = self.db.execute("SELECT opt_id FROM wav_jobs")
            for row in cursor:
                existing_wav_ids.add(row[0])

            wav_inserts = []
            wav_updates = []

            for job_dir, wav_job in wav_jobs.items():
                opt_dir = small_functions.get_opt_dir(job_dir)
                opt_id = opt_dir_to_id.get(opt_dir)

                if opt_id is None:
                    logger.warning(
                        f"no opt job at directory {opt_dir}. Expected as was adding a wav_job"
                    )
                    continue

                if wav_job.opt_id == -1:
                    wav_job.opt_id = opt_id

                resolved_opt_id = wav_job.opt_id

                if resolved_opt_id in existing_wav_ids:
                    wav_updates.append((wav_job.wav_status.value, wav_job.wav_last_on.value, resolved_opt_id))
                else:
                    wav_inserts.append((resolved_opt_id, wav_job.wav_status.value, wav_job.wav_last_on.value))

            if wav_inserts:
                self.db.executemany("INSERT INTO wav_jobs VALUES (?, ?, ?)", wav_inserts)
            if wav_updates:
                self.db.executemany("UPDATE wav_jobs SET wav_status = ?, wav_last_on = ? WHERE opt_id = ?", wav_updates)

        self.db.connection.commit()
        logger.info("automagician.db updated")

    """

new_content = content[:method_start] + new_method + content[next_method_start:]

with open('src/automagician/database.py', 'w') as f:
    f.write(new_content)
