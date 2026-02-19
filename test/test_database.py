import os

import pytest

import automagician.classes
from automagician.classes import DosJob, GoneJob, JobStatus, Machine, OptJob, WavJob
from automagician.database import Database


def test_db_init_empty_file(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    print("first db init sucessfull")
    names = database.db.execute("select name from sqlite_master where type='table'")
    assert check_db_tables(names)
    # Test to see if something was present if it gets overwriten
    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        ("/tmp", automagician.classes.JobStatus.CONVERGED.value, 0, 0),
    )
    database.db.connection.commit()
    database = Database(database_path)

    print("Second db init passed")
    names = database.db.execute("select name from sqlite_master where type='table'")
    assert check_db_tables(names)
    opt_jobs = database.db.execute("SELECT * from opt_jobs").fetchall()
    assert len(opt_jobs) == 1
    # Test to see that data was the same
    job = opt_jobs[0]
    assert job[0] == "/tmp"
    assert job[1] == automagician.classes.JobStatus.CONVERGED.value
    assert job[2] == 0
    assert job[3] == 0

    database.db.execute("DROP TABLE dos_jobs")
    database = Database(database_path)
    names = database.db.execute("select name from sqlite_master where type='table'")
    assert check_db_tables(names)

    opt_jobs = database.db.execute("SELECT * from opt_jobs").fetchall()
    assert len(opt_jobs) == 1
    # Test to see that data was the same
    job = opt_jobs[0]
    assert job[0] == "/tmp"
    assert job[1] == automagician.classes.JobStatus.CONVERGED.value
    assert job[2] == 0
    assert job[3] == 0
    database.db.connection.commit()


def test_get_string_from_db_names(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    names = database.get_string_from_db(
        "select name from sqlite_master where type='table'"
    )

    assert names == "opt_jobs"


def test_get_string_from_db_invalid_command(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    with pytest.raises(Exception):
        database.get_string_from_db(
            "select nothing from sqlite_master where type='table'"
        )


def test_get_string_from_db_none(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    names = database.get_string_from_db("SELECT * from opt_jobs")

    assert names == ""


def test_delpwd_remove_one(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        ("/tmp", automagician.classes.JobStatus.CONVERGED.value, 2, 4),
    )
    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        ("/tmp/automagician", automagician.classes.JobStatus.NOT_FOUND.value, 1, 3),
    )
    database.db.connection.commit()
    database.delpwd("/tmp")
    entries = database.db.execute("select * from opt_jobs").fetchall()
    print(entries)
    assert len(entries) == 1
    assert entries[0][0] == "/tmp/automagician"
    assert entries[0][1] == automagician.classes.JobStatus.NOT_FOUND.value
    assert entries[0][2] == 1
    assert entries[0][3] == 3


def test_delpwd_remove_none(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        ("/tmp/automagician", automagician.classes.JobStatus.NOT_FOUND.value, 1, 3),
    )
    database.db.connection.commit()
    database.delpwd("/nonsense")
    entries = database.db.execute("select * from opt_jobs").fetchall()
    print(entries)
    assert len(entries) == 1
    assert entries[0][0] == "/tmp/automagician"
    assert entries[0][1] == automagician.classes.JobStatus.NOT_FOUND.value
    assert entries[0][2] == 1
    assert entries[0][3] == 3


def test_delpwd_remove_all(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        ("/tmp", automagician.classes.JobStatus.CONVERGED.value, 2, 4),
    )
    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        ("/tmp", automagician.classes.JobStatus.NOT_FOUND.value, 1, 3),
    )
    database.db.connection.commit()
    database.delpwd("/tmp")
    entries = database.db.execute("select * from opt_jobs").fetchall()
    print(entries)
    assert len(entries) == 0


def test_write_plain_text_db_with_1_value(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    opt_job_1 = OptJob(JobStatus.NOT_FOUND, Machine.FRI, Machine.FRI)
    database.add_opt_job_to_db(opt_job_1, "/tmp/hi")
    plain_text_path = os.path.join(tmp_path, "plain_text_db")
    database.write_plain_text_db(plain_text_path)

    assert os.path.exists(plain_text_path)
    with open(plain_text_path, "r") as f:
        lines = f.readlines()
        assert lines[0] == "OPT JOBS\n"
        assert lines[1] == "   status    | home machine |    last on    | job dir\n"
        assert lines[2] == "-------------|--------------|---------------|--------\n"
        assert lines[3] == "NOT_FOUND    |FRI           |FRI            |/tmp/hi\n"
        assert len(lines) == 4


def test_write_plain_text_db_with_multiple_opt(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    opt_jobs = {
        "/tmp/job1": OptJob(
            JobStatus.NOT_FOUND, Machine.FRI, Machine.FRONTERA_TACC
        ),
        "/tmp/job2": OptJob(
            JobStatus.CONVERGED, Machine.HALIFAX, Machine.LS6_TACC
        ),
        "/tmp/job3": OptJob(
            JobStatus.ERROR, Machine.STAMPEDE2_TACC, Machine.STAMPEDE2_TACC
        ),
        "/tmp/job4": OptJob(JobStatus.RUNNING, Machine.LS6_TACC, Machine.HALIFAX),
        "/tmp/job5": OptJob(
            JobStatus.INCOMPLETE, Machine.FRONTERA_TACC, Machine.FRI
        ),
    }
    database.write_job_statuses(opt_jobs, {}, {})

    plain_text_path = os.path.join(tmp_path, "plain_text_db")
    database.write_plain_text_db(plain_text_path)

    assert os.path.exists(plain_text_path)
    with open(plain_text_path, "r") as f:
        lines = f.readlines()
        assert lines[0] == "OPT JOBS\n"
        assert lines[1] == "   status    | home machine |    last on    | job dir\n"
        assert lines[2] == "-------------|--------------|---------------|--------\n"
        assert lines[3] == "NOT_FOUND    |FRI           |FRONTERA_TACC  |/tmp/job1\n"
        assert lines[4] == "CONVERGED    |HALIFAX       |LS6_TACC       |/tmp/job2\n"
        assert lines[5] == "ERROR        |STAMPEDE2_TACC|STAMPEDE2_TACC |/tmp/job3\n"
        assert lines[6] == "RUNNING      |LS6_TACC      |HALIFAX        |/tmp/job4\n"
        assert lines[7] == "INCOMPLETE   |FRONTERA_TACC |FRI            |/tmp/job5\n"
        assert len(lines) == 8


def test_write_plain_text_db_with_multiple_opt_dos_wav(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    opt_jobs = {
        "/tmp/job1": OptJob(
            JobStatus.NOT_FOUND, Machine.FRI, Machine.FRONTERA_TACC
        ),
        "/tmp/job2": OptJob(
            JobStatus.CONVERGED, Machine.HALIFAX, Machine.LS6_TACC
        ),
        "/tmp/job3": OptJob(
            JobStatus.ERROR, Machine.STAMPEDE2_TACC, Machine.STAMPEDE2_TACC
        ),
        "/tmp/job4": OptJob(JobStatus.RUNNING, Machine.LS6_TACC, Machine.HALIFAX),
        "/tmp/job5": OptJob(
            JobStatus.INCOMPLETE, Machine.FRONTERA_TACC, Machine.FRI
        ),
    }
    dos_jobs = {
        "/tmp/job1": DosJob(
            -1,
            JobStatus.NOT_FOUND,
            JobStatus.INCOMPLETE,
            Machine.FRI,
            Machine.FRONTERA_TACC,
        ),
        "/tmp/job2": DosJob(
            -1,
            JobStatus.CONVERGED,
            JobStatus.RUNNING,
            Machine.HALIFAX,
            Machine.LS6_TACC,
        ),
        "/tmp/job3": DosJob(
            -1,
            JobStatus.ERROR,
            JobStatus.ERROR,
            Machine.STAMPEDE2_TACC,
            Machine.STAMPEDE2_TACC,
        ),
        "/tmp/job4": DosJob(
            -1,
            JobStatus.RUNNING,
            JobStatus.CONVERGED,
            Machine.LS6_TACC,
            Machine.HALIFAX,
        ),
        "/tmp/job5": DosJob(
            -1,
            JobStatus.INCOMPLETE,
            JobStatus.NOT_FOUND,
            Machine.FRONTERA_TACC,
            Machine.FRI,
        ),
    }
    wav_jobs = {
        "/tmp/job1": WavJob(-1, JobStatus.NOT_FOUND, Machine.FRI),
        "/tmp/job2": WavJob(-1, JobStatus.CONVERGED, Machine.HALIFAX),
        "/tmp/job3": WavJob(-1, JobStatus.ERROR, Machine.STAMPEDE2_TACC),
        "/tmp/job4": WavJob(-1, JobStatus.RUNNING, Machine.LS6_TACC),
        "/tmp/job5": WavJob(-1, JobStatus.INCOMPLETE, Machine.FRONTERA_TACC),
    }
    database.write_job_statuses(opt_jobs, dos_jobs, wav_jobs)

    plain_text_path = os.path.join(tmp_path, "plain_text_db")
    database.write_plain_text_db(plain_text_path)

    assert os.path.exists(plain_text_path)
    with open(plain_text_path, "r") as f:
        lines = f.readlines()
        assert lines[0] == "OPT JOBS\n"
        assert lines[1] == "   status    | home machine |    last on    | job dir\n"
        assert lines[2] == "-------------|--------------|---------------|--------\n"
        assert lines[3] == "NOT_FOUND    |FRI           |FRONTERA_TACC  |/tmp/job1\n"
        assert lines[4] == "CONVERGED    |HALIFAX       |LS6_TACC       |/tmp/job2\n"
        assert lines[5] == "ERROR        |STAMPEDE2_TACC|STAMPEDE2_TACC |/tmp/job3\n"
        assert lines[6] == "RUNNING      |LS6_TACC      |HALIFAX        |/tmp/job4\n"
        assert lines[7] == "INCOMPLETE   |FRONTERA_TACC |FRI            |/tmp/job5\n"
        assert lines[8] == "DOS JOBS\n"
        assert (
            lines[9]
            == "  sc status  |  dos status |  sc last on  |  dos last on  | job dir\n"
        )
        assert (
            lines[10]
            == "-------------|-------------|--------------|---------------|--------\n"
        )
        assert (
            lines[11]
            == "NOT_FOUND    |INCOMPLETE   |FRI           |FRONTERA_TACC  |/tmp/job1/dos\n"
        )
        assert (
            lines[12]
            == "CONVERGED    |RUNNING      |HALIFAX       |LS6_TACC       |/tmp/job2/dos\n"
        )
        assert (
            lines[13]
            == "ERROR        |ERROR        |STAMPEDE2_TACC|STAMPEDE2_TACC |/tmp/job3/dos\n"
        )
        assert (
            lines[14]
            == "RUNNING      |CONVERGED    |LS6_TACC      |HALIFAX        |/tmp/job4/dos\n"
        )
        assert (
            lines[15]
            == "INCOMPLETE   |NOT_FOUND    |FRONTERA_TACC |FRI            |/tmp/job5/dos\n"
        )
        assert lines[16] == "WAV JOBS\n"
        assert lines[17] == "  wav status  |  wav last on  | job dir\n"
        assert lines[18] == "--------------|---------------|--------\n"
        assert lines[19] == "NOT_FOUND     |FRI            |/tmp/job1/wav\n"
        assert lines[20] == "CONVERGED     |HALIFAX        |/tmp/job2/wav\n"
        assert lines[21] == "ERROR         |STAMPEDE2_TACC |/tmp/job3/wav\n"
        assert lines[22] == "RUNNING       |LS6_TACC       |/tmp/job4/wav\n"
        assert lines[23] == "INCOMPLETE    |FRONTERA_TACC  |/tmp/job5/wav\n"
        assert len(lines) == 24


def test_write_plain_text_db_empty(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    plain_text_path = os.path.join(tmp_path, "plain_text_db")
    database.write_plain_text_db(plain_text_path)
    assert os.path.exists(plain_text_path)
    assert os.path.exists(plain_text_path)
    with open(plain_text_path, "r") as f:
        lines = f.readlines()
        assert lines[0] == "NO OPT JOBS FOUND\n"
        assert len(lines) == 1


def test_get_opt_jobs(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        (
            "/tmp",
            JobStatus.CONVERGED.value,
            Machine.FRI.value,
            Machine.FRONTERA_TACC.value,
        ),
    )
    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        (
            "/hi",
            JobStatus.NOT_FOUND.value,
            Machine.HALIFAX.value,
            Machine.LS6_TACC.value,
        ),
    )

    database.db.execute(
        "INSERT into dos_jobs values (?,?,?,?, ?)",
        (
            "/hi",
            -1,
            JobStatus.NOT_FOUND.value,
            Machine.STAMPEDE2_TACC.value,
            Machine.FRI.value,
        ),
    )
    database.db.connection.commit()

    result = database.get_opt_jobs()
    assert result == {
        "/tmp": OptJob(
            status=JobStatus.CONVERGED,
            home_machine=Machine.FRI,
            last_on=Machine.FRONTERA_TACC,
        ),
        "/hi": OptJob(
            status=JobStatus.NOT_FOUND,
            home_machine=Machine.HALIFAX,
            last_on=Machine.LS6_TACC,
        ),
    }


def test_get_dos_jobs(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        ("/tmp", JobStatus.CONVERGED.value, Machine.FRI, Machine.HALIFAX),
    )

    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        (
            "/tmp/hi",
            JobStatus.RUNNING.value,
            Machine.STAMPEDE2_TACC,
            Machine.FRONTERA_TACC,
        ),
    )
    database.db.connection.commit()
    opt_id_1 = int(
        database.get_string_from_db(
            'select rowid from opt_jobs where dir = "' + "/tmp" + '"'
        )
    )
    opt_id_2 = int(
        database.get_string_from_db(
            'select rowid from opt_jobs where dir = "' + "/tmp/hi" + '"'
        )
    )

    database.db.execute(
        "INSERT into dos_jobs values (?,?,?,?,?)",
        (
            opt_id_1,
            JobStatus.CONVERGED.value,
            JobStatus.NOT_FOUND.value,
            Machine.LS6_TACC,
            Machine.FRI,
        ),
    )

    database.db.execute(
        "INSERT into dos_jobs values (?,?,?,?,?)",
        (
            opt_id_2,
            JobStatus.RUNNING.value,
            JobStatus.CONVERGED.value,
            Machine.HALIFAX,
            Machine.FRONTERA_TACC,
        ),
    )
    database.db.connection.commit()

    result = database.get_dos_jobs()
    assert result == {
        "/tmp/dos": DosJob(
            opt_id=opt_id_1,
            sc_status=JobStatus.CONVERGED,
            dos_status=JobStatus.NOT_FOUND,
            sc_last_on=Machine.LS6_TACC,
            dos_last_on=Machine.FRI,
        ),
        "/tmp/hi/dos": DosJob(
            opt_id=opt_id_2,
            sc_status=JobStatus.RUNNING,
            dos_status=JobStatus.CONVERGED,
            sc_last_on=Machine.HALIFAX,
            dos_last_on=Machine.FRONTERA_TACC,
        ),
    }


def test_get_wav_jobs(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")

    database = Database(database_path)
    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        ("/tmp", JobStatus.CONVERGED.value, Machine.FRI, Machine.HALIFAX),
    )

    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        ("/tmp/hi", JobStatus.RUNNING.value, Machine.FRI, Machine.HALIFAX),
    )
    database.db.connection.commit()
    opt_id_1 = int(
        database.get_string_from_db(
            'select rowid from opt_jobs where dir = "' + "/tmp" + '"'
        )
    )
    opt_id_2 = int(
        database.get_string_from_db(
            'select rowid from opt_jobs where dir = "' + "/tmp/hi" + '"'
        )
    )

    database.db.execute(
        "INSERT into wav_jobs values (?,?,?)",
        (opt_id_1, JobStatus.CONVERGED.value, Machine.FRI),
    )

    database.db.execute(
        "INSERT into wav_jobs values (?,?,?)",
        (opt_id_2, JobStatus.RUNNING.value, Machine.HALIFAX),
    )
    database.db.connection.commit()

    result = database.get_wav_jobs()
    assert result == {
        "/tmp/wav": WavJob(
            opt_id=opt_id_1,
            wav_status=JobStatus.CONVERGED,
            wav_last_on=Machine.FRI,
        ),
        "/tmp/hi/wav": WavJob(
            opt_id=opt_id_2,
            wav_status=JobStatus.RUNNING,
            wav_last_on=Machine.HALIFAX,
        ),
    }


def test_write_job_status_dos_and_wav(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)

    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        (
            "/home/jw53959/opt_job_1",
            JobStatus.RUNNING.value,
            Machine.FRI,
            Machine.HALIFAX,
        ),
    )

    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        (
            "/home/jw53959/opt_job_2",
            JobStatus.CONVERGED.value,
            Machine.HALIFAX,
            Machine.STAMPEDE2_TACC,
        ),
    )

    database.db.connection.commit()

    opt_id_1 = int(
        database.get_string_from_db(
            'select rowid from opt_jobs where dir = "' + "/home/jw53959/opt_job_1" + '"'
        )
    )
    opt_id_2 = int(
        database.get_string_from_db(
            'select rowid from opt_jobs where dir = "' + "/home/jw53959/opt_job_2" + '"'
        )
    )

    opt_jobs = {
        "/home/jw53959/opt_job_1": OptJob(
            JobStatus.RUNNING, Machine.FRI, Machine.HALIFAX
        ),
        "/home/jw53959/opt_job_2": OptJob(
            JobStatus.CONVERGED, Machine.HALIFAX, Machine.STAMPEDE2_TACC
        ),
    }
    dos_jobs = {
        "/home/jw53959/opt_job_1/dos": DosJob(
            opt_id_1,
            JobStatus.RUNNING,
            JobStatus.CONVERGED,
            Machine.FRI,
            Machine.HALIFAX,
        ),
        "/home/jw53959/opt_job_2/dos": DosJob(
            opt_id_2,
            JobStatus.RUNNING,
            JobStatus.CONVERGED,
            Machine.STAMPEDE2_TACC,
            Machine.LS6_TACC,
        ),
    }
    wav_jobs = {
        "/home/jw53959/opt_job_1/wav": WavJob(
            opt_id_1, JobStatus.ERROR, Machine.FRI
        ),
        "/home/jw53959/opt_job_2/wav": WavJob(
            opt_id_2, JobStatus.NOT_FOUND, Machine.HALIFAX
        ),
    }
    database.write_job_statuses(opt_jobs, dos_jobs, wav_jobs)

    assert opt_jobs == {
        "/home/jw53959/opt_job_1": OptJob(
            JobStatus.RUNNING, Machine.FRI, Machine.HALIFAX
        ),
        "/home/jw53959/opt_job_2": OptJob(
            JobStatus.CONVERGED, Machine.HALIFAX, Machine.STAMPEDE2_TACC
        ),
    }
    assert dos_jobs == {
        "/home/jw53959/opt_job_1/dos": DosJob(
            opt_id_1,
            JobStatus.RUNNING,
            JobStatus.CONVERGED,
            Machine.FRI,
            Machine.HALIFAX,
        ),
        "/home/jw53959/opt_job_2/dos": DosJob(
            opt_id_2,
            JobStatus.RUNNING,
            JobStatus.CONVERGED,
            Machine.STAMPEDE2_TACC,
            Machine.LS6_TACC,
        ),
    }

    assert wav_jobs == {
        "/home/jw53959/opt_job_1/wav": WavJob(
            opt_id_1, JobStatus.ERROR, Machine.FRI
        ),
        "/home/jw53959/opt_job_2/wav": WavJob(
            opt_id_2, JobStatus.NOT_FOUND, Machine.HALIFAX
        ),
    }

    assert database.get_opt_jobs() == {
        "/home/jw53959/opt_job_1": OptJob(
            JobStatus.RUNNING, Machine.FRI, Machine.HALIFAX
        ),
        "/home/jw53959/opt_job_2": OptJob(
            JobStatus.CONVERGED, Machine.HALIFAX, Machine.STAMPEDE2_TACC
        ),
    }

    assert database.get_dos_jobs() == {
        "/home/jw53959/opt_job_1/dos": DosJob(
            opt_id_1,
            JobStatus.RUNNING,
            JobStatus.CONVERGED,
            Machine.FRI,
            Machine.HALIFAX,
        ),
        "/home/jw53959/opt_job_2/dos": DosJob(
            opt_id_2,
            JobStatus.RUNNING,
            JobStatus.CONVERGED,
            Machine.STAMPEDE2_TACC,
            Machine.LS6_TACC,
        ),
    }

    assert database.get_wav_jobs() == {
        "/home/jw53959/opt_job_1/wav": WavJob(
            opt_id_1, JobStatus.ERROR, Machine.FRI
        ),
        "/home/jw53959/opt_job_2/wav": WavJob(
            opt_id_2, JobStatus.NOT_FOUND, Machine.HALIFAX
        ),
    }


def test_write_job_status_opt_no_id_dos_and_wav(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)

    opt_job_1 = OptJob(JobStatus.RUNNING, Machine.FRI, Machine.FRI)
    opt_job_2 = OptJob(JobStatus.CONVERGED, Machine.FRI, Machine.FRI)
    dos_job_1 = DosJob(
        -1,
        JobStatus.RUNNING,
        JobStatus.CONVERGED,
        Machine.FRONTERA_TACC,
        Machine.HALIFAX,
    )
    dos_job_2 = DosJob(
        -1,
        JobStatus.RUNNING,
        JobStatus.CONVERGED,
        Machine.LS6_TACC,
        Machine.STAMPEDE2_TACC,
    )
    wav_job_1 = WavJob(-1, JobStatus.ERROR, Machine.FRI)
    wav_job_2 = WavJob(-1, JobStatus.NOT_FOUND, Machine.FRONTERA_TACC)

    opt_jobs = {
        "/home/jw53959/opt_job_1": opt_job_1,
        "/home/jw53959/opt_job_2": opt_job_2,
    }
    dos_jobs = {
        "/home/jw53959/opt_job_1/dos": dos_job_1,
        "/home/jw53959/opt_job_2/dos": dos_job_2,
    }
    wav_jobs = {
        "/home/jw53959/opt_job_1/wav": wav_job_1,
        "/home/jw53959/opt_job_2/wav": wav_job_2,
    }
    database.write_job_statuses(opt_jobs, dos_jobs, wav_jobs)

    assert opt_jobs == {
        "/home/jw53959/opt_job_1": opt_job_1,
        "/home/jw53959/opt_job_2": opt_job_2,
    }
    assert dos_jobs == {
        "/home/jw53959/opt_job_1/dos": dos_job_1,
        "/home/jw53959/opt_job_2/dos": dos_job_2,
    }

    assert wav_jobs == {
        "/home/jw53959/opt_job_1/wav": wav_job_1,
        "/home/jw53959/opt_job_2/wav": wav_job_2,
    }

    assert database.get_opt_jobs() == {
        "/home/jw53959/opt_job_1": opt_job_1,
        "/home/jw53959/opt_job_2": opt_job_2,
    }

    assert database.get_dos_jobs() == {
        "/home/jw53959/opt_job_1/dos": dos_job_1,
        "/home/jw53959/opt_job_2/dos": dos_job_2,
    }

    assert database.get_wav_jobs() == {
        "/home/jw53959/opt_job_1/wav": wav_job_1,
        "/home/jw53959/opt_job_2/wav": wav_job_2,
    }


def test_write_job_status_update_all(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)

    opt_job_1 = OptJob(JobStatus.RUNNING, Machine.FRI, Machine.HALIFAX)
    opt_job_2 = OptJob(JobStatus.CONVERGED, Machine.FRI, Machine.FRONTERA_TACC)

    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        (
            "/home/jw53959/opt_job_1",
            JobStatus.RUNNING.value,
            Machine.FRI,
            Machine.HALIFAX,
        ),
    )

    database.db.execute(
        "INSERT into opt_jobs values (?,?,?,?)",
        (
            "/home/jw53959/opt_job_2",
            JobStatus.CONVERGED.value,
            Machine.FRI,
            Machine.FRONTERA_TACC,
        ),
    )

    database.db.connection.commit()

    opt_id_1 = int(
        database.get_string_from_db(
            'select rowid from opt_jobs where dir = "' + "/home/jw53959/opt_job_1" + '"'
        )
    )
    opt_id_2 = int(
        database.get_string_from_db(
            'select rowid from opt_jobs where dir = "' + "/home/jw53959/opt_job_2" + '"'
        )
    )

    dos_job_1 = DosJob(
        opt_id_1,
        JobStatus.RUNNING,
        JobStatus.CONVERGED,
        Machine.FRI,
        Machine.HALIFAX,
    )
    dos_job_2 = DosJob(
        opt_id_2,
        JobStatus.RUNNING,
        JobStatus.CONVERGED,
        Machine.FRI,
        Machine.FRONTERA_TACC,
    )
    wav_job_1 = WavJob(opt_id_1, JobStatus.ERROR, Machine.LS6_TACC)
    wav_job_2 = WavJob(opt_id_2, JobStatus.NOT_FOUND, Machine.STAMPEDE2_TACC)

    opt_jobs = {
        "/home/jw53959/opt_job_1": opt_job_1,
        "/home/jw53959/opt_job_2": opt_job_2,
    }
    dos_jobs = {
        "/home/jw53959/opt_job_1/dos": dos_job_1,
        "/home/jw53959/opt_job_2/dos": dos_job_2,
    }
    wav_jobs = {
        "/home/jw53959/opt_job_1/wav": wav_job_1,
        "/home/jw53959/opt_job_2/wav": wav_job_2,
    }
    database.write_job_statuses(opt_jobs, dos_jobs, wav_jobs)

    opt_jobs["/home/jw53959/opt_job_1"] = OptJob(
        JobStatus.CONVERGED, Machine.FRONTERA_TACC, Machine.HALIFAX
    )
    dos_jobs["/home/jw53959/opt_job_1/dos"] = DosJob(
        opt_id_1,
        JobStatus.CONVERGED,
        JobStatus.RUNNING,
        Machine.STAMPEDE2_TACC,
        Machine.FRONTERA_TACC,
    )
    wav_jobs["/home/jw53959/opt_job_1/wav"] = WavJob(
        opt_id_1, JobStatus.RUNNING, Machine.FRI
    )

    database.write_job_statuses(opt_jobs, dos_jobs, wav_jobs)

    assert database.get_opt_jobs() == {
        "/home/jw53959/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRONTERA_TACC, Machine.HALIFAX
        ),
        "/home/jw53959/opt_job_2": opt_job_2,
    }

    assert database.get_dos_jobs() == {
        "/home/jw53959/opt_job_1/dos": DosJob(
            opt_id_1,
            JobStatus.CONVERGED,
            JobStatus.RUNNING,
            Machine.STAMPEDE2_TACC,
            Machine.FRONTERA_TACC,
        ),
        "/home/jw53959/opt_job_2/dos": dos_job_2,
    }

    assert database.get_wav_jobs() == {
        "/home/jw53959/opt_job_1/wav": WavJob(
            opt_id_1, JobStatus.RUNNING, Machine.FRI
        ),
        "/home/jw53959/opt_job_2/wav": wav_job_2,
    }


def test_write_job_status_no_opt(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)

    dos_job_1 = DosJob(
        -1, JobStatus.RUNNING, JobStatus.CONVERGED, Machine.FRI, Machine.FRI
    )
    dos_job_2 = DosJob(
        -1, JobStatus.RUNNING, JobStatus.CONVERGED, Machine.FRI, Machine.FRI
    )
    wav_job_1 = WavJob(-1, JobStatus.ERROR, Machine.FRI)
    wav_job_2 = WavJob(-1, JobStatus.NOT_FOUND, Machine.FRI)

    opt_jobs = {}
    dos_jobs = {
        "/home/jw53959/opt_job_1/dos": dos_job_1,
        "/home/jw53959/opt_job_2/dos": dos_job_2,
    }
    wav_jobs = {
        "/home/jw53959/opt_job_1/wav": wav_job_1,
        "/home/jw53959/opt_job_2/wav": wav_job_2,
    }
    database.write_job_statuses(opt_jobs, dos_jobs, wav_jobs)
    assert opt_jobs == {}
    assert dos_jobs == {
        "/home/jw53959/opt_job_1/dos": dos_job_1,
        "/home/jw53959/opt_job_2/dos": dos_job_2,
    }

    assert wav_jobs == {
        "/home/jw53959/opt_job_1/wav": wav_job_1,
        "/home/jw53959/opt_job_2/wav": wav_job_2,
    }

    assert database.get_opt_jobs() == {}

    assert database.get_dos_jobs() == {}

    assert database.get_wav_jobs() == {}


def test_add_opt_to_db_add(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    opt_job_1 = OptJob(JobStatus.CONVERGED, Machine.FRI, Machine.HALIFAX)
    opt_job_2 = OptJob(
        JobStatus.CONVERGED, Machine.STAMPEDE2_TACC, Machine.FRONTERA_TACC
    )

    database.add_opt_job_to_db(opt_job_1, "/tmp/opt_job_1", True)
    assert database.get_opt_jobs() == {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRI, Machine.HALIFAX
        )
    }
    database.add_opt_job_to_db(opt_job_2, "/tmp/opt_job_2", True)
    assert database.get_opt_jobs() == {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRI, Machine.HALIFAX
        ),
        "/tmp/opt_job_2": OptJob(
            JobStatus.CONVERGED, Machine.STAMPEDE2_TACC, Machine.FRONTERA_TACC
        ),
    }


def test_add_opt_to_db_update(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    opt_job_1 = OptJob(JobStatus.CONVERGED, Machine.FRI, Machine.HALIFAX)
    opt_job_2 = OptJob(
        JobStatus.CONVERGED, Machine.STAMPEDE2_TACC, Machine.FRONTERA_TACC
    )

    database.add_opt_job_to_db(opt_job_1, "/tmp/opt_job_1")
    database.add_opt_job_to_db(opt_job_2, "/tmp/opt_job_2", True)
    assert database.get_opt_jobs() == {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRI, Machine.HALIFAX
        ),
        "/tmp/opt_job_2": OptJob(
            JobStatus.CONVERGED, Machine.STAMPEDE2_TACC, Machine.FRONTERA_TACC
        ),
    }
    opt_job_2 = OptJob(JobStatus.NOT_FOUND, Machine.FRI, Machine.LS6_TACC)
    database.add_opt_job_to_db(opt_job_2, "/tmp/opt_job_2", True)
    assert database.get_opt_jobs() == {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRI, Machine.HALIFAX
        ),
        "/tmp/opt_job_2": OptJob(
            JobStatus.NOT_FOUND, Machine.FRI, Machine.LS6_TACC
        ),
    }

    assert database.get_dos_jobs() == {}
    assert database.get_wav_jobs() == {}


def test_add_dos_to_db_invalid(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    dos_job_1 = DosJob(
        -1, JobStatus.INCOMPLETE, JobStatus.ERROR, Machine.FRI, Machine.LS6_TACC
    )
    with pytest.raises(ValueError):
        database.add_dos_job_to_db(dos_job_1)

    assert database.get_opt_jobs() == {}
    assert database.get_dos_jobs() == {}
    assert database.get_wav_jobs() == {}


def test_add_dos_to_db_opt_dir_specified_invalid(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    dos_job_1 = DosJob(
        -1,
        JobStatus.INCOMPLETE,
        JobStatus.ERROR,
        Machine.FRI,
        Machine.HALIFAX,
    )
    with pytest.raises(ValueError):
        database.add_dos_job_to_db(dos_job_1, "/tmp/opt_job_1")

    assert database.get_opt_jobs() == {}
    assert database.get_dos_jobs() == {}
    assert database.get_wav_jobs() == {}


def test_add_dos_to_db_opt_dir_specified_valid(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    opt_job_1 = OptJob(JobStatus.CONVERGED, Machine.FRI, Machine.HALIFAX)
    database.add_opt_job_to_db(opt_job_1, "/tmp/opt_job_1", True)
    dos_job_1 = DosJob(
        -1,
        JobStatus.INCOMPLETE,
        JobStatus.ERROR,
        Machine.FRONTERA_TACC,
        Machine.STAMPEDE2_TACC,
    )
    database.add_dos_job_to_db(
        dos_job_1, "/tmp/opt_job_1", commit=True, add_opt_id=False
    )
    assert dos_job_1.opt_id == -1

    assert database.get_opt_jobs() == {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRI, Machine.HALIFAX
        )
    }
    dos_jobs = database.get_dos_jobs()
    assert len(dos_jobs) == 1
    dos_job_db = dos_jobs["/tmp/opt_job_1/dos"]

    opt_id = database.db.execute(
        "SELECT rowid from opt_jobs WHERE dir = ?", ["/tmp/opt_job_1"]
    ).fetchone()[0]
    assert dos_job_db.opt_id == opt_id
    assert dos_job_db.sc_status == JobStatus.INCOMPLETE
    assert dos_job_db.dos_status == JobStatus.ERROR
    assert dos_job_db.sc_last_on == Machine.FRONTERA_TACC
    assert dos_job_db.dos_last_on == Machine.STAMPEDE2_TACC
    assert database.get_wav_jobs() == {}


def test_add_dos_to_db_opt_dir_specified_add_opt_id(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    opt_job_1 = OptJob(JobStatus.CONVERGED, Machine.FRONTERA_TACC, Machine.FRI)
    database.add_opt_job_to_db(opt_job_1, "/tmp/opt_job_1", True)
    dos_job_1 = DosJob(
        -1,
        JobStatus.INCOMPLETE,
        JobStatus.ERROR,
        Machine.HALIFAX,
        Machine.FRI,
    )
    database.add_dos_job_to_db(dos_job_1, "/tmp/opt_job_1", commit=True)

    opt_id = database.db.execute(
        "SELECT rowid from opt_jobs WHERE dir = ?", ["/tmp/opt_job_1"]
    ).fetchone()[0]
    assert dos_job_1.opt_id == opt_id
    dos_id = dos_job_1.opt_id

    assert database.get_opt_jobs() == {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRONTERA_TACC, Machine.FRI
        )
    }
    assert database.get_dos_jobs() == {
        "/tmp/opt_job_1/dos": DosJob(
            dos_id,
            JobStatus.INCOMPLETE,
            JobStatus.ERROR,
            Machine.HALIFAX,
            Machine.FRI,
        )
    }
    assert database.get_wav_jobs() == {}


def test_add_dos_to_db_opt_id_specified_add(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    opt_job_1 = OptJob(JobStatus.CONVERGED, Machine.HALIFAX, Machine.FRI)
    database.add_opt_job_to_db(opt_job_1, "/tmp/opt_job_1", True)
    opt_id = database.db.execute(
        "SELECT rowid from opt_jobs WHERE dir = ?", ["/tmp/opt_job_1"]
    ).fetchone()[0]
    dos_job_1 = DosJob(
        opt_id,
        JobStatus.INCOMPLETE,
        JobStatus.ERROR,
        Machine.FRI,
        Machine.STAMPEDE2_TACC,
    )
    database.add_dos_job_to_db(dos_job_1, commit=True)

    opt_id = database.db.execute(
        "SELECT rowid from opt_jobs WHERE dir = ?", ["/tmp/opt_job_1"]
    ).fetchone()[0]
    assert dos_job_1.opt_id == opt_id
    dos_id = dos_job_1.opt_id

    assert database.get_opt_jobs() == {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.HALIFAX, Machine.FRI
        )
    }
    assert database.get_dos_jobs() == {
        "/tmp/opt_job_1/dos": DosJob(
            dos_id,
            JobStatus.INCOMPLETE,
            JobStatus.ERROR,
            Machine.FRI,
            Machine.STAMPEDE2_TACC,
        )
    }
    assert database.get_wav_jobs() == {}


def test_add_dos_to_db_opt_id_specified_update(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    opt_job_1 = OptJob(JobStatus.CONVERGED, Machine.FRI, Machine.STAMPEDE2_TACC)
    database.add_opt_job_to_db(opt_job_1, "/tmp/opt_job_1", True)
    opt_id = database.db.execute(
        "SELECT rowid from opt_jobs WHERE dir = ?", ["/tmp/opt_job_1"]
    ).fetchone()[0]
    dos_job_1 = DosJob(
        opt_id,
        JobStatus.INCOMPLETE,
        JobStatus.ERROR,
        Machine.FRI,
        Machine.STAMPEDE2_TACC,
    )
    database.add_dos_job_to_db(dos_job_1, commit=True)
    dos_job_2 = DosJob(
        opt_id,
        JobStatus.CONVERGED,
        JobStatus.CONVERGED,
        Machine.FRONTERA_TACC,
        Machine.FRI,
    )
    database.add_dos_job_to_db(dos_job_2)

    assert dos_job_1.opt_id == opt_id
    assert dos_job_2.opt_id == opt_id

    assert database.get_opt_jobs() == {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRI, Machine.STAMPEDE2_TACC
        )
    }
    assert database.get_dos_jobs() == {
        "/tmp/opt_job_1/dos": DosJob(
            opt_id,
            JobStatus.CONVERGED,
            JobStatus.CONVERGED,
            Machine.FRONTERA_TACC,
            Machine.FRI,
        )
    }
    assert database.get_wav_jobs() == {}


def test_add_wav_to_db_invalid(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    wav_job_1 = WavJob(-1, JobStatus.INCOMPLETE, Machine.FRI)
    with pytest.raises(ValueError):
        database.add_wav_job_to_db(wav_job_1)

    assert database.get_opt_jobs() == {}
    assert database.get_dos_jobs() == {}
    assert database.get_wav_jobs() == {}


def test_add_wav_to_db_opt_dir_specified_invalid(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    wav_job_1 = WavJob(-1, JobStatus.INCOMPLETE, Machine.FRONTERA_TACC)
    with pytest.raises(ValueError):
        database.add_wav_job_to_db(wav_job_1, "/tmp/opt_job_1")

    assert database.get_opt_jobs() == {}
    assert database.get_dos_jobs() == {}
    assert database.get_wav_jobs() == {}


def test_add_wav_to_db_opt_dir_specified_valid(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    opt_job_1 = OptJob(JobStatus.CONVERGED, Machine.FRONTERA_TACC, Machine.FRI)
    database.add_opt_job_to_db(opt_job_1, "/tmp/opt_job_1", True)
    wav_job_1 = WavJob(-1, JobStatus.INCOMPLETE, Machine.FRI)
    database.add_wav_job_to_db(
        wav_job_1, "/tmp/opt_job_1", commit=True, add_opt_id=False
    )
    assert wav_job_1.opt_id == -1

    assert database.get_opt_jobs() == {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRONTERA_TACC, Machine.FRI
        )
    }
    wav_jobs = database.get_wav_jobs()
    assert len(wav_jobs) == 1
    wav_job_db = wav_jobs["/tmp/opt_job_1/wav"]
    assert wav_job_1.opt_id == -1
    assert wav_job_db.wav_status == JobStatus.INCOMPLETE
    assert wav_job_db.wav_last_on == Machine.FRI
    assert database.get_dos_jobs() == {}


def test_add_wav_to_db_opt_dir_specified_add_opt_id(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    opt_job_1 = OptJob(
        JobStatus.CONVERGED, Machine.FRONTERA_TACC, Machine.HALIFAX
    )
    database.add_opt_job_to_db(opt_job_1, "/tmp/opt_job_1", True)
    wav_job_1 = WavJob(-1, JobStatus.INCOMPLETE, Machine.LS6_TACC)

    database.add_wav_job_to_db(wav_job_1, "/tmp/opt_job_1", commit=True)

    opt_id = database.db.execute(
        "SELECT rowid from opt_jobs WHERE dir = ?", ["/tmp/opt_job_1"]
    ).fetchone()[0]
    assert wav_job_1.opt_id == opt_id
    wav_id = wav_job_1.opt_id

    assert database.get_opt_jobs() == {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRONTERA_TACC, Machine.HALIFAX
        )
    }
    assert database.get_wav_jobs() == {
        "/tmp/opt_job_1/wav": WavJob(wav_id, JobStatus.INCOMPLETE, Machine.LS6_TACC)
    }
    assert database.get_dos_jobs() == {}


def test_add_wav_to_db_opt_id_specified_add(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    opt_job_1 = OptJob(JobStatus.CONVERGED, Machine.FRI, Machine.FRI)
    database.add_opt_job_to_db(opt_job_1, "/tmp/opt_job_1", True)
    opt_id = database.db.execute(
        "SELECT rowid from opt_jobs WHERE dir = ?", ["/tmp/opt_job_1"]
    ).fetchone()[0]
    print(opt_id)
    wav_job_1 = WavJob(opt_id, JobStatus.INCOMPLETE, Machine.FRONTERA_TACC)
    database.add_wav_job_to_db(wav_job_1, commit=True)
    assert wav_job_1.opt_id == opt_id

    assert database.get_opt_jobs() == {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRI, Machine.FRI
        )
    }
    assert database.get_wav_jobs() == {
        "/tmp/opt_job_1/wav": WavJob(
            opt_id, JobStatus.INCOMPLETE, Machine.FRONTERA_TACC
        )
    }
    assert database.get_dos_jobs() == {}


def test_add_wav_to_db_opt_id_specified_update(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    opt_job_1 = OptJob(JobStatus.CONVERGED, Machine.FRI, Machine.FRI)
    database.add_opt_job_to_db(opt_job_1, "/tmp/opt_job_1", True)
    opt_id = database.db.execute(
        "SELECT rowid from opt_jobs WHERE dir = ?", ["/tmp/opt_job_1"]
    ).fetchone()[0]
    wav_job_1 = WavJob(opt_id, JobStatus.INCOMPLETE, Machine.FRONTERA_TACC)
    database.add_wav_job_to_db(wav_job_1, commit=True)
    wav_job_2 = WavJob(opt_id, JobStatus.CONVERGED, Machine.FRI)
    database.add_wav_job_to_db(wav_job_2)
    assert wav_job_1.opt_id == opt_id
    assert wav_job_2.opt_id == opt_id

    assert database.get_opt_jobs() == {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRI, Machine.FRI
        )
    }
    assert database.get_wav_jobs() == {
        "/tmp/opt_job_1/wav": WavJob(opt_id, JobStatus.CONVERGED, Machine.FRI)
    }
    assert database.get_dos_jobs() == {}


def test_add_gone_to_db_add(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    gone_job_1 = GoneJob(
        "/tmp/opt_job_1", JobStatus.CONVERGED, Machine.FRONTERA_TACC, Machine.FRI
    )
    gone_job_2 = GoneJob(
        "/tmp/opt_job_2", JobStatus.CONVERGED, Machine.HALIFAX, Machine.LS6_TACC
    )

    database.add_gone_job_to_db(gone_job_1, True)
    assert database.get_gone_jobs() == {
        "/tmp/opt_job_1": GoneJob(
            "/tmp/opt_job_1",
            JobStatus.CONVERGED,
            Machine.FRONTERA_TACC,
            Machine.FRI,
        )
    }
    database.add_gone_job_to_db(gone_job_2, True)
    assert database.get_gone_jobs() == {
        "/tmp/opt_job_1": GoneJob(
            "/tmp/opt_job_1",
            JobStatus.CONVERGED,
            Machine.FRONTERA_TACC,
            Machine.FRI,
        ),
        "/tmp/opt_job_2": GoneJob(
            "/tmp/opt_job_2",
            JobStatus.CONVERGED,
            Machine.HALIFAX,
            Machine.LS6_TACC,
        ),
    }


def test_add_gone_to_db_update(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)
    gone_job_1 = GoneJob(
        "/tmp/opt_job_1", JobStatus.CONVERGED, Machine.FRONTERA_TACC, Machine.FRI
    )
    gone_job_2 = GoneJob(
        "/tmp/opt_job_2", JobStatus.CONVERGED, Machine.HALIFAX, Machine.LS6_TACC
    )

    database.add_gone_job_to_db(gone_job_1)
    database.add_gone_job_to_db(gone_job_2, True)
    assert database.get_opt_jobs() == {}
    assert database.get_gone_jobs() == {
        "/tmp/opt_job_1": GoneJob(
            "/tmp/opt_job_1",
            JobStatus.CONVERGED,
            Machine.FRONTERA_TACC,
            Machine.FRI,
        ),
        "/tmp/opt_job_2": GoneJob(
            "/tmp/opt_job_2",
            JobStatus.CONVERGED,
            Machine.HALIFAX,
            Machine.LS6_TACC,
        ),
    }
    gone_job_2 = GoneJob(
        "/tmp/opt_job_2", JobStatus.NOT_FOUND, Machine.FRI, Machine.HALIFAX
    )
    database.add_gone_job_to_db(gone_job_2, True)
    assert database.get_gone_jobs() == {
        "/tmp/opt_job_1": GoneJob(
            "/tmp/opt_job_1",
            JobStatus.CONVERGED,
            Machine.FRONTERA_TACC,
            Machine.FRI,
        ),
        "/tmp/opt_job_2": GoneJob(
            "/tmp/opt_job_2",
            JobStatus.NOT_FOUND,
            Machine.FRI,
            Machine.HALIFAX,
        ),
    }
    assert database.get_opt_jobs() == {}

    assert database.get_dos_jobs() == {}
    assert database.get_wav_jobs() == {}


def test_reset_job_status(tmp_path):
    database_path = os.path.join(tmp_path, "test_db")
    database = Database(database_path)

    opt_jobs = {
        "/tmp/opt_job_1": OptJob(
            JobStatus.CONVERGED, Machine.FRI, Machine.LS6_TACC
        ),
        "/tmp/opt_job_2": OptJob(
            JobStatus.ERROR, Machine.HALIFAX, Machine.FRONTERA_TACC
        ),
        "/tmp/opt_job_3": OptJob(
            JobStatus.NOT_FOUND, Machine.STAMPEDE2_TACC, Machine.STAMPEDE2_TACC
        ),
        "/tmp/opt_job_4": OptJob(
            JobStatus.INCOMPLETE, Machine.FRONTERA_TACC, Machine.HALIFAX
        ),
        "/tmp/opt_job_5": OptJob(JobStatus.RUNNING, Machine.LS6_TACC, Machine.FRI),
    }
    dos_jobs = {}
    wav_jobs = {}
    database.write_job_statuses(opt_jobs=opt_jobs, dos_jobs=dos_jobs, wav_jobs=wav_jobs)

    database.reset_job_status()

    opt_jobs = {
        "/tmp/opt_job_1": OptJob(
            JobStatus.INCOMPLETE, Machine.FRI, Machine.LS6_TACC
        ),
        "/tmp/opt_job_2": OptJob(
            JobStatus.INCOMPLETE, Machine.HALIFAX, Machine.FRONTERA_TACC
        ),
        "/tmp/opt_job_3": OptJob(
            JobStatus.INCOMPLETE, Machine.STAMPEDE2_TACC, Machine.STAMPEDE2_TACC
        ),
        "/tmp/opt_job_4": OptJob(
            JobStatus.INCOMPLETE, Machine.FRONTERA_TACC, Machine.HALIFAX
        ),
        "/tmp/opt_job_5": OptJob(
            JobStatus.INCOMPLETE, Machine.LS6_TACC, Machine.FRI
        ),
    }


def check_db_tables(names: list[str]):
    tables = 0
    for name in names:
        trimmed_name = name[0]
        if trimmed_name == "opt_jobs":
            tables |= 1
        elif trimmed_name == "dos_jobs":
            tables |= 2
        elif trimmed_name == "wav_jobs":
            tables |= 4
        elif trimmed_name == "gone_jobs":
            tables |= 8
        elif trimmed_name == "insta_submit":
            tables |= 16
        else:
            tables |= 32
    return tables == 31
