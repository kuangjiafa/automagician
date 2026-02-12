import os
import shutil

import pytest

from automagician.classes import JobLimitError
from automagician.create_job import (
    add_to_sub_queue,
    copy_inputs,
    create_dos_from_sc,
    create_sc,
    create_wav,
)


def test_sub_hit_limit(tmp_path):
    subfile_path = str(tmp_path) + "/fri.sub"
    open(subfile_path, "w")
    jobs = []
    with pytest.raises(JobLimitError):
        add_to_sub_queue(
            job_directory=tmp_path,
            continue_past_limit=False,
            limit=1,
            sub_queue=jobs,
            machine=0,
            hit_limit=False,
        )

    assert tmp_path in jobs


def test_sub_hit_limit_allowed(tmp_path):
    subfile_path = str(tmp_path) + "/fri.sub"
    open(subfile_path, "w")
    jobs = []
    hit_limit = add_to_sub_queue(
        job_directory=tmp_path,
        continue_past_limit=True,
        limit=1,
        sub_queue=jobs,
        machine=0,
        hit_limit=False,
    )
    assert hit_limit is True
    assert tmp_path in jobs


def test_qsub_hit_limit_allowed_multiple_paths(tmp_path):
    subfile_path = str(tmp_path) + "/fri.sub"
    open(subfile_path, "w")
    sub_quene = ["/tmp/hi"]
    hit_limit = add_to_sub_queue(
        job_directory=tmp_path,
        continue_past_limit=True,
        limit=1,
        sub_queue=sub_quene,
        machine=0,
        hit_limit=False,
    )
    assert hit_limit is True
    assert sub_quene == ["/tmp/hi", tmp_path]


def test_sub_already_hit_limit(tmp_path):
    subfile_path = str(tmp_path) + "/fri.sub"
    open(subfile_path, "w")
    jobs = []
    hit_limit = add_to_sub_queue(
        job_directory=tmp_path,
        continue_past_limit=True,
        limit=1000,
        sub_queue=jobs,
        machine=0,
        hit_limit=True,
    )
    assert hit_limit is True
    assert tmp_path not in jobs


def test_sub_hit_limit_allowed_did_not_hit(tmp_path):
    subfile_path = str(tmp_path) + "/fri.sub"
    open(subfile_path, "w")
    jobs = []
    hit_limit = add_to_sub_queue(
        job_directory=tmp_path,
        continue_past_limit=True,
        limit=2,
        sub_queue=jobs,
        machine=0,
        hit_limit=False,
    )
    assert hit_limit is False
    assert tmp_path in jobs


def test_create_dos_from_sc_poscar(tmp_path):
    jobs = []
    old_cwd = os.getcwd()
    sc_job_path = os.path.join(tmp_path, "sc")
    dos_job_path = os.path.join(tmp_path, "dos")
    shutil.copytree("test/test_files/h2_sc", sc_job_path)
    os.remove(os.path.join(sc_job_path, "CONTCAR"))

    create_dos_from_sc(sc_job_path, False, 2, jobs, 0, False)
    new_cwd = os.getcwd()

    assert old_cwd == new_cwd
    assert os.path.isfile(os.path.join(dos_job_path, "INCAR"))
    assert os.path.isfile(os.path.join(dos_job_path, "fri.sub"))
    assert os.path.isfile(os.path.join(dos_job_path, "KPOINTS"))
    assert os.path.isfile(os.path.join(dos_job_path, "POTCAR"))
    assert os.path.isfile(os.path.join(dos_job_path, "CHGCAR"))
    assert os.path.isfile(os.path.join(dos_job_path, "POSCAR"))
    incar = open(os.path.join(dos_job_path, "INCAR"))
    incar_text = incar.read()
    assert "ICHARGE=11" in incar_text
    assert "LORBIT=11" in incar_text
    expected_jobs = [dos_job_path]
    assert jobs == expected_jobs


def test_create_dos_from_sc_contcar(tmp_path):
    sub_quene = []
    old_cwd = os.getcwd()
    sc_job_path = os.path.join(tmp_path, "sc")
    dos_job_path = os.path.join(tmp_path, "dos")
    shutil.copytree("test/test_files/h2_sc", sc_job_path)
    os.remove(os.path.join(sc_job_path, "POSCAR"))

    create_dos_from_sc(sc_job_path, False, 2, sub_quene, 0, False)
    new_cwd = os.getcwd()

    assert old_cwd == new_cwd
    assert os.path.isfile(os.path.join(dos_job_path, "INCAR"))
    assert os.path.isfile(os.path.join(dos_job_path, "fri.sub"))
    assert os.path.isfile(os.path.join(dos_job_path, "KPOINTS"))
    assert os.path.isfile(os.path.join(dos_job_path, "POTCAR"))
    assert os.path.isfile(os.path.join(dos_job_path, "CHGCAR"))
    assert os.path.isfile(os.path.join(dos_job_path, "CONTCAR"))
    incar = open(os.path.join(dos_job_path, "INCAR"))
    incar_text = incar.read()
    assert "ICHARGE=11" in incar_text
    assert "LORBIT=11" in incar_text
    expected_sub_quene = [dos_job_path]
    assert sub_quene == expected_sub_quene


def test_create_dos_from_sc_invalid_dir(tmp_path):
    sub_quene = []
    old_cwd = os.getcwd()
    sc_job_path = os.path.join(tmp_path, "sc")
    shutil.copytree("test/test_files/h2_sc", sc_job_path)
    os.remove(os.path.join(sc_job_path, "POSCAR"))
    os.remove(os.path.join(sc_job_path, "CONTCAR"))
    with pytest.raises(FileNotFoundError):
        create_dos_from_sc(sc_job_path, False, 2, sub_quene, 0, False)
    new_cwd = os.getcwd()

    assert old_cwd == new_cwd
    assert sub_quene == []


def test_create_wav_potcar(tmp_path):
    sub_quene = []
    old_cwd = os.getcwd()
    job_path = os.path.join(tmp_path, "job")
    wav_job_path = os.path.join(tmp_path, "wav")
    shutil.copytree("test/test_files/h2_sc", job_path)
    os.remove(os.path.join(job_path, "CONTCAR"))

    create_wav(job_path, False, 2, sub_quene, 0, False)
    new_cwd = os.getcwd()

    assert old_cwd == new_cwd
    assert os.path.isdir(wav_job_path)
    assert os.path.isfile(os.path.join(wav_job_path, "INCAR"))
    assert os.path.isfile(os.path.join(wav_job_path, "fri.sub"))
    assert os.path.isfile(os.path.join(wav_job_path, "KPOINTS"))
    assert os.path.isfile(os.path.join(wav_job_path, "POTCAR"))
    assert os.path.isfile(os.path.join(wav_job_path, "POSCAR"))
    incar = open(os.path.join(wav_job_path, "INCAR"))
    incar_text = incar.read()
    assert "IBRION=-1" in incar_text
    assert "LWAVE=.TRUE." in incar_text
    assert "NSW=0" in incar_text
    expected_sub_quene = [wav_job_path]
    assert sub_quene == expected_sub_quene


def test_create_wav_contcar(tmp_path):
    sub_quene = []
    old_cwd = os.getcwd()
    job_path = os.path.join(tmp_path, "job")
    wav_job_path = os.path.join(tmp_path, "wav")
    shutil.copytree("test/test_files/h2_sc", job_path)
    os.remove(os.path.join(job_path, "POSCAR"))

    create_wav(job_path, False, 2, sub_quene, 0, False)
    new_cwd = os.getcwd()

    assert old_cwd == new_cwd
    assert os.path.isdir(wav_job_path)
    assert os.path.isfile(os.path.join(wav_job_path, "INCAR"))
    assert os.path.isfile(os.path.join(wav_job_path, "fri.sub"))
    assert os.path.isfile(os.path.join(wav_job_path, "KPOINTS"))
    assert os.path.isfile(os.path.join(wav_job_path, "POTCAR"))
    assert os.path.isfile(os.path.join(wav_job_path, "CONTCAR"))
    incar = open(os.path.join(wav_job_path, "INCAR"))
    incar_text = incar.read()
    assert "IBRION=-1" in incar_text
    assert "LWAVE=.TRUE." in incar_text
    assert "NSW=0" in incar_text
    expected_sub_quene = [wav_job_path]
    assert sub_quene == expected_sub_quene


def test_create_sc_potcar(tmp_path):
    sub_quene = []
    old_cwd = os.getcwd()
    job_path = os.path.join(tmp_path, "job")
    sc_job_path = os.path.join(tmp_path, "sc")
    shutil.copytree("test/test_files/h2_sc", job_path)
    os.remove(os.path.join(job_path, "CONTCAR"))

    create_sc(job_path, False, 2, sub_quene, 0, False)
    new_cwd = os.getcwd()

    assert old_cwd == new_cwd
    assert os.path.isdir(sc_job_path)
    assert os.path.isfile(os.path.join(sc_job_path, "INCAR"))
    assert os.path.isfile(os.path.join(sc_job_path, "fri.sub"))
    assert os.path.isfile(os.path.join(sc_job_path, "KPOINTS"))
    assert os.path.isfile(os.path.join(sc_job_path, "POTCAR"))
    assert os.path.isfile(os.path.join(sc_job_path, "POSCAR"))
    incar = open(os.path.join(sc_job_path, "INCAR"))
    incar_text = incar.read()
    assert "IBRION=-1" in incar_text
    assert "LCHARGE=.TRUE." in incar_text
    assert "NSW=0" in incar_text
    expected_sub_quene = [sc_job_path]
    assert sub_quene == expected_sub_quene


def test_copy_inputs_full(tmp_path):
    job_dir = os.path.join(str(tmp_path), "job")
    os.makedirs(job_dir)
    dest_dir = os.path.join(str(tmp_path), "dest")

    subfile = "test.sub"
    with open(os.path.join(job_dir, subfile), "w") as f:
        f.write("sub")
    with open(os.path.join(job_dir, "KPOINTS"), "w") as f:
        f.write("kpoints")
    with open(os.path.join(job_dir, "POTCAR"), "w") as f:
        f.write("potcar")
    with open(os.path.join(job_dir, "INCAR"), "w") as f:
        f.write("incar")
    with open(os.path.join(job_dir, "CHGCAR"), "w") as f:
        f.write("chgcar")
    with open(os.path.join(job_dir, "CONTCAR"), "w") as f:
        f.write("contcar")
    with open(os.path.join(job_dir, "POSCAR"), "w") as f:
        f.write("poscar")

    copy_inputs(subfile, job_dir, dest_dir)

    assert os.path.isfile(os.path.join(dest_dir, subfile))
    assert os.path.isfile(os.path.join(dest_dir, "KPOINTS"))
    assert os.path.isfile(os.path.join(dest_dir, "POTCAR"))
    assert os.path.isfile(os.path.join(dest_dir, "INCAR"))
    assert os.path.isfile(os.path.join(dest_dir, "CHGCAR"))
    assert os.path.isfile(os.path.join(dest_dir, "CONTCAR"))
    # If CONTCAR exists, POSCAR should NOT be copied
    assert not os.path.isfile(os.path.join(dest_dir, "POSCAR"))


def test_copy_inputs_minimal(tmp_path):
    job_dir = os.path.join(str(tmp_path), "job")
    os.makedirs(job_dir)
    dest_dir = os.path.join(str(tmp_path), "dest")

    subfile = "test.sub"
    with open(os.path.join(job_dir, subfile), "w") as f:
        f.write("sub")
    with open(os.path.join(job_dir, "KPOINTS"), "w") as f:
        f.write("kpoints")
    with open(os.path.join(job_dir, "POTCAR"), "w") as f:
        f.write("potcar")
    with open(os.path.join(job_dir, "INCAR"), "w") as f:
        f.write("incar")
    with open(os.path.join(job_dir, "POSCAR"), "w") as f:
        f.write("poscar")

    copy_inputs(subfile, job_dir, dest_dir)

    assert os.path.isfile(os.path.join(dest_dir, subfile))
    assert os.path.isfile(os.path.join(dest_dir, "KPOINTS"))
    assert os.path.isfile(os.path.join(dest_dir, "POTCAR"))
    assert os.path.isfile(os.path.join(dest_dir, "INCAR"))
    assert os.path.isfile(os.path.join(dest_dir, "POSCAR"))
    assert not os.path.isfile(os.path.join(dest_dir, "CHGCAR"))
    assert not os.path.isfile(os.path.join(dest_dir, "CONTCAR"))


def test_copy_inputs_missing_mandatory(tmp_path):
    job_dir = os.path.join(str(tmp_path), "job")
    os.makedirs(job_dir)
    dest_dir = os.path.join(str(tmp_path), "dest")

    subfile = "test.sub"
    with open(os.path.join(job_dir, subfile), "w") as f:
        f.write("sub")
    with open(os.path.join(job_dir, "KPOINTS"), "w") as f:
        f.write("kpoints")
    with open(os.path.join(job_dir, "POTCAR"), "w") as f:
        f.write("potcar")
    # INCAR is missing

    with pytest.raises(FileNotFoundError):
        copy_inputs(subfile, job_dir, dest_dir)


def test_create_sc_contcar(tmp_path):
    sub_quene = []
    old_cwd = os.getcwd()
    job_path = os.path.join(tmp_path, "job")
    sc_job_path = os.path.join(tmp_path, "sc")
    shutil.copytree("test/test_files/h2_sc", job_path)
    os.remove(os.path.join(job_path, "POSCAR"))

    create_sc(job_path, False, 2, sub_quene, 0, False)
    new_cwd = os.getcwd()

    assert old_cwd == new_cwd
    assert os.path.isdir(sc_job_path)
    assert os.path.isfile(os.path.join(sc_job_path, "INCAR"))
    assert os.path.isfile(os.path.join(sc_job_path, "fri.sub"))
    assert os.path.isfile(os.path.join(sc_job_path, "KPOINTS"))
    assert os.path.isfile(os.path.join(sc_job_path, "POTCAR"))
    assert os.path.isfile(os.path.join(sc_job_path, "CONTCAR"))
    incar = open(os.path.join(sc_job_path, "INCAR"))
    incar_text = incar.read()
    assert "IBRION=-1" in incar_text
    assert "LCHARGE=.TRUE." in incar_text
    assert "NSW=0" in incar_text
    expected_sub_quene = [sc_job_path]
    assert sub_quene == expected_sub_quene
