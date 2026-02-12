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
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    dest_dir = tmp_path / "dest"

    subfile = "test.sub"
    (job_dir / subfile).write_text("sub")
    (job_dir / "KPOINTS").write_text("kpoints")
    (job_dir / "POTCAR").write_text("potcar")
    (job_dir / "INCAR").write_text("incar")
    (job_dir / "CHGCAR").write_text("chgcar")
    (job_dir / "CONTCAR").write_text("contcar")
    (job_dir / "POSCAR").write_text("poscar")

    copy_inputs(subfile, str(job_dir), str(dest_dir))

    assert (dest_dir / subfile).exists()
    assert (dest_dir / "KPOINTS").exists()
    assert (dest_dir / "POTCAR").exists()
    assert (dest_dir / "INCAR").exists()
    assert (dest_dir / "CHGCAR").exists()
    assert (dest_dir / "CONTCAR").exists()
    # If CONTCAR exists, POSCAR should NOT be copied
    assert not (dest_dir / "POSCAR").exists()


def test_copy_inputs_minimal(tmp_path):
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    dest_dir = tmp_path / "dest"

    subfile = "test.sub"
    (job_dir / subfile).write_text("sub")
    (job_dir / "KPOINTS").write_text("kpoints")
    (job_dir / "POTCAR").write_text("potcar")
    (job_dir / "INCAR").write_text("incar")
    (job_dir / "POSCAR").write_text("poscar")

    copy_inputs(subfile, str(job_dir), str(dest_dir))

    assert (dest_dir / subfile).exists()
    assert (dest_dir / "KPOINTS").exists()
    assert (dest_dir / "POTCAR").exists()
    assert (dest_dir / "INCAR").exists()
    assert (dest_dir / "POSCAR").exists()
    assert not (dest_dir / "CHGCAR").exists()
    assert not (dest_dir / "CONTCAR").exists()


def test_copy_inputs_missing_mandatory(tmp_path):
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    dest_dir = tmp_path / "dest"

    subfile = "test.sub"
    (job_dir / subfile).write_text("sub")
    (job_dir / "KPOINTS").write_text("kpoints")
    (job_dir / "POTCAR").write_text("potcar")
    # INCAR is missing

    with pytest.raises(FileNotFoundError):
        copy_inputs(subfile, str(job_dir), str(dest_dir))


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
