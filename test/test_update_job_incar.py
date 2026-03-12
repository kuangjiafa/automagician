import os
from automagician.update_job import set_incar_tags

def test_set_incar_tags_update_existing(tmp_path):
    """Test updating an existing tag."""
    incar_path = tmp_path / "INCAR"
    incar_path.write_text("ENCUT=500\nISMEAR=0\n")

    tags_dict = {"ENCUT": "600"}
    set_incar_tags(str(incar_path), tags_dict)

    content = incar_path.read_text()
    assert "ENCUT=600" in content
    assert "ISMEAR=0" in content
    assert content.count("ENCUT=") == 1

def test_set_incar_tags_add_new(tmp_path):
    """Test adding a new tag."""
    incar_path = tmp_path / "INCAR"
    incar_path.write_text("ENCUT=500\n")

    tags_dict = {"PREC": "Accurate"}
    set_incar_tags(str(incar_path), tags_dict)

    content = incar_path.read_text()
    assert "ENCUT=500" in content
    assert "PREC=Accurate" in content

def test_set_incar_tags_preserve_existing(tmp_path):
    """Test preserving existing tags not in tags_dict."""
    incar_path = tmp_path / "INCAR"
    incar_path.write_text("ENCUT=500\nISMEAR=0\n")

    tags_dict = {}
    set_incar_tags(str(incar_path), tags_dict)

    content = incar_path.read_text()
    assert "ENCUT=500" in content
    assert "ISMEAR=0" in content

def test_set_incar_tags_mixed(tmp_path):
    """Test updating one tag and adding another."""
    incar_path = tmp_path / "INCAR"
    incar_path.write_text("ENCUT=500\nISMEAR=0\n")

    tags_dict = {"ENCUT": "600", "PREC": "Accurate"}
    set_incar_tags(str(incar_path), tags_dict)

    content = incar_path.read_text()
    assert "ENCUT=600" in content
    assert "ISMEAR=0" in content
    assert "PREC=Accurate" in content

def test_set_incar_tags_empty_file(tmp_path):
    """Test updating an empty file."""
    incar_path = tmp_path / "INCAR"
    incar_path.touch()

    tags_dict = {"ENCUT": "600"}
    set_incar_tags(str(incar_path), tags_dict)

    content = incar_path.read_text()
    assert "ENCUT=600" in content

def test_set_incar_tags_empty_dict(tmp_path):
    """Test passing an empty dictionary."""
    incar_path = tmp_path / "INCAR"
    original_content = "ENCUT=500\nISMEAR=0\n"
    incar_path.write_text(original_content)

    tags_dict = {}
    set_incar_tags(str(incar_path), tags_dict)

    assert incar_path.read_text() == original_content

def test_set_incar_tags_whitespace_handling(tmp_path):
    """Test handling of whitespace around '='."""
    incar_path = tmp_path / "INCAR"
    incar_path.write_text("ENCUT = 500\n")

    tags_dict = {"ENCUT": "600"}
    set_incar_tags(str(incar_path), tags_dict)

    content = incar_path.read_text()
    # Expect "ENCUT = 500" to be replaced by "ENCUT=600"
    assert "ENCUT=600" in content
    assert "ENCUT = 500" not in content
    assert content.count("ENCUT") == 1

def test_set_incar_tags_comments(tmp_path):
    """Test handling of comments."""
    incar_path = tmp_path / "INCAR"
    incar_path.write_text("ENCUT=500 # This is a comment\n")

    tags_dict = {"ENCUT": "600"}
    set_incar_tags(str(incar_path), tags_dict)

    content = incar_path.read_text()
    # The comment is lost because the whole line is replaced.
    assert "ENCUT=600" in content
    assert "# This is a comment" not in content
