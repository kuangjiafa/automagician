from automagician.update_job import set_incar_tags

def test_set_incar_tags_missing_equals(tmp_path):
    """Test handling of lines without '='."""
    incar_path = tmp_path / "INCAR"
    original_content = "ENCUT=500\nThis is a header\n\n# Another comment\n"
    incar_path.write_text(original_content)

    tags_dict = {"ENCUT": "600", "ISMEAR": "0"}
    set_incar_tags(str(incar_path), tags_dict)

    content = incar_path.read_text()
    assert "ENCUT=600" in content
    assert "ISMEAR=0" in content
    assert "This is a header" in content
    assert "# Another comment" in content
    # The empty line should also be preserved
    assert "\n\n" in content

def test_set_incar_tags_multiple_equals(tmp_path):
    """Test handling of lines with multiple '='."""
    incar_path = tmp_path / "INCAR"
    # A contrived example, but valid syntax-wise for our simple parser.
    # It should split on the first '=' and use the rest as value (or just update based on the tag).
    incar_path.write_text("MAGMOM=1 2 = 3\n")

    tags_dict = {"MAGMOM": "1 2 3"}
    set_incar_tags(str(incar_path), tags_dict)

    content = incar_path.read_text()
    assert "MAGMOM=1 2 3" in content
    assert "MAGMOM=1 2 = 3" not in content

def test_set_incar_tags_none_value(tmp_path):
    """Test handling of None values in tags_dict."""
    incar_path = tmp_path / "INCAR"
    incar_path.write_text("ENCUT=500\n")

    tags_dict = {"ENCUT": None, "PREC": "Accurate"}
    set_incar_tags(str(incar_path), tags_dict)

    content = incar_path.read_text()
    # The original value should be preserved when None is passed
    assert "ENCUT=500" in content
    assert "PREC=Accurate" in content
