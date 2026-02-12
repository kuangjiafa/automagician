import os
import sys
import pytest

sys.path.append(os.getcwd())
import old_automagician

@pytest.fixture
def restore_cwd():
    original_cwd = os.getcwd()
    yield
    os.chdir(original_cwd)

def test_get_error_message_with_errors(tmp_path, restore_cwd):
    job_dir = tmp_path / "job_with_errors"
    job_dir.mkdir()
    ll_out = job_dir / "ll_out"
    ll_out.write_text("Some normal output\nERROR: Something went wrong\nMore output\nAnother error occurred\n")

    messages = old_automagician.get_error_message(str(job_dir))
    assert len(messages) == 2
    assert "ERROR: Something went wrong\n" in messages
    assert "Another error occurred\n" in messages

def test_get_error_message_no_errors(tmp_path, restore_cwd):
    job_dir = tmp_path / "job_no_errors"
    job_dir.mkdir()
    ll_out = job_dir / "ll_out"
    ll_out.write_text("All good here\nNo issues found\n")

    messages = old_automagician.get_error_message(str(job_dir))
    # Expect empty list after fix
    assert messages == []

def test_log_error_with_errors(tmp_path, restore_cwd):
    home_dir = tmp_path
    job_dir = tmp_path / "job_log_errors"
    job_dir.mkdir()
    ll_out = job_dir / "ll_out"
    ll_out.write_text("ERROR: Test error\n")

    # We need to ensure log_error writes correctly.
    # The timestamp makes exact comparison tricky, so we check for presence.

    old_automagician.log_error(str(job_dir), str(home_dir))

    error_log = home_dir / "error_log.dat"
    assert error_log.exists()
    content = error_log.read_text()
    assert str(job_dir) in content
    assert "ERROR: Test error" in content

def test_log_error_no_errors(tmp_path, restore_cwd):
    home_dir = tmp_path
    job_dir = tmp_path / "job_log_no_errors"
    job_dir.mkdir()
    ll_out = job_dir / "ll_out"
    ll_out.write_text("No issues\n")

    old_automagician.log_error(str(job_dir), str(home_dir))

    error_log = home_dir / "error_log.dat"
    # File is created because of open(..., "a+")
    assert error_log.exists()
    content = error_log.read_text()
    # Should be empty if get_error_message returns empty list
    assert content == ""
