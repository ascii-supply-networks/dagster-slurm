"""Tests for runners."""

from dagster_slurm.runners import LocalRunner


def test_local_runner_execute_script(temp_dir):
    """Test local runner script execution."""
    runner = LocalRunner()

    script_lines = [
        "#!/bin/bash",
        "echo 'Hello from test'",
        "echo 'Test output' > output.txt",
    ]

    working_dir = str(temp_dir)
    job_id = runner.execute_script(
        script_lines=script_lines,
        working_dir=working_dir,
        wait=True,
    )

    assert job_id > 0

    # Check output file was created
    output_file = temp_dir / "output.txt"
    assert output_file.exists()
    assert output_file.read_text().strip() == "Test output"


def test_local_runner_upload_file(temp_dir):
    """Test local runner file upload."""
    runner = LocalRunner()

    # Create source file
    source = temp_dir / "source.txt"
    source.write_text("test content")

    # Upload to destination
    dest = temp_dir / "subdir" / "dest.txt"
    runner.upload_file(str(source), str(dest))

    assert dest.exists()
    assert dest.read_text() == "test content"


def test_local_runner_create_directory(temp_dir):
    """Test local runner directory creation."""
    runner = LocalRunner()

    new_dir = temp_dir / "nested" / "directory"
    runner.create_directory(str(new_dir))

    assert new_dir.exists()
    assert new_dir.is_dir()
