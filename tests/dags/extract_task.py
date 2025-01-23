import pytest
from unittest.mock import mock_open, patch
from tasks.extract import extract_task

def test_extract_task(tmpdir):
    # Mock data
    csv_content = "context\n{\"title\": \"Software Engineer\"}\n{\"title\": \"Data Scientist\"}"
    input_path = "/fake/path/jobs.csv"
    output_dir = str(tmpdir)  # Use pytest's tmpdir fixture for temporary files

    # Mock file operations
    with patch("builtins.open", mock_open(read_data=csv_content)) as mock_file:
        extract_task()

        # Verify the mock was called with the correct file paths
        mock_file.assert_any_call(input_path, "r")
        mock_file.assert_any_call(f"{output_dir}/context_0.txt", "w")
        mock_file.assert_any_call(f"{output_dir}/context_1.txt", "w")

        # Verify the content written to the output files
        mock_file().write.assert_any_call("{\"title\": \"Software Engineer\"}")
        mock_file().write.assert_any_call("{\"title\": \"Data Scientist\"}")