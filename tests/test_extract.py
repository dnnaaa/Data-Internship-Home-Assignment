import json
import pytest
from unittest.mock import patch, mock_open
from dags.tasks.extract import extract

# Test the 'extract' function to verify CSV reading and JSON writing functionality
@patch("dags.tasks.extract.pd.read_csv")
@patch("builtins.open", new_callable=mock_open)
@patch("os.path.join", side_effect=lambda *args: "/".join(args)) 
def test_extract(mock_path_join, mock_file, mock_read_csv, mock_csv_data):

    # Mock the return value of pd.read_csv to simulate reading the CSV data
    mock_read_csv.return_value = mock_csv_data
    
    # Define the paths for the CSV input and output directory
    csv_file_path = "/opt/airflow/source/jobs.csv"
    output_dir = "/opt/airflow/output"

    # Call the extract function to test its behavior
    extract(csv_file_path, output_dir)

    # Ensure the CSV file was read exactly once with the correct file path
    mock_read_csv.assert_called_once_with(csv_file_path)

    # Check that the mock file was opened for writing with the expected file path
    mock_file.assert_called_once_with('/opt/airflow/output/Software Engineer.json', 'w')

    # Define the expected structure of the output JSON content
    expected_output = {
        "title": "Software Engineer",
        "industry": "Tech",
        "description": "Develop software.",
        "employmentType": "Full-time",
        "datePosted": "2025-01-01",
        "hiringOrganization": {
            "name": "TechCompany",
            "sameAs": "https://www.linkedin.com/company/techcompany"
        }
    }

    # Retrieve the content that was "written" to the file through mock_open
    written_content = "".join(call.args[0] for call in mock_file().write.call_args_list)
    
    # Assert that the written content matches the expected JSON structure
    assert json.loads(written_content) == expected_output, "Written content does not match the expected content."

    print("Test passed: Data extracted and written correctly.")
