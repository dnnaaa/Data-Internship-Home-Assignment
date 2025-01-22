import json
import pytest
from unittest.mock import patch, mock_open
from dags.tasks.load import load  # Assuming the load function is in the `dags.tasks.load` module

# Test the 'load' function to verify the reading of JSON data and correct saving of the processed file
@patch("builtins.open", new_callable=mock_open)
@patch("os.path.join", side_effect=lambda *args: "/".join(args))
def test_load(mock_path_join, mock_file, mock_load_data):
    
    # Simulated JSON data that will be "loaded" from the input file
    loaded_data = {
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
    
    # Setup the mock to simulate reading the file and returning the expected JSON data
    mock_file.return_value.read.return_value = json.dumps(loaded_data)  # Mock the file read operation
    
    # Define the paths for the input JSON file and output directory
    json_file_path = "/opt/airflow/output/Software Engineer.json"
    output_dir = "/opt/airflow/processed"

    # Call the load function, which should read the file and process the data
    load(json_file_path, output_dir)

    # Verify that the JSON file was opened for reading with the correct file path
    mock_file.assert_called_once_with(json_file_path, 'r')  # Ensures that the file was opened for reading

    # Verify that the output file path was constructed correctly using the provided output directory
    mock_path_join.assert_called_once_with(output_dir, "Software Engineer_processed.json")

    # Ensure that the processed data is written to the expected output file
    mock_file().write.assert_called_once_with(json.dumps(loaded_data))  # Check that the data was written correctly

    print("Test passed: Data loaded and written correctly.")
