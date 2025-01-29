import pytest
import pandas as pd
import json
from pathlib import Path

from dags.utils.data_extractor import extract_from_csv

@pytest.fixture
def setup_test_environment(tmp_path, monkeypatch):
    # Set the current working directory to the temporary path
    monkeypatch.chdir(tmp_path)
    # Create the source directory
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    return source_dir


def test_output_directory_created(setup_test_environment, tmp_path):
    # Create a minimal CSV file
    csv_file = setup_test_environment / "jobs.csv"
    csv_file.write_text('0,"{}"\n')

    # Call the function with temporary paths
    extract_from_csv(input_file=str(csv_file), output_dir=str(tmp_path / "staging" / "extracted"))  

    # Check if the output directory was created
    output_dir = tmp_path / "staging" / "extracted"
    assert output_dir.exists()


def test_valid_and_invalid_rows(setup_test_environment, tmp_path):
    # CSV content with valid and invalid entries
    csv_content = [
        '0,"{""valid"": ""json""}"',   # Valid row
        ',"{""invalid_id"": ""data""}"',  # Missing id (invalid)
        '1,',  # Missing job_data (invalid)
        '2,"invalid json"',  # Invalid JSON
        '3,"{""another"": ""valid""}"'  # Valid row
    ]
    csv_file = setup_test_environment / "jobs.csv"
    csv_file.write_text("\n".join(csv_content))

    # Call the function with temporary paths
    extract_from_csv(input_file=str(csv_file), output_dir=str(tmp_path / "staging" / "extracted"))

    # Read the output file
    output_file = tmp_path / "staging" / "extracted" / "pure_jobs.json.txt"
    assert output_file.exists()

    with open(output_file, "r") as f:
        lines = f.readlines()

    # Should have 2 valid JSON entries
    assert len(lines) == 2
    assert json.loads(lines[0].strip()) == {"valid": "json"}
    assert json.loads(lines[1].strip()) == {"another": "valid"}


# def test_id_column_int64_type(setup_test_environment):
#     # Create CSV with a non-integer ID (should be filtered out)
#     csv_content = [
#         'abc,"{""valid"": ""json""}"',  # Invalid id (string)
#         '4,"{""data"": ""value""}"'     # Valid id
#     ]
#     csv_file = setup_test_environment / "jobs.csv"
#     csv_file.write_text("\n".join(csv_content))

#     extract_from_csv()

#     # Check if invalid id (abc) was filtered out
#     df = pd.read_csv(
#         csv_file,
#         header=None,
#         names=['id', 'job_data'],
#         dtype={'id': 'Int64'}
#     )
#     # Check dtype of 'id' column
#     assert pd.api.types.is_integer_dtype(df['id'])
#     # Check that 'abc' becomes NaN and is filtered out
#     valid_jobs = df.dropna(subset=['id', 'job_data'])
#     assert len(valid_jobs) == 1  # Only the row with id=4 remains


def test_empty_csv(setup_test_environment, tmp_path):
    # Create an empty CSV
    csv_file = setup_test_environment / "jobs.csv"
    csv_file.write_text("")

    # Call the function with temporary paths
    extract_from_csv(input_file=str(csv_file), output_dir=str(tmp_path / "staging" / "extracted"))

    output_file = tmp_path / "staging" / "extracted" / "pure_jobs.json.txt"
    assert output_file.exists()
    
    # Output file should be empty
    with open(output_file, "r") as f:
        assert len(f.readlines()) == 0


def test_all_invalid_json(setup_test_environment, tmp_path):
    # CSV with valid ids/job_data but invalid JSON
    csv_content = [
        '1,"invalid json"',
        '2,"another invalid"'
    ]
    csv_file = setup_test_environment / "jobs.csv"
    csv_file.write_text("\n".join(csv_content))

    # Call the function with temporary paths
    extract_from_csv(input_file=str(csv_file), output_dir=str(tmp_path / "staging" / "extracted"))

    output_file = tmp_path / "staging" / "extracted" / "pure_jobs.json.txt"
    with open(output_file, "r") as f:
        assert len(f.readlines()) == 0  # No valid JSON written