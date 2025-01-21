import os
import json
import pytest
from unittest.mock import mock_open, patch
from dags.etl_dag import extract, transform, load, EXTRACTED_DIR

@pytest.fixture
def mock_jobs_csv():
    return """context
{"job_title": "Developer"}
{"job_title": "Analyst"}
"""

def test_extract(mock_jobs_csv, tmp_path, mocker):
    mocker.patch("pandas.read_csv", return_value=mocker.Mock(context=["job_0", "job_1"]))
    mocker.patch("os.makedirs", return_value=None)
    mocked_open = mocker.patch("builtins.open", mock_open())

    # Call the extract task
    extract()

    # Check files were written correctly
    assert mocked_open.call_count == 2
    mocked_open.assert_any_call(os.path.join(EXTRACTED_DIR, "job_0.txt"), "w")
    mocked_open.assert_any_call(os.path.join(EXTRACTED_DIR, "job_1.txt"), "w")

def test_transform(tmp_path, mocker):
    mock_extracted_dir = tmp_path / "extracted"
    mock_transformed_dir = tmp_path / "transformed"
    os.makedirs(mock_extracted_dir, exist_ok=True)
    os.makedirs(mock_transformed_dir, exist_ok=True)

    # Create mock extracted files
    extracted_file = mock_extracted_dir / "job_0.txt"
    with open(extracted_file, "w") as f:
        json.dump({"job_title": "Developer", "job_description": "Coding"}, f)

    mocker.patch("dags.etl_dag.EXTRACTED_DIR", mock_extracted_dir)
    mocker.patch("dags.etl_dag.TRANSFORMED_DIR", mock_transformed_dir)

    # Call transform task
    transform()

    # Assert the transformed file exists and has correct content
    transformed_file = mock_transformed_dir / "job_0.txt"
    assert transformed_file.exists()

    with open(transformed_file, "r") as f:
        data = json.load(f)
        assert data["job"]["title"] == "Developer"
        assert data["job"]["description"] == "Coding"

def test_load(mocker):
    mock_cursor = mocker.Mock()
    mock_conn = mocker.Mock(cursor=lambda: mock_cursor)
    mocker.patch("dags.etl_dag.SqliteHook.get_conn", return_value=mock_conn)

    # Create mock transformed files
    mock_data = {
        "job": {"title": "Developer", "industry": "Tech", "description": "Coding", "employment_type": "Full-time", "date_posted": "2024-01-01"},
        "company": {"name": "TechCorp", "link": "http://linkedin.com/TechCorp"},
        "education": {"required_credential": "Bachelor's"},
        "experience": {"months_of_experience": 24, "seniority_level": "Mid"},
        "salary": {"currency": "USD", "min_value": 50000, "max_value": 80000, "unit": "year"},
        "location": {"country": "USA", "locality": "New York", "region": "NY", "postal_code": "10001", "street_address": "123 Tech Lane", "latitude": 40.7128, "longitude": -74.0060},
    }
    mocker.patch("os.listdir", return_value=["job_0.txt"])
    mocker.patch("builtins.open", mock_open(read_data=json.dumps(mock_data)))

    # Call the load task
    load()

    # Verify database insertion
    assert mock_cursor.execute.call_count == 6
    mock_cursor.execute.assert_any_call(
        "INSERT INTO job (title, industry, description, employment_type, date_posted) VALUES (?, ?, ?, ?, ?)",
        ("Developer", "Tech", "Coding", "Full-time", "2024-01-01")
    )
