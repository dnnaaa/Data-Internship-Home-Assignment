import json
from unittest.mock import patch, mock_open
from pathlib import Path
import pytest
from dags.tasks.transform import transform, description_cleaning

# Sample input data for the transform task (mock data)
sample_input_data = {
    "title": "Software Engineer",
    "industry": "Tech",
    "description": "<p>Develop software applications.</p>",
    "employmentType": "Full-time",
    "datePosted": "2025-01-01",
    "hiringOrganization": {"name": "TechCompany", "sameAs": "https://www.linkedin.com/company/techcompany"},
    "educationRequirements": {"credentialCategory": "Bachelor's Degree"},
    "experienceRequirements": {"monthsOfExperience": 24, "seniorityLevel": "Mid"},
    "estimatedSalary": {"currency": "USD", "value": {"minValue": 60000, "maxValue": 80000, "unitText": "YEAR"}},
    "jobLocation": {
        "address": {
            "addressCountry": "USA",
            "addressLocality": "San Francisco",
            "addressRegion": "CA",
            "postalCode": "94105",
            "streetAddress": "123 Tech St",
            "latitude": "37.7749",
            "longitude": "-122.4194"
        }
    }
}

# Mocked file paths
input_dir = "staging/extracted"
output_dir = "staging/transformed"

# Unit test for description_cleaning function (tests HTML tag removal from descriptions)
def test_description_cleaning():
    raw_description = "<p>Develop software applications.</p>"
    expected_clean_description = "Develop software applications."
    
    # Verify that HTML tags are removed correctly
    assert description_cleaning(raw_description) == expected_clean_description

# Test for the transform task that processes input data and saves output in JSON format
@patch("builtins.open", new_callable=mock_open)
@patch("pathlib.Path.glob", return_value=["mocked_file.txt"])  # Simulating the file fetch
@patch("os.path.join", side_effect=lambda *args: "/".join(args))  # Mock the path joining
@patch("dags.tasks.transform.Path.mkdir")  # Mock directory creation for output
def test_transform(mock_mkdir, mock_path_join, mock_glob, mock_open_file):

    # Simulate reading the input file and returning JSON data (mocking file read)
    mock_open_file.return_value = mock_open(read_data=json.dumps(sample_input_data)).return_value

    # Call the transform function, which is responsible for reading, transforming, and saving data
    transform(input_dir=input_dir, output_dir=output_dir)

    # Expected output after transformation (data structure in JSON)
    expected_output = {
        "job": {
            "title": "Software Engineer",
            "industry": "Tech",
            "description": "Develop software applications.",
            "employment_type": "Full-time",
            "date_posted": "2025-01-01",
        },
        "company": {
            "name": "TechCompany",
            "link": "https://www.linkedin.com/company/techcompany",
        },
        "education": {
            "required_credential": "Bachelor's Degree",
        },
        "experience": {
            "months_of_experience": 24,
            "seniority_level": "Mid",
        },
        "salary": {
            "currency": "USD",
            "min_value": 60000,
            "max_value": 80000,
            "unit": "YEAR",
        },
        "location": {
            "country": "USA",
            "locality": "San Francisco",
            "region": "CA",
            "postal_code": "94105",
            "street_address": "123 Tech St",
            "latitude": "37.7749",
            "longitude": "-122.4194",
        }
    }

    # Verify that the output file was written correctly
    mock_open_file.assert_called_once_with(f'{output_dir}/mocked_file.json', 'w', encoding='utf-8')
    
    # Capture the written content to compare with the expected output
    written_content = "".join(call.args[0] for call in mock_open_file().write.call_args_list)

    # Check that the written content matches the expected output
    assert json.loads(written_content) == expected_output, f"Expected: {expected_output}, but got: {json.loads(written_content)}"

    # Ensure the directory creation was triggered
    mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)

    # Verify the glob method was called to list input files (fetching '.txt' files)
    mock_glob.assert_called_once_with("*.txt")

    # Ensure that the path joining operation for output files works as expected
    mock_path_join.assert_called_once_with(output_dir, "mocked_file.json")

    print("Test for transform task passed successfully.")
