import pytest
import json
import os
from pathlib import Path
from dags.utils.data_extractor import extract_from_csv
from dags.utils.data_cleaner import clean_description
from dags.utils.data_transformer import transform_job, transform_and_save_job

# Fixture for creating temporary files and directories
@pytest.fixture
def setup_test_environment(tmp_path):
    # Create a temporary input file
    input_file = tmp_path / "pure_jobs.json.txt"
    input_file.write_text("")
    return input_file, tmp_path

# Test cases for transform_job
def test_transform_job_basic():
    job_data = {
        "title": "Software Engineer",
        "industry": "Information Technology",
        "description": "<p>This is a <b>test</b> description.</p>",
        "employmentType": "FULL_TIME",
        "datePosted": "2023-10-01",
        "hiringOrganization": {
            "name": "Tech Corp",
            "sameAs": "https://techcorp.com"
        },
        "educationRequirements": {
            "credentialCategory": "bachelor degree"
        },
        "experienceRequirements": {
            "monthsOfExperience": 60
        },
        "estimatedSalary": {
            "currency": "USD",
            "value": {
                "minValue": 100000,
                "maxValue": 150000,
                "unitText": "YEAR"
            }
        },
        "jobLocation": {
            "address": {
                "addressCountry": "US",
                "addressLocality": "New York",
                "addressRegion": "NY",
                "postalCode": "10001",
                "streetAddress": "123 Main St"
            },
            "latitude": 40.7128,
            "longitude": -74.0060
        }
    }

    expected_output = {
        "job": {
            "title": "Software Engineer",
            "industry": "Information Technology",
            "description": "This is a test description.",
            "employment_type": "FULL_TIME",
            "date_posted": "2023-10-01",
        },
        "company": {
            "name": "Tech Corp",
            "link": "https://techcorp.com"
        },
        "education": {
            "required_credential": "bachelor degree"
        },
        "experience": {
            "months_of_experience": 60,
            "seniority_level": None
        },
        "salary": {
            "currency": "USD",
            "min_value": 100000,
            "max_value": 150000,
            "unit": "YEAR"
        },
        "location": {
            "country": "US",
            "locality": "New York",
            "region": "NY",
            "postal_code": "10001",
            "street_address": "123 Main St",
            "latitude": 40.7128,
            "longitude": -74.0060
        }
    }

    assert transform_job(job_data) == expected_output

def test_transform_job_missing_fields():
    job_data = {
        "title": "Software Engineer",
        "description": "<p>This is a <b>test</b> description.</p>",
    }

    expected_output = {
        "job": {
            "title": "Software Engineer",
            "industry": "",
            "description": "This is a test description.",
            "employment_type": "",
            "date_posted": "",
        }
    }

    assert transform_job(job_data) == expected_output



# Test cases for transform_and_save_job
def test_transform_and_save_job(setup_test_environment):
    input_file, tmp_path = setup_test_environment
    output_dir = tmp_path / "transformed"
    output_file = output_dir / "all_jobs.json"

    # Write valid JSON lines to the input file
    job1 = {
        "title": "Software Engineer",
        "description": "<p>This is a <b>test</b> description.</p>",
    }
    job2 = {
        "title": "Data Scientist",
        "description": "<p>Another <b>test</b> description.</p>",
    }
    input_file.write_text(json.dumps(job1) + "\n" + json.dumps(job2))

    # Call the function
    transform_and_save_job(input_file=str(input_file), output_dir=str(output_dir))

    # Verify the output file
    assert output_file.exists()
    with open(output_file, "r") as f:
        transformed_jobs = json.load(f)
        assert len(transformed_jobs) == 2
        assert transformed_jobs[0]["job"]["title"] == "Software Engineer"
        assert transformed_jobs[1]["job"]["title"] == "Data Scientist"

def test_transform_and_save_job_invalid_json(setup_test_environment):
    input_file, tmp_path = setup_test_environment
    output_dir = tmp_path / "transformed"
    output_file = output_dir / "all_jobs.json"

    # Write invalid JSON lines to the input file
    input_file.write_text("invalid json\n{\"title\": \"Valid Job\"}")

    # Call the function
    transform_and_save_job(input_file=str(input_file), output_dir=str(output_dir))

    # Verify the output file
    assert output_file.exists()
    with open(output_file, "r") as f:
        transformed_jobs = json.load(f)
        assert len(transformed_jobs) == 1
        assert transformed_jobs[0]["job"]["title"] == "Valid Job"

def test_transform_and_save_job_empty_file(setup_test_environment):
    input_file, tmp_path = setup_test_environment
    output_dir = tmp_path / "transformed"
    output_file = output_dir / "all_jobs.json"

    # Call the function with an empty file
    transform_and_save_job(input_file=str(input_file), output_dir=str(output_dir))

    # Verify the output file
    assert output_file.exists()
    with open(output_file, "r") as f:
        transformed_jobs = json.load(f)
        assert len(transformed_jobs) == 0