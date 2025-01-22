import pytest
import sqlite3
from unittest.mock import patch, mock_open
import json
import pandas as pd

# Fixture to give mock CSV data, simulating job listings li kaynin f DataFrame
@pytest.fixture
def mock_csv_data():
    return pd.DataFrame({
        'context': [
            '{"title": "Software Engineer", "industry": "Tech", "description": "Develop software.", '
            '"employmentType": "Full-time", "datePosted": "2025-01-01", '
            '"hiringOrganization": {"name": "TechCompany", "sameAs": "https://www.linkedin.com/company/techcompany"}}'
        ]
    })

# Fixture that mocks reading a JSON file, representing a sample job posting
@pytest.fixture
def mock_json_open():
    return mock_open(read_data=json.dumps({
        "job": {
            "title": "Software Engineer",
            "industry": "Technology",
            "description": "Develop software",
            "employment_type": "Full-time",
            "date_posted": "2025-01-22"
        },
        "company": {
            "name": "Tech Corp",
            "link": "https://techcorp.com"
        },
        "education": {
            "required_credential": "Bachelor's Degree"
        },
        "experience": {
            "months_of_experience": "24",
            "seniority_level": "Mid-Level"
        },
        "salary": {
            "currency": "USD",
            "min_value": "60000",
            "max_value": "80000",
            "unit": "year"
        },
        "location": {
            "country": "USA",
            "locality": "San Francisco",
            "region": "CA",
            "postal_code": "94105",
            "street_address": "123 Tech St",
            "latitude": "37.7749",
            "longitude": "-122.4194"
        }
    }))

# Fixture to mock an SQLite connection, simulating database interactions
@pytest.fixture
def mock_sqlite_connection():
    mock_conn = patch("sqlite3.connect")
    mock_conn.return_value.cursor.return_value = mock_conn
    return mock_conn

# Fixture to mock the behavior of OS functions, like checking file existence and listing directory contents
@pytest.fixture
def mock_os_functions():
    with patch("os.path.isfile", return_value=True), \
         patch("os.listdir", return_value=["job1.json"]):
        yield

