# tests/conftest.py
import pytest
import json
import os
from unittest.mock import MagicMock

@pytest.fixture
def sample_job_data():
    return {
        "title": "Software Engineer",
        "industry": "Technology",
        "description": "Python developer position",
        "employmentType": "FULL_TIME",
        "datePosted": "2024-01-20",
        "hiringOrganization": {
            "name": "TechCorp",
            "sameAs": "https://linkedin.com/company/techcorp"
        },
        "educationRequirements": {
            "credentialCategory": "Bachelor's Degree"
        },
        "experienceRequirements": {
            "monthsOfExperience": 36,
            "seniority": "MID_LEVEL"
        },
        "baseSalary": {
            "currency": "USD",
            "value": {
                "minValue": 80000,
                "maxValue": 120000
            },
            "unit": "YEAR"
        },
        "jobLocation": {
            "address": {
                "addressCountry": "USA",
                "addressLocality": "San Francisco",
                "addressRegion": "CA",
                "postalCode": "94105",
                "streetAddress": "123 Tech St"
            },
            "geo": {
                "latitude": 37.7749,
                "longitude": -122.4194
            }
        }
    }

@pytest.fixture
def mock_sqlite_hook():
    mock = MagicMock()
    mock.run.return_value = 1  # Simulating successful insertion with job_id=1
    return mock

@pytest.fixture
def setup_staging_directories():
    """Create temporary staging directories for tests"""
    os.makedirs("staging/extracted", exist_ok=True)
    os.makedirs("staging/transformed", exist_ok=True)
    yield
    # Cleanup after tests
    if os.path.exists("staging"):
        import shutil
        shutil.rmtree("staging")
