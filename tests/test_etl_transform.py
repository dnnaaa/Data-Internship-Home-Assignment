import os
import json
import pytest
from etl_transform import transform

def test_transform():
    # Creating a dummy data file for testing
    with open('staging/extracted/test_data.txt', 'w') as file:
        json.dump({
            "title": "Test Job Title",
            "industry": "Test Industry",
            "description": "Test Description",
            "employmentType": "Test Employment Type",
            "datePosted": "2023-01-01",
            "hiringOrganization": {
                "name": "Test Company",
                "sameAs": "Test LinkedIn Link"
            },
            "educationRequirements": {
                "credentialCategory": "Test Credential"
            },
            "experienceRequirements": {
                "monthsOfExperience": 24
            },
            "salary": {
                "currency": "USD",
                "min_value": 50000,
                "max_value": 70000,
                "unit": "yearly"
            },
            "jobLocation": {
                "address": {
                    "addressCountry": "Test Country",
                    "addressLocality": "Test Locality",
                    "addressRegion": "Test Region",
                    "postalCode": "12345",
                    "streetAddress": "Test Street",
                    "latitude": 40.7128,
                    "longitude": -74.0060
                }
            }
        }, file)

    transform()

    assert os.path.exists('staging/transformed')

    assert len(os.listdir('staging/transformed')) > 0
