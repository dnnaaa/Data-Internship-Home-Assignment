import os
import sqlite3
import json
import pytest
from etl_load import load

def test_load():
    # Creating a dummy JSON file for testing
    dummy_data = {
        "job": {
            "title": "Test Job Title",
            "industry": "Test Industry",
            "description": "Test Description",
            "employment_type": "Test Employment Type",
            "date_posted": "2023-01-01"
        },
        "company": {
            "name": "Test Company",
            "link": "Test LinkedIn Link"
        },
        "education": {
            "required_credential": "Test Credential"
        },
        "experience": {
            "months_of_experience": 24,
            "seniority_level": "Test Seniority"
        },
        "salary": {
            "currency": "USD",
            "min_value": 50000,
            "max_value": 70000,
            "unit": "yearly"
        },
        "location": {
            "country": "Test Country",
            "locality": "Test Locality",
            "region": "Test Region",
            "postal_code": "12345",
            "street_address": "Test Street",
            "latitude": 40.7128,
            "longitude": -74.0060
        }
    }
    
    
    os.makedirs('staging/transformed', exist_ok=True)

    with open('staging/transformed/test_data.json', 'w') as file:
        json.dump(dummy_data, file)

    load()

    assert os.path.exists('your_database_file.db')

    conn = sqlite3.connect('your_database_file.db')
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM job")
    result = cursor.fetchone()

    conn.close()

    assert result[0] == 1  
