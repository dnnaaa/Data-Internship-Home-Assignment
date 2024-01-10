import os
import json
import sqlite3
import pytest
from dags.load import load


def test_load(tmpdir):
    # Crée des fichiers JSON de test
    transformed_dir = tmpdir.mkdir('transformed')
    
    
    job_data_1 = {
        "job": {
            "title": "Job 1",
            "industry": "Tech",
            "description": "Description 1",
            "employment_type": "Full-time",
            "date_posted": "2022-01-01"
        },
        "company": {
            "name": "Company 1",
            "link": "https://www.company1.com"
        },
        "education": {
            "required_credential": "Bachelor's Degree"
        },
        "experience": {
            "months_of_experience": "24",
            "seniority_level": "Intermediate"
        },
        "salary": {
            "currency": "USD",
            "min_value": 50000,
            "max_value": 70000,
            "unit": "year"
        },
        "location": {
            "country": "USA",
            "locality": "New York",
            "region": "NY",
            "postal_code": "10001",
            "street_address": "123 Main St",
            "latitude": 40.7128,
            "longitude": -74.0060
        }
    }
    
    job_data_2 = {
        "job": {
            "title": "Job 2",
            "industry": "Finance",
            "description": "Description 2",
            "employment_type": "Part-time",
            "date_posted": "2022-02-01"
        },
        "company": {
            "name": "Company 2",
            "link": "https://www.company2.com"
        },
        "education": {
            "required_credential": "Master's Degree"
        },
        "experience": {
            "months_of_experience": "36",
            "seniority_level": "Senior"
        },
        "salary": {
            "currency": "USD",
            "min_value": 80000,
            "max_value": 100000,
            "unit": "year"
        },
        "location": {
            "country": "USA",
            "locality": "San Francisco",
            "region": "CA",
            "postal_code": "94101",
            "street_address": "456 Elm St",
            "latitude": 37.7749,
            "longitude": -122.4194
        }
    }
    
    transformed_dir.join('0.json').write(json.dumps(job_data_1))
    transformed_dir.join('1.json').write(json.dumps(job_data_2))

    load()

    conn = sqlite3.connect('/tmp/sqlite_default.db')
    cursor = conn.cursor()


    cursor.execute("SELECT * FROM job")
    job_rows = cursor.fetchall()
    assert len(job_rows) == 2


    cursor.execute("SELECT * FROM company")
    company_rows = cursor.fetchall()
    assert len(company_rows) == 2

 
    cursor.execute("SELECT * FROM education")
    education_rows = cursor.fetchall()
    assert len(education_rows) == 2

    cursor.execute("SELECT * FROM experience")
    experience_rows = cursor.fetchall()
    assert len(experience_rows) == 2

    cursor.execute("SELECT * FROM salary")
    salary_rows = cursor.fetchall()
    assert len(salary_rows) == 2

    cursor.execute("SELECT * FROM location")
    location_rows = cursor.fetchall()
    assert len(location_rows) == 2

    conn.close()