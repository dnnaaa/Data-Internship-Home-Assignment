from dags.utils.db_loader import safe_extract, insert_related_data, load_jobs_to_database
import pytest
import sqlite3
import json
import os
from pathlib import Path

# Fixture for creating a temporary SQLite database
@pytest.fixture
def setup_database(tmp_path):
    db_path = tmp_path / "test_jobs.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create tables
    cursor.execute('''
        CREATE TABLE job (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            industry TEXT,
            description TEXT,
            employment_type TEXT,
            date_posted TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE company (
            job_id INTEGER,
            name TEXT,
            link TEXT,
            FOREIGN KEY (job_id) REFERENCES job (id)
        )
    ''')
    cursor.execute('''
        CREATE TABLE education (
            job_id INTEGER,
            required_credential TEXT,
            FOREIGN KEY (job_id) REFERENCES job (id)
        )
    ''')
    cursor.execute('''
        CREATE TABLE experience (
            job_id INTEGER,
            months_of_experience INTEGER,
            seniority_level TEXT,
            FOREIGN KEY (job_id) REFERENCES job (id)
        )
    ''')
    cursor.execute('''
        CREATE TABLE salary (
            job_id INTEGER,
            currency TEXT,
            min_value INTEGER,
            max_value INTEGER,
            unit TEXT,
            FOREIGN KEY (job_id) REFERENCES job (id)
        )
    ''')
    cursor.execute('''
        CREATE TABLE location (
            job_id INTEGER,
            country TEXT,
            locality TEXT,
            region TEXT,
            postal_code TEXT,
            street_address TEXT,
            latitude REAL,
            longitude REAL,
            FOREIGN KEY (job_id) REFERENCES job (id)
        )
    ''')

    conn.commit()
    yield db_path
    conn.close()

# Fixture for creating a temporary JSON file
@pytest.fixture
def setup_json_file(tmp_path):
    input_file = tmp_path / "all_jobs.json"
    return input_file

# Test cases for safe_extract
def test_safe_extract_existing_key():
    data = {"a": {"b": {"c": 123}}}
    assert safe_extract(data, ["a", "b", "c"]) == 123

def test_safe_extract_missing_key():
    data = {"a": {"b": {}}}
    assert safe_extract(data, ["a", "b", "c"]) is None

def test_safe_extract_custom_default():
    data = {"a": {"b": {}}}
    assert safe_extract(data, ["a", "b", "c"], default=0) == 0

# Test cases for insert_related_data
def test_insert_company_data(setup_database):
    db_path = setup_database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Insert a job to get a valid job_id
    cursor.execute('''
        INSERT INTO job (title, industry, description, employment_type, date_posted)
        VALUES (?, ?, ?, ?, ?)
    ''', ("Software Engineer", "IT", "Test description", "FULL_TIME", "2023-10-01"))
    job_id = cursor.lastrowid

    # Insert company data
    data = {"company": {"name": "Tech Corp", "link": "https://techcorp.com"}}
    insert_related_data(conn, job_id, data)

    # Verify the data was inserted
    cursor.execute('SELECT name, link FROM company WHERE job_id = ?', (job_id,))
    result = cursor.fetchone()
    assert result == ("Tech Corp", "https://techcorp.com")

    conn.close()

# Test cases for load_jobs_to_database
def test_load_valid_jobs(setup_database, setup_json_file):
    db_path = setup_database
    input_file = setup_json_file

    # Write valid job data to the JSON file
    jobs = [
        {
            "job": {
                "title": "Software Engineer",
                "industry": "IT",
                "description": "Test description",
                "employment_type": "FULL_TIME",
                "date_posted": "2023-10-01"
            },
            "company": {
                "name": "Tech Corp",
                "link": "https://techcorp.com"
            }
        }
    ]
    with open(input_file, "w") as f:
        json.dump(jobs, f)

    # Call the function with explicit paths
    load_jobs_to_database(input_file=str(input_file), db_path=str(db_path))

    # Verify the data was inserted
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('SELECT title, industry FROM job')
    job_result = cursor.fetchone()
    assert job_result == ("Software Engineer", "IT")

    cursor.execute('SELECT name, link FROM company')
    company_result = cursor.fetchone()
    assert company_result == ("Tech Corp", "https://techcorp.com")

    conn.close()

def test_load_invalid_jobs(setup_database, setup_json_file):
    db_path = setup_database
    input_file = setup_json_file

    # Write invalid job data to the JSON file
    jobs = [
        {
            "job": {
                "title": "Software Engineer",
                "industry": "IT",
                "description": "Test description",
                "employment_type": "FULL_TIME",
                "date_posted": "2023-10-01"
            },
            "company": {
                "name": "Tech Corp",
                "link": "https://techcorp.com"
            }
        },
        {
            "job": {
                "title": "Data Scientist",
                "industry": "IT",
                "description": "Test description",
                "employment_type": "FULL_TIME",
                "date_posted": "2023-10-01"
            },
            "company": {
                "name": None,  # Invalid data
                "link": None   # Invalid data
            }
        }
    ]
    with open(input_file, "w") as f:
        json.dump(jobs, f)

    # Call the function with explicit paths
    load_jobs_to_database(input_file=str(input_file), db_path=str(db_path))

    # Verify only valid data was inserted
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('SELECT COUNT(*) FROM job')
    job_count = cursor.fetchone()[0]
    assert job_count == 2  # Both jobs should be inserted

    cursor.execute('SELECT COUNT(*) FROM company WHERE name IS NOT NULL')
    company_count = cursor.fetchone()[0]
    assert company_count == 1  # Only the valid company data should be inserted

    conn.close()

def test_load_empty_file(setup_database, setup_json_file):
    db_path = setup_database
    input_file = setup_json_file

    # Write an empty JSON file
    with open(input_file, "w") as f:
        json.dump([], f)

    # Call the function with explicit paths
    load_jobs_to_database(input_file=str(input_file), db_path=str(db_path))

    # Verify no data was inserted
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('SELECT COUNT(*) FROM job')
    job_count = cursor.fetchone()[0]
    assert job_count == 0

    conn.close()