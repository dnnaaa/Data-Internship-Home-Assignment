import os
import sys
sys.path.append("/workspaces/Data-Internship-Home-Assignment")
import json
import sqlite3
import pytest
from etl_process_operations.operations import extract_data, transform_data, load_data

JOB_TABLE_CREATION = """CREATE TABLE IF NOT EXISTS job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(225),
    industry VARCHAR(225),
    description TEXT,
    employment_type VARCHAR(125),
    date_posted DATE
);
"""

COMPANY_TABLE_CREATION = """CREATE TABLE IF NOT EXISTS company (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    name VARCHAR(225),
    link TEXT,
    FOREIGN KEY (job_id) REFERENCES job(id)
);
"""

EDUCATION_TABLE_CREATION = """CREATE TABLE IF NOT EXISTS education (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    required_credential VARCHAR(225),
    FOREIGN KEY (job_id) REFERENCES job(id)
);
"""

EXPERIENCE_TABLE_CREATION = """CREATE TABLE IF NOT EXISTS experience (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    months_of_experience INTEGER,
    seniority_level VARCHAR(25),
    FOREIGN KEY (job_id) REFERENCES job(id)
);
"""

SALARY_TABLE_CREATION = """CREATE TABLE IF NOT EXISTS salary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    currency VARCHAR(3),
    min_value NUMERIC,
    max_value NUMERIC,
    unit VARCHAR(12),
    FOREIGN KEY (job_id) REFERENCES job(id)
);
"""
LOCATION_TABLE_CREATION = """CREATE TABLE IF NOT EXISTS location (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    country VARCHAR(60),
    locality VARCHAR(60),
    region VARCHAR(60),
    postal_code VARCHAR(25),
    street_address VARCHAR(225),
    latitude NUMERIC,
    longitude NUMERIC,
    FOREIGN KEY (job_id) REFERENCES job(id)
)
"""

tables_creation_queries = (JOB_TABLE_CREATION, COMPANY_TABLE_CREATION, EDUCATION_TABLE_CREATION, EXPERIENCE_TABLE_CREATION, SALARY_TABLE_CREATION, LOCATION_TABLE_CREATION)


@pytest.fixture
def sample_data_path(tmp_path):
    data_path = os.path.join(tmp_path, "jobs.csv")
    
    with open(data_path, "w") as data_file:
        with open("/workspaces/Data-Internship-Home-Assignment/tests/tmp/source/sample_jobs.csv","r") as data_source:
            data_content=data_source.read()
            data_file.write(data_content)
    return data_path

@pytest.fixture
def sample_extracted_data(tmp_path):
    extracted_data_path = os.path.join(tmp_path, "extracted")
    os.makedirs(extracted_data_path)
    extracted_file_path = os.path.join(extracted_data_path, "item_1.txt")
    with open(extracted_file_path, "w") as extracted_file:
        with open("/workspaces/Data-Internship-Home-Assignment/tests/tmp/extracted/test_extracted_item.txt","r") as extracted_data_source:
            extracted_file_content=extracted_data_source.read()
            extracted_file.write(extracted_file_content)
    return extracted_data_path

@pytest.fixture
def sample_transformed_data(tmp_path):
    transformed_data_path = os.path.join(tmp_path, "transformed")
    os.makedirs(transformed_data_path)
    transformed_file_path = os.path.join(transformed_data_path, "item_1.txt")
    with open(transformed_file_path, "w") as transformed_file:
        with open("/workspaces/Data-Internship-Home-Assignment/tests/tmp/transformed/test_transformed_item.txt","r") as transformed_data_source:
            transformed_file_content=transformed_data_source.read()
            transformed_file.write(transformed_file_content)
    return transformed_data_path

def test_extract_data(sample_data_path, tmp_path):
    extract_data(data_path=sample_data_path, extract_folder=tmp_path)
    extracted_file_path = os.path.join(tmp_path, "item_1.txt")
    assert os.path.exists(extracted_file_path)

def test_transform_data(sample_extracted_data, tmp_path):
    transform_data(src=sample_extracted_data, transform_folder=tmp_path)
    transformed_file_path = os.path.join(tmp_path, "item_1.txt")
    assert os.path.exists(transformed_file_path)

def test_load_data(sample_transformed_data, tmp_path):
    
    
    db_path = os.path.join(tmp_path, "test_db.db")
    sql_connection = sqlite3.connect(db_path)
    cursor = sql_connection.cursor()
    for query in tables_creation_queries:
        cursor.execute(query)
    load_data(sql_connection, src=sample_transformed_data)
    cursor.execute("SELECT * FROM job")
    result = cursor.fetchall()
    cursor.close()
    
    assert len(result) == 1
