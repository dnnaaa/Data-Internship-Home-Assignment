from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from tasks.transform import transform 
from tasks.extract import extract_data 
from tasks.load import load_data_to_sqlite
import pandas as pd
import json
import os
import logging

TABLES_CREATION_QUERY = """CREATE TABLE IF NOT EXISTS job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(225),
    industry VARCHAR(225),
    description TEXT,
    employment_type VARCHAR(125),
    date_posted DATE
);

CREATE TABLE IF NOT EXISTS company (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    name VARCHAR(225),
    link TEXT,
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS education (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    required_credential VARCHAR(225),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS experience (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    months_of_experience INTEGER,
    seniority_level VARCHAR(25),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS salary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    currency VARCHAR(3),
    min_value NUMERIC,
    max_value NUMERIC,
    unit VARCHAR(12),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS location (
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
@task
def create__tables():
    """Create tables in the SQLite database."""
    sqlite_hook = SqliteHook(sqlite_conn_id="sqlite_default")  # Initialize SQLite hook
    
    # Split the queries into individual statements
    queries = TABLES_CREATION_QUERY.strip().split(";")
    
    # Execute each query in the SQLite database
    for query in queries:
        if query.strip():  # Skip empty statements
            sqlite_hook.run(query)
@task()
def extract():
    """Extract data using the function from transform.py."""
    extracted_files = extract_data()
    if extracted_files:
        logging.info(f"Extracted files: {extracted_files}")
    else:
        logging.warning("No files were extracted.")

@task()
def transform_data():
    """Clean and convert extracted elements to JSON."""
    transform()  # Call the transform function imported from your transform.py file

@task()
def load():
    """Load data using the function from transform.py."""
    result = load_data_to_sqlite()
    logging.info(result)

DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

@dag(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)
def etl_dag():
    """ETL pipeline"""       

    create__tables() >> extract() >> transform_data() >> load()

etl_dag()
