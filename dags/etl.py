from datetime import timedelta, datetime
import json
import glob
import os
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.operators.python_operator import PythonOperator
from html import unescape
from typing import List, Dict, Any
import re
from html import unescape

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

@task()
@task()
def extract_job(**kwargs):
    df = pd.read_csv("source/jobs.csv")
    context_data = df['context']

    extracted_folder = "staging/extracted"
    os.makedirs(extracted_folder, exist_ok=True)

    for i, data in enumerate(context_data):
        file_path = os.path.join(extracted_folder, f"extracted_{i}.txt")
        with open(file_path, 'w') as file:
            file.write(data)

    return extracted_folder


@task()
@task()
def transform_job(data_extracted: str, **kwargs):
    transformed_data = []

    for i in range(len(os.listdir(data_extracted))):
        file_path = os.path.join(data_extracted, f"extracted_{i}.txt")
        with open(file_path, 'r') as file:
            data = file.read()
            json_data = json.loads(data)

            transformed_row = {
                "job": {
                    "title": json_data.get("title", ""),
                    "industry": json_data.get("industry", ""),
                    "description": clean_description(json_data.get("description", "")),
                    "employment_type": json_data.get("employmentType", ""),
                    "date_posted": json_data.get("datePosted", ""),
                },
                "company": {
                    "name": json_data.get("hiringOrganization.name", ""),
                    "link": json_data.get("hiringOrganization.sameAs", ""),
                },
                "education": {
                    "required_credential": json_data.get("educationRequirements.credentialCategory", ""),
                },
                "experience": {
                    "months_of_experience": json_data.get("experienceRequirements.monthsOfExperience", ""),
                    "seniority_level": json_data.get("experienceRequirements.seniorityLevel", ""),
                },
                "salary": {
                    "currency": json_data.get("salary.currency", ""),
                    "min_value": json_data.get("salary.min_value", ""),
                    "max_value": json_data.get("salary.max_value", ""),
                    "unit": json_data.get("salary.unit", ""),
                },
                "location": {
                    "country": json_data.get("jobLocation.address.addressCountry", ""),
                    "locality": json_data.get("jobLocation.address.addressLocality", ""),
                    "region": json_data.get("jobLocation.address.addressRegion", ""),
                    "postal_code": json_data.get("jobLocation.address.postalCode", ""),
                    "street_address": json_data.get("jobLocation.address.streetAddress", ""),
                    "latitude": json_data.get("jobLocation.latitude", ""),
                    "longitude": json_data.get("jobLocation.longitude", ""),
                },
            }

            transformed_data.append(transformed_row)

    return transformed_data


def clean_description(description):
    # Removing HTML tags
    cleaned_description = re.sub(r'<[^>]*>', '', description)
    
    # Unescaping HTML entities (convert &lt; to <, &gt; to >, etc.)
    cleaned_description = unescape(cleaned_description)
    
    # Removing extra whitespaces and newline characters
    cleaned_description = ' '.join(cleaned_description.split())

    # Removing special characters and clean formatting
    cleaned_description = re.sub(r'[^\w\s]', '', cleaned_description)

    # Adding more custom cleaning logic as needed
    # Example: Remove special characters, clean formatting, etc.

    return cleaned_description

def load_data_to_sqlite(cursor, data_transformed):
    # Assuming 'data_transformed' is a list of dictionaries, each representing a row

    # Iterating through each table and load data
    for table_name, columns in TABLES_CREATION_QUERY.items():
        # Extracting values for the specific table
        table_data = [row[table_name] for row in data_transformed]

        # Building the INSERT query
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['?' for _ in columns])
        insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        # Executing the INSERT query with multiple values
        cursor.executemany(insert_query, table_data)


@task()
def load_job(data_transformed: List[Dict[str, Any]], **kwargs):
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    conn = sqlite_hook.get_conn()
    cursor = conn.cursor()

    # Assuming you have a function to convert DataFrame to SQLite
    load_data_to_sqlite(cursor, data_transformed)

    # Commitming changes and close the connection
    conn.commit()
    conn.close()

    return f"Loaded {len(data_transformed)} rows to SQLite"


DAG_DEFAULT_ARGS = {ss
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

@dag(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)
def etl_dag():
    """ETL pipeline"""

    create_tables = SqliteOperator(
        task_id="create_tables",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY
    )

    extract_data = extract()
    transform_data_task = transform(extract_data)
    load_data_task = load(transform_data_task)

    create_tables >> extract_data >> transform_data_task >> load_data_task

etl_dag()

