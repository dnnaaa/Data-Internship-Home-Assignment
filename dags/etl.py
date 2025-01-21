from datetime import timedelta, datetime
import pandas as pd
import json
import os

from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

TABLES_CREATION_QUERY = [
    """CREATE TABLE IF NOT EXISTS job (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title VARCHAR(225),
        industry VARCHAR(225),
        description TEXT,
        employment_type VARCHAR(125),
        date_posted DATE
    );""",
    """CREATE TABLE IF NOT EXISTS company (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        name VARCHAR(225),
        link TEXT,
        FOREIGN KEY (job_id) REFERENCES job(id)
    );""",
    """CREATE TABLE IF NOT EXISTS education (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        required_credential VARCHAR(225),
        FOREIGN KEY (job_id) REFERENCES job(id)
    );""",
    """CREATE TABLE IF NOT EXISTS experience (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        months_of_experience INTEGER,
        seniority_level VARCHAR(25),
        FOREIGN KEY (job_id) REFERENCES job(id)
    );""",
    """CREATE TABLE IF NOT EXISTS salary (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        currency VARCHAR(3),
        min_value NUMERIC,
        max_value NUMERIC,
        unit VARCHAR(12),
        FOREIGN KEY (job_id) REFERENCES job(id)
    );""",
    """CREATE TABLE IF NOT EXISTS location (
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
    );"""
]

create_tables = SQLExecuteQueryOperator(
    task_id="create_tables",
    sql=TABLES_CREATION_QUERY,
    conn_id="sqlite_default",
    split_statements=True
)
# Define constants for directories and file paths
SOURCE_FILE = "source/jobs.csv"
EXTRACTED_DIR = "staging/extracted"
TRANSFORMED_DIR = "staging/transformed"


@task()
def extract():
    """Extract data from jobs.csv."""
    os.makedirs(EXTRACTED_DIR, exist_ok=True)
    df = pd.read_csv(SOURCE_FILE)

    # Extract the context column and save each item to a text file
    for i, context in enumerate(df["context"]):
        # Convert the context to a string, replacing NaN values with an empty string
        context = str(context) if pd.notna(context) else ""
        with open(f"{EXTRACTED_DIR}/context_{i}.txt", "w") as file:
            file.write(context)
    return EXTRACTED_DIR

@task()
def transform():
    """Clean and convert extracted elements to JSON."""
    os.makedirs(TRANSFORMED_DIR, exist_ok=True)

    for file_name in os.listdir(EXTRACTED_DIR):
        file_path = os.path.join(EXTRACTED_DIR, file_name)

        # Read the extracted text file and validate its content
        with open(file_path, "r") as file:
            content = file.read().strip()
            if not content:  # Check for empty file
                print(f"Warning: {file_name} is empty. Skipping.")
                continue
            try:
                context_data = json.loads(content)  # Validate JSON
            except json.JSONDecodeError:
                print(f"Error: {file_name} contains invalid JSON. Skipping.")
                continue

        # Clean and reshape the data
        transformed_data = {
            "job": {
                "title": "job_title",
                "industry": "job_industry",
                "description": "job_description",
                "employment_type": "job_employment_type",
                "date_posted": "job_date_posted",
            },
            "company": {
                "name": "company_name",
                "link": "company_linkedin_link",
            },
            "education": {
                "required_credential": "job_required_credential",
            },
            "experience": {
                "months_of_experience": "job_months_of_experience",
                "seniority_level": "seniority_level",
            },
            "salary": {
                "currency": "salary_currency",
                "min_value": "salary_min_value",
                "max_value": "salary_max_value",
                "unit": "salary_unit",
            },
            "location": {
                "country": "country",
                "locality": "locality",
                "region": "region",
                "postal_code": "postal_code",
                "street_address": "street_address",
                "latitude": "latitude",
                "longitude": "longitude",
            },
        }

        # Save the transformed data as a JSON file
        with open(f"{TRANSFORMED_DIR}/transformed_{file_name}.json", "w") as json_file:
            json.dump(transformed_data, json_file)

    return TRANSFORMED_DIR
    

@task()
def load():
    """Load data to SQLite database."""
    sqlite_hook = SqliteHook(sqlite_conn_id="sqlite_default")

    for file_name in os.listdir(TRANSFORMED_DIR):
        file_path = os.path.join(TRANSFORMED_DIR, file_name)

        # Read the JSON file
        try:
            with open(file_path, "r") as json_file:
                data = json.load(json_file)
        except (json.JSONDecodeError, FileNotFoundError) as e:
            print(f"Error: Could not read or parse {file_name}. Skipping. ({e})")
            continue

        try:
            # Start a transaction
            with sqlite_hook.get_conn() as conn:
                cursor = conn.cursor()

                # Insert into the job table and get the job_id
                cursor.execute("""
                    INSERT INTO job (title, industry, description, employment_type, date_posted)
                    VALUES (?, ?, ?, ?, ?);
                """, (
                    data["job"]["title"],
                    data["job"]["industry"],
                    data["job"]["description"],
                    data["job"]["employment_type"],
                    data["job"]["date_posted"]
                ))
                job_id = cursor.lastrowid  # Retrieve the auto-generated job_id

                # Insert into the company table
                cursor.execute("""
                    INSERT INTO company (job_id, name, link)
                    VALUES (?, ?, ?);
                """, (
                    job_id,
                    data["company"]["name"],
                    data["company"]["link"]
                ))

                # Insert into the education table
                cursor.execute("""
                    INSERT INTO education (job_id, required_credential)
                    VALUES (?, ?);
                """, (
                    job_id,
                    data["education"]["required_credential"]
                ))

                # Insert into the experience table
                cursor.execute("""
                    INSERT INTO experience (job_id, months_of_experience, seniority_level)
                    VALUES (?, ?, ?);
                """, (
                    job_id,
                    data["experience"]["months_of_experience"],
                    data["experience"]["seniority_level"]
                ))

                # Insert into the salary table
                cursor.execute("""
                    INSERT INTO salary (job_id, currency, min_value, max_value, unit)
                    VALUES (?, ?, ?, ?, ?);
                """, (
                    job_id,
                    data["salary"]["currency"],
                    data["salary"]["min_value"],
                    data["salary"]["max_value"],
                    data["salary"]["unit"]
                ))

                # Insert into the location table
                cursor.execute("""
                    INSERT INTO location (job_id, country, locality, region, postal_code, street_address, latitude, longitude)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """, (
                    job_id,
                    data["location"]["country"],
                    data["location"]["locality"],
                    data["location"]["region"],
                    data["location"]["postal_code"],
                    data["location"]["street_address"],
                    data["location"]["latitude"],
                    data["location"]["longitude"]
                ))

                # Commit the transaction
                conn.commit()

        except Exception as e:
            print(f"Error: Failed to load data from {file_name}. Skipping. ({e})")


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

    create_tables_task = SQLExecuteQueryOperator(
    task_id="create_tables",
    sql=TABLES_CREATION_QUERY,  # Pass the list of queries
    conn_id="sqlite_default",
    split_statements=True  # Enable executing multiple statements
    )

    create_tables_task >> extract() >> transform() >> load()

etl_dag()
