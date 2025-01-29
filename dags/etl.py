from datetime import timedelta, datetime
import os
import json

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

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

STAGING_DIR = "staging"
EXTRACTED_DIR = os.path.join(STAGING_DIR, "extracted")
TRANSFORMED_DIR = os.path.join(STAGING_DIR, "transformed")

os.makedirs(EXTRACTED_DIR, exist_ok=True)
os.makedirs(TRANSFORMED_DIR, exist_ok=True)

@task()
def extract():
    """Extract data from jobs.csv."""
    df = pd.read_csv("source/jobs.csv")
    df["context"] = df["context"].fillna("")
    for i, context in enumerate(df["context"]):
        with open(os.path.join(EXTRACTED_DIR, f"job_{i}.txt"), "w") as f:
            f.write(str(context))

@task()
def transform():
    """Clean and convert extracted elements to json."""
    for file_name in os.listdir(EXTRACTED_DIR):
        with open(os.path.join(EXTRACTED_DIR, file_name), "r") as f:
            raw_data = json.loads(f.read())

        transformed_data = {
            "job": {
                "title": raw_data.get("title"),
                "industry": raw_data.get("industry"),
                "description": raw_data.get("description"),
                "employment_type": raw_data.get("employmentType"),
                "date_posted": raw_data.get("datePosted"),
            },
            "company": {
                "name": raw_data.get("hiringOrganization", {}).get("name", ""),
                "link": raw_data.get("hiringOrganization", {}).get("sameAs", ""),
            },
            "education": {
                "required_credential": raw_data.get("educationRequirements", {}).get("credentialCategory", ""),
            },
            "experience": {
                "months_of_experience": raw_data.get("job_months_of_experience"),
                "seniority_level": raw_data.get("experienceRequirements", {}).get("seniorityLevel", ""),
            },
            "salary": {
                "currency": raw_data.get("estimatedSalary", {}).get("currency", ""),
                "min_value": raw_data.get("estimatedSalary", {}).get("value", {}).get("minValue", ""),
                "max_value": raw_data.get("estimatedSalary", {}).get("value", {}).get("maxValue", ""),
                "unit": raw_data.get("estimatedSalary", {}).get("value", {}).get("unitText", ""),
            },
            "location": {
                "country": raw_data.get("jobLocation", {}).get("address", {}).get("addressCountry", ""),
                "locality": raw_data.get("jobLocation", {}).get("address", {}).get("addressLocality", ""),
                "region": raw_data.get("jobLocation", {}).get("address", {}).get("addressRegion", ""),
                "postal_code": raw_data.get("jobLocation", {}).get("address", {}).get("postalCode", ""),
                "street_address": raw_data.get("jobLocation", {}).get("address", {}).get("streetAddress", ""),
                "latitude": raw_data.get("jobLocation", {}).get("latitude", ""),
                "longitude": raw_data.get("jobLocation", {}).get("longitude", ""),
            },
        }

        with open(os.path.join(TRANSFORMED_DIR, file_name), "w") as f:
            json.dump(transformed_data, f)

@task()
def load():
    """Load data to sqlite database."""
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    with sqlite_hook.get_conn() as conn:
        cursor = conn.cursor()
        for file_name in os.listdir(TRANSFORMED_DIR):
            with open(os.path.join(TRANSFORMED_DIR, file_name), "r") as f:
                data = json.load(f)
                job = data["job"]
                company = data["company"]
                education = data["education"]
                experience = data["experience"]
                salary = data["salary"]
                location = data["location"]

                cursor.execute(
                    """
                    INSERT INTO job (title, industry, description, employment_type, date_posted)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (job["title"], job["industry"], job["description"], job["employment_type"], job["date_posted"])
                )
                job_id = cursor.lastrowid

                cursor.execute(
                    """
                    INSERT INTO company (job_id, name, link)
                    VALUES (?, ?, ?)
                    """,
                    (job_id, company["name"], company["link"])
                )

                cursor.execute(
                    """
                    INSERT INTO education (job_id, required_credential)
                    VALUES (?, ?)
                    """,
                    (job_id, education["required_credential"])
                )

                cursor.execute(
                    """
                    INSERT INTO experience (job_id, months_of_experience, seniority_level)
                    VALUES (?, ?, ?)
                    """,
                    (job_id, experience["months_of_experience"], experience["seniority_level"])
                )

                cursor.execute(
                    """
                    INSERT INTO salary (job_id, currency, min_value, max_value, unit)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (job_id, salary["currency"], salary["min_value"], salary["max_value"], salary["unit"])
                )

                cursor.execute(
                    """
                    INSERT INTO location (job_id, country, locality, region, postal_code, street_address, latitude, longitude)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job_id, location["country"], location["locality"], location["region"],
                        location["postal_code"], location["street_address"], location["latitude"], location["longitude"]
                    )
                )

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

    create_tables = SqliteOperator(
        task_id="create_tables",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY
    )

    create_tables >> extract() >> transform() >> load()

etl_dag()
