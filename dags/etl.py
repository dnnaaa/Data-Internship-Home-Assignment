from datetime import timedelta, datetime
import os
import json
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

TABLES_CREATION_QUERY = """
CREATE TABLE IF NOT EXISTS job (
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
);
"""

@task()
def extract(staging_dir):
    """Extract data from jobs.csv."""
    source_file = "source/jobs.csv"
    staging_file = os.path.join(staging_dir, "jobs.csv")
    shutil.copy(source_file, staging_file)

@task()
def transform(staging_dir):
    """Clean and convert extracted elements to json."""
    staging_file = os.path.join(staging_dir, "jobs.csv")

    # Perform data transformation
    transformed_data = transform_data(staging_file)

    transformed_file = os.path.join(staging_dir, "transformed.json")
    with open(transformed_file, "w") as f:
        json.dump(transformed_data, f)

@task()
def load(staging_dir):
    """Load data to sqlite database."""
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')

    transformed_file = os.path.join(staging_dir, "transformed.json")
    with open(transformed_file, "r") as f:
        data = json.load(f)

    # Load data to the SQLite database
    # Insert the data into the appropriate tables using the SQLiteHook

    # Example:
    job_data = data['job']
    company_data = data['company']
    # ...

    # Insert job data
    job_id = sqlite_hook.insert_rows(
        table='job',
        rows=[(
            job_data['title'],
            job_data['industry'],
            job_data['description'],
            job_data['employment_type'],
            job_data['date_posted']
        )]
    )

    # Insert company data
    company_id = sqlite_hook.insert_rows(
        table='company',
        rows=[(
            job_id,
            company_data['name'],
            company_data['link']
        )]
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

    staging_dir = "staging"

    create_tables = SqliteOperator(
        task_id="create_tables",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY
    )

    extract_task = extract(staging_dir)
    transform_task = transform(staging_dir)
    load_task = load(staging_dir)

    create_tables >> extract_task >> transform_task >> load_task

etl_dag()