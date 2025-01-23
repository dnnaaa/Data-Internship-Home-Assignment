from datetime import timedelta, datetime
from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Import tasks from the tasks folder (assuming the tasks are defined in a module)
from tasks.extract import extract_task
from tasks.transform import transform_task
from tasks.load import load_task

# SQL to create the tables (adjust if necessary)
TABLES_CREATION_QUERY = """
-- Create Job Table
CREATE TABLE IF NOT EXISTS job (
    id SERIAL PRIMARY KEY,
    title TEXT,
    industry TEXT,
    description TEXT,
    employment_type TEXT,
    date_posted TEXT
);

-- Create Company Table
CREATE TABLE IF NOT EXISTS company (
    id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES job(id),
    name TEXT,
    link TEXT
);

-- Create Education Table
CREATE TABLE IF NOT EXISTS education (
    id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES job(id),
    required_credential TEXT
);

-- Create Experience Table
CREATE TABLE IF NOT EXISTS experience (
    id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES job(id),
    months_of_experience INTEGER,
    seniority_level TEXT
);

-- Create Salary Table
CREATE TABLE IF NOT EXISTS salary (
    id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES job(id),
    currency TEXT,
    min_value INTEGER,
    max_value INTEGER,
    unit TEXT
);

-- Create Location Table
CREATE TABLE IF NOT EXISTS location (
    id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES job(id),
    country TEXT,
    locality TEXT,
    region TEXT,
    postal_code TEXT,
    street_address TEXT,
    latitude REAL,
    longitude REAL
);
"""

DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

@dag(
    dag_id="def20",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule="@daily",  # Adjust the schedule as needed
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)
def etl_dag():
    """ETL pipeline for LinkedIn job posts"""
    
    # Task to create tables in PostgreSQL
    create_tables_task = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="postgres_db_connection",  # Make sure this connection is configured in Airflow UI
        sql=TABLES_CREATION_QUERY  # SQL query to create tables
    )

    # Define task dependencies (ensure extract, transform, and load tasks are defined in tasks module)
    create_tables_task >> extract_task >> transform_task >> load_task

# Instantiate the DAG
etl_dag()