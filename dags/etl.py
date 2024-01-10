from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

from extract import extract_jobs_csv
from transform import transform_extracted_files
from load import load_to_database

# Split your table creation queries into separate statements
table_creation_queries = [
    """
    CREATE TABLE IF NOT EXISTS job (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title VARCHAR(225),
        industry VARCHAR(225),
        description TEXT,
        employment_type VARCHAR(125),
        date_posted DATE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS company (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        name VARCHAR(225),
        link TEXT,
        FOREIGN KEY (job_id) REFERENCES job(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS education (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        required_credential VARCHAR(225),
        FOREIGN KEY (job_id) REFERENCES job(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS experience (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        months_of_experience INTEGER,
        seniority_level VARCHAR(25),
        FOREIGN KEY (job_id) REFERENCES job(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS salary (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        currency VARCHAR(3),
        min_value NUMERIC,
        max_value NUMERIC,
        unit VARCHAR(12),
        FOREIGN KEY (job_id) REFERENCES job(id)
    );
    """,
    """
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
]

@dag(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args={
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=15)
    }
)
def etl_dag():
    # Create a separate task for each table creation query
    create_table_tasks = [
        SqliteOperator(
            task_id=f'create_{table_name}_table',
            sqlite_conn_id="sqlite_default",
            sql=query
        )
        for table_name, query in zip(
            ['job', 'company', 'education', 'experience', 'salary', 'location'], 
            table_creation_queries
        )
    ]

    # Task generator function to extract data
    @task(task_id='extract')
    def extract():
        """Extract data from jobs.csv."""
        extract_jobs_csv('/workspaces/Data-Internship-Home-Assignment/source/jobs.csv', '/workspaces/Data-Internship-Home-Assignment/staging/extracted')

    # Task generator function to transform data
    @task(task_id='transform')
    def transform():
        """Clean and convert extracted elements to json."""
        transform_extracted_files('/workspaces/Data-Internship-Home-Assignment/staging/extracted', '/workspaces/Data-Internship-Home-Assignment/staging/transformed')

    # Task generator function to load data
    @task(task_id='load')
    def load():
        """Load data to sqlite database."""
        load_to_database('staging/transformed')

    # Generate task instances
    extract_task = extract()
    transform_task = transform()
    load_task = load()

    # Set up the task dependencies
    for create_table_task in create_table_tasks:
        create_table_task >> extract_task
    
    extract_task >> transform_task >> load_task

# Instantiate the DAG
etl_pipeline = etl_dag()


