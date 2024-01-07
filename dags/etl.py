from datetime import timedelta, datetime
import pandas as pd
import json
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

@task()
def extract():
    """Extract data from jobs.csv."""
    df=pd.read_csv('../source/jobs.csv')
    return df.to_csv('../staging/extracted.txt', sep='\t', index=False) 

@task()
def transform():
    """Clean and convert extracted elements to json."""
    with open('../staging/extracted.txt', 'r') as txt_file:
        lines = txt_file.readlines()

    columns = lines[0].strip().split('\t')
    data = []

    for line in lines[1:]:
        values = line.strip().split('\t')
        row = dict(zip(columns, values))
        data.append(row)

    with open('../staging/transformed.json', 'w') as json_file:
        return json.dump(data, json_file, indent=2)
     
@task()
def load():
    """Load data to sqlite database."""
    json_file_path = 'staging/transformed.json'

    with open(json_file_path, 'r') as json_file:
        data = json.load(json_file)

    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    connection = sqlite_hook.get_conn()
    cursor = connection.cursor()

    for row in data:
        columns = ', '.join(row.keys())
        values = ', '.join(['?' for _ in row.values()])
        insert_query = f"INSERT INTO table_name ({columns}) VALUES ({values})"

        cursor.execute(insert_query, tuple(row.values()))

    connection.commit()
    connection.close()


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
