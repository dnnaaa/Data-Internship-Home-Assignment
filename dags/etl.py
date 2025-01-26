from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from extract import extract
from transform import transform
from load import load
from datetime import datetime

# Définir les requêtes de création de tables
TABLES_CREATION_QUERIES = [
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
        FOREIGN KEY (job_id) REFERENCES job (id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS education (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        required_credential VARCHAR(225),
        FOREIGN KEY (job_id) REFERENCES job (id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS experience (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        months_of_experience INTEGER,
        seniority_level VARCHAR(125),
        FOREIGN KEY (job_id) REFERENCES job (id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS salary (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        currency VARCHAR(10),
        min_value INTEGER,
        max_value INTEGER,
        unit VARCHAR(50),
        FOREIGN KEY (job_id) REFERENCES job (id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS location (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,
        country VARCHAR(100),
        locality VARCHAR(100),
        region VARCHAR(100),
        postal_code VARCHAR(20),
        street_address TEXT,
        latitude REAL,
        longitude REAL,
        FOREIGN KEY (job_id) REFERENCES job (id)
    );
    """
]

# Définir le DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

@dag(
    default_args=default_args,
    description='ETL DAG for job data',
    schedule_interval='@daily',
    catchup=False,
)
def etl_dag():
    # Tâche pour créer les tables
    create_tables_tasks = [
        SQLExecuteQueryOperator(
            task_id=f'create_table_{i}',
            conn_id='sqlite_default',
            sql=query
        ) for i, query in enumerate(TABLES_CREATION_QUERIES)
    ]

    # Tâches d'extraction, de transformation et de chargement
    extract_task = extract()
    transform_task = transform()
    load_task = load()

    # Définir les dépendances des tâches
    create_tables_tasks >> extract_task >> transform_task >> load_task

etl_dag()