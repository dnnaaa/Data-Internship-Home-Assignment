from datetime import timedelta, datetime
import os
from airflow.decorators import dag
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from tasks.extract import extract
from tasks.transform import transform
from tasks.load import load
from tasks.create_tables import TABLES_CREATION_QUERY

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
    start_date=datetime(2025, 1, 20),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)

def etl_dag():

    create_tables = SqliteOperator(
        task_id="create_tables",
        sqlite_conn_id="sqlite_default",
        sql= TABLES_CREATION_QUERY
    )


    source_file = os.path.abspath("source/jobs.csv")

    extracted_dir = os.path.abspath("staging/extracted")
    transformed_dir = os.path.abspath("staging/transformed")

    database_path = os.path.abspath('jobs.sqlite')

    extract_task = extract(source_file, extracted_dir)
    transform_task = transform(extracted_dir, transformed_dir)
    load_task = load(transformed_dir, database_path)


    create_tables >> extract_task >> transform_task >> load_task

dag = etl_dag()
