from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

from create_tables import TABLES_CREATION_QUERY
from extract import extract_job_data
from transform import transform_job_data
from load import load_transformed_data

DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

def create_dag():
    dag = DAG(
        dag_id="etl_dag",
        description="ETL LinkedIn job posts",
        tags=["etl"],
        schedule_interval="@daily",
        start_date=datetime(2022, 1, 2),  
        catchup=False,
        default_args=DAG_DEFAULT_ARGS,
    )

    create_tables = SqliteOperator(
        task_id="create_tables",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY,
        dag=dag
    )

    extract_task = extract_job_data(
        source_csv_path="source/jobs.csv",
        extracted_folder="staging/extracted",
    )

    transform_task = transform_job_data(
        extracted_folder="staging/extracted",
        transformed_folder="staging/transformed",
    )

    load_task = load_transformed_data(
        transformed_folder="staging/transformed",
        sqlite_db_path="sqlite:////home/mohammed/Desktop/Project/Data-Internship-Home-Assignment-main/airflow.db",
    )

    create_tables >> extract_task >> transform_task >> load_task

    return dag

etl_dag_instance = create_dag()
