from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from extract_data import extract_data
from clean_transform import clean_transform
from load_data import load_data
from tables_creation import TABLES_CREATION_QUERY
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

dag = DAG(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)

create_tables_task = SqliteOperator(
    task_id="create_tables",
    sqlite_conn_id="sqlite_default",
    sql=TABLES_CREATION_QUERY,
    dag=dag,
)

extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='clean_transform',
    python_callable=clean_transform,
    provide_context=True,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

create_tables_task >> extract_data_task >> transform_data_task >> load_data_task
