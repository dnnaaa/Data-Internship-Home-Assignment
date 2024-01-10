from datetime import timedelta, datetime
from airflow.decorators import dag
from etl_extract import extract
from etl_transform import transform
from etl_load import load

DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

@dag(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)
def etl_dag():
    """ETL pipeline"""
    extract_task = extract()
    transform_task = transform()
    load_task = load()

    extract_task >> transform_task >> load_task

etl_dag = etl_dag()
