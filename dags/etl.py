from datetime import timedelta, datetime

from airflow.decorators import dag

from load import load
from create_table import create_tables
from extract import extract
from transform import transform

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
    start_date=datetime(2024, 1, 10),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)

def etl_dag():
    """ETL pipeline"""

    create_tables_task = create_tables()
    extract_task = extract()
    transform_task = transform()
    load_task = load()

    create_tables_task >> extract_task >> transform_task >> load_task


etl_dag()