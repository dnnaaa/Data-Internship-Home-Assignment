from datetime import timedelta, datetime
from airflow.decorators import dag
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from etl.extract import extract_to_text_file
from etl.transform import transform_to_json
from etl.load import load_to_database
from sql.sql_queries import TABLES_CREATION_QUERY

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

    create_tables_task = SqliteOperator(
        task_id="create_tables",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY
    )

    extract_task = extract_to_text_file()
    transform_task = transform_to_json()
    load_task = load_to_database()

    create_tables_task >> extract_task >> transform_task >> load_task

etl_dag()
