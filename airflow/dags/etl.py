from datetime import timedelta, datetime
from airflow.decorators import dag
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from tasks.extract import extract_data
from tasks.transform import transform_data
from tasks.load import load_data
from utils.constants import DAG_DEFAULT_ARGS, TABLES_CREATION_QUERY

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
    """ETL pipeline for processing LinkedIn job posts"""
    
    create_tables = SqliteOperator(
        task_id="create_tables",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY
    )

    # Define tasks
    extract = extract_data()
    transform = transform_data()
    load = load_data()

    # Set dependencies
    create_tables >> extract >> transform >> load

etl_dag()