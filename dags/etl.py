from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.models.baseoperator import chain

# Import tasks from separate modules
from Extracting import extract
from Transforming import transform
from Loading import load
from creating_Tables import TABLES_CREATION_QUERY

DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

@dag(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule=None,
    start_date=datetime(2024, 1, 6),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)

def etl_dag():
    """ETL pipeline"""
    create_table_statements = TABLES_CREATION_QUERY.strip().split(";")
    create_table_statements = [stmt.strip() for stmt in create_table_statements if stmt.strip()]

    # Create a SqliteOperator task for each CREATE TABLE statement
    create_table_tasks = []
    for i, create_table_statement in enumerate(create_table_statements):
        task = SqliteOperator(
            task_id=f"create_table_{i}",
            sqlite_conn_id="sqlite_default",
            sql=create_table_statement
        )
        create_table_tasks.append(task)

    # Chain the tasks together in the required order
    chain(*create_table_tasks, extract(), transform(), load())

etl_dag_instance = etl_dag()
