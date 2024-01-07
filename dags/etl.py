from datetime import timedelta, datetime
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from dags.tasks.extract import extract
from dags.tasks.transform import transform
from dags.tasks.load import load
from dags.sql.creates_tables import CREATE_TABLES_SQL
import os



DEFAULT_ARGS = {
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
    default_args=DEFAULT_ARGS
)
def etl_dag():
    source_file = os.path.join(os.path.dirname(__file__), '..', 'source', 'jobs.csv')
    source_dir = os.path.join(os.path.dirname(__file__), '..', 'staging', 'extracted')
    target_dir = os.path.join(os.path.dirname(__file__), '..', 'staging', 'transformed')
    db_file = os.path.join(os.path.dirname(__file__), 'sql', 'db.sqlite')
    create_tables = SQLExecuteQueryOperator(
        task_id='create_tables',
        conn_id='sqlite_nedday',
        sql=CREATE_TABLES_SQL
    )

    extract_task = extract(source_file, source_dir)
    transform_task = transform(source_dir, target_dir)
    load_task = load(target_dir,db_file)

    create_tables >> extract_task >> transform_task >> load_task


dag = etl_dag()
