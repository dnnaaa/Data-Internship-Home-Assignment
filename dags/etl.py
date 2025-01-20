from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
from etl.sql import TABLES_CREATION_QUERY

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
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)
def etl_dag():
    """ETL pipeline"""

    create_tables = SqliteOperator(
        task_id="create_tables",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY
    )

    # Définir les répertoires de travail
    extract_directory = "/path/to/extracted"
    transform_directory = "/path/to/transformed"
    db_path = "/path/to/sqlite.db"

    create_tables >> extract() >> transform() >> load()

    # Enregistrer les tâches d'extraction, de transformation et de chargement
    extract = task(extract_data)(source_file="source/jobs.csv", output_directory=extract_directory)
    transform = task(transform_data)(input_directory=extract_directory, output_directory=transform_directory)
    load = task(load_data)(input_directory=transform_directory, db_path=db_path)

etl_dag()
