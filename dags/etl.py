from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.sqlite.operators.sqlite import SqliteOperator


from packages.extract import extract_csv_data
from packages.transform import transform_data
from packages.load import load_data_to_sqlite
from packages import utils


@task()
def extract():
    message = extract_csv_data(
        utils.TEST_FILE_PATH, utils.EXTRACTION_PATH)
    return message


@task()
def transform():
    message = transform_data(
        utils.EXTRACTION_PATH, utils.TRANSFORMATION_PATH)
    return message


@task()
def load():
    message = load_data_to_sqlite(utils.TRANSFORMATION_PATH)
    return message


@dag(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=utils.DAG_DEFAULT_ARGS
)
def etl_dag():
    """ETL pipeline"""

    previous_task = None
    for table_name, sql_query in utils.TABLES_CREATION_QUERY.items():
        create_tables = SqliteOperator(
            task_id=f"create_{table_name}_table",
            sqlite_conn_id="sqlite_default",
            sql=sql_query
        )

        if previous_task:
            previous_task >> create_tables

        previous_task = create_tables

    extraction = extract()
    transformation = transform()
    loading = load()

    previous_task >> extraction >> transformation >> loading


etl_dag()
