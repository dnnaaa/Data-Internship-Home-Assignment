from datetime import timedelta, datetime

from airflow import DAG
from airflow.decorators import dag, task
# from airflow.models.dag import DAG
# from airflow.providers.sqlite.hooks.sqlite import SqliteHook
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


from src.extract import extract_data
from src.transform import transform_data
from src.load import load_data

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
    start_date=datetime(2024, 1, 7,21,56),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)
def etl_dag():
    """ETL pipeline"""

    # create_tables = SQLExecuteQueryOperator(
    #     task_id="create_tables",
    #     conn_id="my_sqlite",
    #     sql=TABLES_CREATION_QUERIES,
    #     database="airflow.db"
    # )s

    @task(task_id="extract_data")
    def extract():
        """Extract data from jobs.csv."""
        extract_data()

    @task(task_id="transform_data")
    def transform():
        """Clean and convert extracted elements to json."""
        transform_data()

    @task(task_id="load_data")
    def load():
        """Load data to sqlite database."""
        load_data()
        
        # sqlite_hook = SqliteHook(sqlite_conn_id="my_sqlite")
        
        # data_dictionary, cols_dictionary = read_load_data()
        # for k in data_dictionary.keys():
        #     print(f"Inserting data into table {k}")
        #     sqlite_hook.insert_rows(table=k, rows=data_dictionary[k], target_fields=cols_dictionary[k])

    
    extract_task = extract()
    transform_task = transform()
    load_task = load()

    extract_task >> transform_task >> load_task
    
    # create_tables >> extract() >> transform() >> load()

etl_dag()