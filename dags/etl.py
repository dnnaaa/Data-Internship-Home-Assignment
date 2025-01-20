from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from constants import TABLES_CREATION_QUERY
from extract import JobsExtractor
from transform import JobsTransformer
from load import SqliteLoader

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
    @task()
    def extract():
        extractor = JobsExtractor()
        return extractor.extract()
        
    @task()
    def transform():
        transformer = JobsTransformer()
        return transformer.transform()
        
    @task()
    def load():
        loader = SqliteLoader()
        return loader.load()

    create_tables = SqliteOperator(
        task_id="create_tables",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY
    )
    
    create_tables >> extract() >> transform() >> load()

etl_dag()