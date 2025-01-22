from datetime import datetime, timedelta
from airflow.decorators import dag
#from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from tasks.extract import extract
from tasks.transform import transform
from tasks.load import load
from tasks.init_database import init_database 

DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

@dag(
    dag_id="etl_dag",
    description="ETL for job posts",
    tags=["etl"],
    schedule="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)
def etl_dag():
    """ETL pipeline with database initialization"""
    
    # Define paths
    SQL_FILE_PATH = "sqlQueries/create_tables.sql"  
    DB_PATH = "database/jobs.db"  
    
    # Task 1: Initialize database
    init_db = init_database(
        sql_path=SQL_FILE_PATH,
        db_path=DB_PATH
    )

    # Task 2: Initialize database
    extract_task = extract(
        csv_file_path='source/jobs.csv',
        output_dir='staging/extracted'
    )
    
    # Task 3: Transform data
    transform_task = transform(
        input_dir='staging/extracted',
        output_dir='staging/transformed'
    )
    
    # Task 4: Load data to sqlite database
    load_task = load(
        input_dir='staging/transformed',
        database_path='database/jobs.db'
    )
    
    # Set up task dependencies
    init_db >> extract_task >> transform_task >> load_task

etl_dag()