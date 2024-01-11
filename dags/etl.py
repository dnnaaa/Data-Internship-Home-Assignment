from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from dags.needed_functions import *

# defining tasks

@task()
def extract():
    extract_data()
    
    

@task()
def transform():
    transform_task_function()

@task()
def load():
    load_data_to_db()
   


@dag(
    dag_id="etl_dag1",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)
def etl_dag():

    # this array contains create table tasks 
    create_table_tasks = []
    
    for queryNumber, query in enumerate(TABLES_CREATION_QUERY):
        task_id = f"create_table_{queryNumber}"
        create_table = SqliteOperator(
            task_id=task_id,
            sql=query
        )
        create_table_tasks.append(create_table)

    create_table_tasks >> extract() >> transform() >> load()

etl_dag()
