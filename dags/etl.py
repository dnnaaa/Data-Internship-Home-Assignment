
import sqlite3
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from db_operations import (CREATE_JOB_TABLE, CREATE_COMPANY_TABLE, CREATE_EDUCATION_TABLE, 
                           CREATE_EXPERIENCE_TABLE, CREATE_SALARY_TABLE, CREATE_LOCATION_TABLE)

from extract_task import extract_jobs
from transform_task import transform_data
from load_task import load_data_into_db


def create_tables():
    # open a connection to the SQLite database
    conn = sqlite3.connect('my_db.db')
    c = conn.cursor()

    # SQL commands 
    sql_commands = [
        CREATE_JOB_TABLE,
        CREATE_COMPANY_TABLE,
        CREATE_EDUCATION_TABLE,
        CREATE_EXPERIENCE_TABLE,
        CREATE_SALARY_TABLE,
        CREATE_LOCATION_TABLE
    ]

    # execute each SQL command
    for command in sql_commands:
        c.execute(command)

    # commit the changes 
    conn.commit()
    conn.close()

# DAG default arguments
DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    'start_date': datetime(2019, 2, 15),
    'end_date': datetime(2019, 2, 15),  
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

dag = DAG(
    'etl',
    default_args=DAG_DEFAULT_ARGS,
    description='A simple ETL pipeline using Airflow and SQLite',
    schedule_interval=timedelta(days=1),
)


create_tables_task = PythonOperator(
    task_id='create_tables',
    python_callable=create_tables,
    dag=dag,
)
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_jobs,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data_into_db,
    dag=dag,
)

# defining the pipeline sequence
create_tables_task >> extract_task >> transform_task >> load_task



