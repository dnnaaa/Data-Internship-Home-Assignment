from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import extract, transform, load


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL pipeline using Airflow',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_job',
        python_callable=extract.extract_job,
    )

    transform_task = PythonOperator(
        task_id='transform_job',
        python_callable=transform.transform_job,
    )

    load_task = PythonOperator(
        task_id='load_job',
        python_callable=load.load_job,
    )

    extract_task >> transform_task >> load_task
