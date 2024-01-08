import pytest
from datetime import datetime
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from dag_test import DAG_DEFAULT_ARGS, extract_data_task, transform_data_task, load_data_to_sqlite_task

def dag():
    return DAG(
        dag_id='test_dag',
        default_args=DAG_DEFAULT_ARGS,
        schedule_interval=None,
        start_date=datetime(2024, 1, 7),
        catchup=False,
    )

def test_extract_data_task(dag):
    task_instance = extract_data_task(dag=dag, task_id='test_extract_data_task')
    assert task_instance is not None
    assert task_instance.task_id == 'test_extract_data_task'

def test_transform_data_task(dag):
    task_instance = transform_data_task(dag=dag, task_id='test_transform_data_task')
    assert task_instance is not None
    assert task_instance.task_id == 'test_transform_data_task'

def test_load_data_to_sqlite_task(dag, mocker):
    mocker.patch('dag_test.sqlite3.connect')
    mocker.patch('dag_test.json.load')

    task_instance = load_data_to_sqlite_task(dag=dag, task_id='test_load_data_to_sqlite_task')
    assert task_instance is not None
    assert task_instance.task_id == 'test_load_data_to_sqlite_task'
