import os
import json
from blinker import ANY
import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import patch, Mock, call
from airflow.models import DagBag, TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
import dags
#from dags.etl import etl_dag, extract, transform, load

# Define file paths
SOURCE_CSV = "source/jobs.csv"
EXTRACTED_DIR = "staging/extracted"
TRANSFORMED_DIR = "staging/transformed"
DATABASE_FILE = "etl_database.db"

@pytest.fixture
def dag_bag():
    return DagBag(dag_folder=os.path.dirname(os.path.realpath(__file__)))

def test_dag_loads_without_errors(dag_bag):
    assert dag_bag.dags is not None
    assert dag_bag.import_errors == {}

def test_etl_dag_structure(dag_bag):
    dag = dag_bag.get_dag(dag_id="etl_dag")
    assert dag is not None

    task_ids = [task.task_id for task in dag.tasks]
    assert task_ids == ["create_tables", "extract", "transform", "load"]

def test_extract_task():
    task = dags.etl.extract()
    assert task is not None
    assert task.task_id == "extract"

def test_transform_task():
    task = dags.etl.transform()
    assert task is not None
    assert task.task_id == "transform"

def test_load_task():
    task = dags.etl.load()
    assert task is not None
    assert task.task_id == "load"

def test_extracted_files_created():
    df = pd.read_csv(SOURCE_CSV)
    df.dropna(axis=0, inplace=True)
    context_data = df['context']

    dags.etl.extract()
    
    # Check if the expected number of files are created
    assert len(os.listdir(EXTRACTED_DIR)) == len(context_data)

def test_transformed_files_created():
    dags.etl.extract()
    dags.etl.transform()

    # Check if the expected number of files are created
    assert len(os.listdir(TRANSFORMED_DIR)) == len(os.listdir(EXTRACTED_DIR))

def test_task_dependencies():
    df = pd.read_csv(SOURCE_CSV)
    df.dropna(axis=0, inplace=True)
    context_data = df['context']

    dag = dags.etl.etl_dag()
    extract_task = dag.get_task("extract")
    transform_task = dag.get_task("transform")
    load_task = dag.get_task("load")

    # Set the execution date to a specific date for testing
    execution_date = datetime(2024, 1, 3)
    
    # Trigger the DAG up to the load task
    dag.clear()
    dag.run(start_date=execution_date, end_date=execution_date)

    # Check that the extract task is executed first
    ti_extract = TaskInstance(task=extract_task, execution_date=execution_date)
    assert ti_extract.is_success()

    # Check that the transform task is executed after the extract task
    ti_transform = TaskInstance(task=transform_task, execution_date=execution_date)
    assert ti_transform.is_success()
    assert ti_transform.get_upstream_failed() == set()

    # Check that the load task is executed after the transform task
    ti_load = TaskInstance(task=load_task, execution_date=execution_date)
    assert ti_load.is_success()
    assert ti_load.get_upstream_failed() == set()

@patch("dags.etl.SqliteHook")
def test_load_task_with_mocked_hook(mock_sqlite_hook):
    # Mocking the SQLiteHook to avoid actual database operations
    mock_hook_instance = Mock(spec=SqliteHook)
    mock_sqlite_hook.return_value = mock_hook_instance

    load_task = dags.etl.load()

    # Execute the task
    load_task.execute(context={})

    # Verify that the SQLiteHook methods are called with the expected arguments
    assert mock_hook_instance.run.call_count == 6  # Number of queries executed

    expected_calls = [
        call("INSERT INTO job (title, industry, description, employment_type, date_posted) VALUES (?, ?, ?, ?, ?)", ANY),
        call("INSERT INTO company (name, link) VALUES (?, ?)", ANY),
        call("INSERT INTO education (required_credential) VALUES (?)", ANY),
        call("INSERT INTO experience (months_of_experience, seniority_level) VALUES (?, ?)", ANY),
        call("INSERT INTO salary (currency, min_value, max_value, unit) VALUES (?, ?, ?, ?)", ANY),
        call("INSERT INTO location (country, locality, region, postal_code, street_address, latitude, longitude) VALUES (?, ?, ?, ?, ?, ?, ?)", ANY),
    ]

    mock_hook_instance.run.assert_has_calls(expected_calls, any_order=True)