import os
import pytest
from airflow.models import DagBag

def test_etl_dag_loads_successfully():
    dagbag = DagBag()
    dag_id = 'etl_dag'
    assert dag_id in dagbag.dags

def test_etl_dag_tasks():
    dagbag = DagBag()
    dag_id = 'etl_dag'
    dag = dagbag.dags[dag_id]
    assert dag

    task_ids = ['create_tables', 'extract', 'transform', 'load']
    for task_id in task_ids:
        assert task_id in dag.task_ids
