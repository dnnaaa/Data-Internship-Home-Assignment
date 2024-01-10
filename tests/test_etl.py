from dags.create_tables import create_tables
from datetime import datetime
from airflow.models import DagBag
import pytest
from airflow import DAG
from airflow.decorators import dag, task
from dags.etl import etl_dag


@pytest.mark.parametrize("dag_id", ["etl_dag"])
def test_etl_dag(dag_id):
    dagbag = DagBag()
    assert dagbag.get_dag(dag_id) is not None
    
    dag = dagbag.dags.get("etl_dag")
    assert dag.description == "ETL LinkedIn job posts"
    assert len(dag.tasks) == 4

    task_ids = [task.task_id for task in dag.tasks]
    assert task_ids[0] == "create_tables"
    assert task_ids[1] == "extract"
    assert task_ids[2] == "transform"
    assert task_ids[3] == "load"

    assert dag.get_task("create_tables").downstream_task_ids == {"extract"}
    assert dag.get_task("extract").upstream_task_ids == {"create_tables"}
    assert dag.get_task("extract").downstream_task_ids == {"transform"}
    assert dag.get_task("transform").upstream_task_ids == {"extract"}
    assert dag.get_task("transform").downstream_task_ids == {"load"}
    assert dag.get_task("load").upstream_task_ids == {"transform"}

    assert dag.schedule_interval == "@daily"
    assert dag.start_date == datetime(2024, 1, 2)
    
pytest.main(["-v"])