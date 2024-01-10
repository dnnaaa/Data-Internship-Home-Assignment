from airflow.models import DagBag


def test_dag_loading():
    dag_bag = DagBag()
    assert len(dag_bag.import_errors) == 0, 'No DAG loading errors'