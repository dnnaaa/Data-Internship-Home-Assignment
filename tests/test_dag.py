from unittest import TestCase
from airflow.models import DagBag
import logging

class TestDagsLoading(TestCase):

    def test_dags_loading_errors(self):
        dag_bag = DagBag()
        self.assertEqual(dag_bag.import_errors, {})
        logging.info("No loading dags errors!")
        for dag in dag_bag.dags.values():
            assert len(dag.tasks) != 0
