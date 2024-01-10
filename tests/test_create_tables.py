from unittest.mock import MagicMock, patch
from dags.create_table import create_tables, TABLES_CREATION_QUERY

def test_create_tables():
    with patch('dags.create_table.SqliteOperator') as mock_sql_execute_query_operator:
        # Create a mock object for the SqliteOperator instance
        mock_operator_instance = MagicMock()

        # Configure the mock_sql_execute_query_operator to return the mock_operator_instance
        mock_sql_execute_query_operator.return_value = mock_operator_instance

        # Call the function under test
        create_tables()

        # Assert that the SqliteOperator constructor was called once with the expected arguments
        mock_sql_execute_query_operator.assert_called_once_with(
            task_id='create_tables',
            sqlite_conn_id='sqlite_default',
            sql=TABLES_CREATION_QUERY
        )