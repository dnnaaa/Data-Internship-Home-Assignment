import pytest
from unittest.mock import MagicMock
from dags.tasks.load import insert_into_table

def test_insert_into_table():
    mock_cursor = MagicMock()
    query = "INSERT INTO job (title, industry) VALUES (?, ?)"
    data = ("Data Scientist", "Informatique")
    insert_into_table(mock_cursor, query, data)
    mock_cursor.execute.assert_called_with(query, data)