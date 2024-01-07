import pytest
from unittest.mock import MagicMock, patch
from dags.tasks.load import load


@patch("sqlite3.connect")
def test_load(mock_connect):
    transformed_dir = "transformed"
    db_file = "db.sqlite"
    mock_connect.return_value.__enter__.return_value.cursor.return_value = MagicMock()
    load(transformed_dir, db_file)
