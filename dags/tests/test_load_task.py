from unittest.mock import patch, MagicMock
from load_task import load_data_into_db

@patch('sqlite3.connect')
def test_load_data_into_db(mocked_connect):
    mocked_cursor = MagicMock()
    mocked_connect.return_value.cursor.return_value = mocked_cursor

    load_data_into_db()

    mocked_cursor.execute.assert_called()  # check if execute was called
    mocked_cursor.commit.assert_called()