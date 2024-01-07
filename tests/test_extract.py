import pytest
from unittest.mock import patch, mock_open
from dags.tasks.extract import extract

@patch("builtins.open", new_callable=mock_open, read_data="data")
@patch("os.makedirs")
def test_extract(mock_makedirs, mock_file):
    source_file = "source.csv"
    staging_dir = "staging"
    extract(source_file, staging_dir)
    mock_file.assert_called_with("staging/extracted.txt", 'w')
