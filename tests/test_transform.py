import pytest
from unittest.mock import patch, mock_open
from dags.tasks.transform import transform
import os

@patch("builtins.open", new_callable=mock_open)
@patch("os.listdir", return_value=["file1.txt"])
def test_transform(mock_listdir, mock_file):
    source_dir = os.path.join(os.path.dirname(__file__), '..', 'staging', 'extracted')
    target_dir = os.path.join(os.path.dirname(__file__), '..', 'staging', 'transformed')
    transform(source_dir, target_dir)

    mock_listdir.assert_called_with(source_dir)
    assert mock_open.call_count == 2
    mock_open.assert_any_call(os.path.join(source_dir, "file1.txt"), 'r', encoding='utf-8')
    mock_open.assert_any_call(os.path.join(target_dir, "transformed"), 'w', encoding='utf-8')

