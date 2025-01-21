
import os
from unittest.mock import patch, mock_open
from dags.tasks.extract import extract

@patch("builtins.open", new_callable=mock_open, read_data="id,context\n1,{\"job_title\": \"Data Engineer\"}")
@patch("os.makedirs")
def test_extract_task(mock_makedirs, mock_open_file):
    
    source_path = os.path.abspath("source/jobs.csv")
    output_dir = os.path.abspath("staging/extracted")
    
    extract(source_path, output_dir)

    mock_makedirs.assert_called_once_with(output_dir, exist_ok=True)
    mock_open_file.assert_called_once_with(f"{output_dir}/extracted.txt", "w")
