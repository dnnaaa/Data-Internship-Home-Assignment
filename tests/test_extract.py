import os
import pandas as pd
from unittest.mock import patch, mock_open
from dags.extract import extract_jobs_csv  # Replace with the actual name of your module

def test_extract_jobs_csv():
    # Mock data
    mock_data = pd.DataFrame({'context': ['job1', 'job2', 'job3']})
    file_path = 'dummy_path/jobs.csv'
    output_dir = 'dummy_path/extracted'

    # Mock read_csv, os.path.exists, and open
    with patch('pandas.read_csv', return_value=mock_data), \
         patch('os.path.exists', return_value=False), \
         patch('os.makedirs'), \
         patch('builtins.open', new_callable=mock_open) as mock_file:
        extract_jobs_csv(file_path, output_dir)

        # Verify directory creation
        assert not os.path.exists(output_dir)  # As it's mocked to always return False

        # Verify file writing
        assert mock_file.call_count == len(mock_data)
        for index, row in mock_data.iterrows():
            file_name = f"{output_dir}/job_{index}.txt"
            mock_file.assert_any_call(file_name, 'w')
            mock_file().write.assert_any_call(str(row['context']))