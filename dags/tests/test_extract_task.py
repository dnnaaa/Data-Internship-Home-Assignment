import pytest
import json
from unittest.mock import mock_open, patch
from extract_task import extract_jobs

# test case 1: successful data extraction
def test_extract_jobs_success():
    
    sample_data = "context\n" + json.dumps({"key": "value"}) + "\n"

    # mock the open function to simulate file reading and writing
    mock_file_open = mock_open(read_data=sample_data)

    with patch("builtins.open", mock_file_open):
        with patch("os.path.exists", return_value=True):
            with patch("os.makedirs"):
                extract_jobs()

    mock_file_open.assert_called()  
    handle = mock_file_open()
    handle.write.assert_called_with(sample_data)  # check if data was written correctly

# test Case 2: missing 'context' column
def test_extract_jobs_missing_context_column():
    
    sample_data = "incorrect_column\nvalue\n"

    mock_file_open = mock_open(read_data=sample_data)

    with patch("builtins.open", mock_file_open), \
         pytest.raises(KeyError) as excinfo:
        extract_jobs()

    assert "Column 'context' not found" in str(excinfo.value) 
