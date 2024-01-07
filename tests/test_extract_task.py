import pytest
from unittest.mock import mock_open, patch
from dags.etl import extract_task  
import pandas as pd

# Define test data
csv_data = "context\nData 1\nData 2\n"

# Define a test
def test_extract_task(tmp_path):
    # Use the pytest.fixture decorator to create a temporary file
    csv_path = tmp_path / "jobs.csv"
    csv_path.write_text(csv_data)

    # Setup: Create a mock of the open function to simulate file writing
    m = mock_open()

    # Use the patch decorator to replace the open function with the mock
    with patch('builtins.open', m):
        # Mock pandas read_csv method to return a DataFrame from  CSV data
        with patch('pandas.read_csv', return_value=pd.read_csv(StringIO(csv_data))):
            # Call your function
            extract_task()

            # Assertion: Check that the open function was called with the correct file name and mode
            m.assert_called_with('staging/extracted/0.txt', 'w')

            # For example, i check if the temporary file was created in the staging/extracted directory
            assert (tmp_path / "staging" / "extracted" / "0.txt").exists()
