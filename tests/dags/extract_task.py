import pytest
from unittest.mock import mock_open, patch
from tasks.extract import extract_task

def test_extract_task(tmpdir):
    
    csv_content = "context\n{\"title\": \"Software Engineer\"}\n{\"title\": \"Data Scientist\"}"
    input_path = "/fake/path/jobs.csv"
    output_dir = str(tmpdir)  
   
    with patch("builtins.open", mock_open(read_data=csv_content)) as mock_file:
        extract_task()

        
        mock_file.assert_any_call(input_path, "r")
        mock_file.assert_any_call(f"{output_dir}/context_0.txt", "w")
        mock_file.assert_any_call(f"{output_dir}/context_1.txt", "w")

        # Verify the content written to the output files
        mock_file().write.assert_any_call("{\"title\": \"Software Engineer\"}")
        mock_file().write.assert_any_call("{\"title\": \"Data Scientist\"}")