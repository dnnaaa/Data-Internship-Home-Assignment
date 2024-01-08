import os
import pytest
from Extracting import extract

def test_extract_creates_files():
    # Setup: Define input CSV and output directory
    input_csv = 'source/jobs.csv'
    output_dir = 'test_staging'

    # Run the extract task
    extract(input_csv, output_dir)

    # Assert: Check if files are created in the output directory
    assert os.path.exists(output_dir)
    assert len(os.listdir(output_dir)) > 0

    # Teardown: Clean up the created files/directory
    for file in os.listdir(output_dir):
        os.remove(os.path.join(output_dir, file))
    os.rmdir(output_dir)