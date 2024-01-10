import pandas as pd
import pytest
from extract import extract
from transform import transform
from load import load


"""
how to run unitests in main.py (dags, etl.py)
"""

@pytest.fixture
def sample_data_file(tmp_path):
    file_path = tmp_path / "sample_data.csv"
    # create folder before writing csv
    # define a sample data as a df (a row from context and write it to a csv) using df.to_csv()
    return file_path

def test_extract(sample_data_file):
    expected_data = "use the same sample data from sample_data_file"
    actual_data = extract(sample_data_file)
    assert actual_data == expected_data

def test_transform():
    data = "use the same sample data from sample_data_file"
    prefix = 'user'
    expected_transformed_data = "define sample transformed version"
    actual_transformed_data = transform(data, prefix)
    assert actual_transformed_data == expected_transformed_data

def test_load():
    transformed_data = "use the sample transformed version"
    conn = "defne conn using our function"
    
    load(transformed_data, conn)

    actual_data = "read data from db: select query"
    expected_data = "define sample data from db"
    # ex for expected_data: (1, job_tilte_value, field2_value...)
    assert actual_data == expected_data
