import os
import pandas as pd
import pytest
from dags.extract import extract

def test_extract(tmpdir):
    csv_file = tmpdir.join('test.csv')
    csv_file.write('id,context\n1,Hello\n2,World\n')


    result = extract(csv_file)

    assert isinstance(result, list)

    expected_files = ['1.txt', '2.txt']
    for file_name in expected_files:
        file_path = os.path.join('staging/extracted', file_name)
        assert os.path.isfile(file_path)

        with open(file_path, 'r') as file:
            content = file.read().strip()

        assert content in ['Hello', 'World']