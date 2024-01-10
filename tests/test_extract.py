import pytest

import pandas as pd

# fuction runs before the tests
@pytest.fixture
def df():
    source_file_path = 'source/jobs.csv'
    df = pd.read_csv(source_file_path)
    return df

# check if column exists
def test_col_exists(df):
    name = "context"
    assert name in df.columns