import os
import pytest
from etl_extract import extract

def test_extract():
    extract()

    assert os.path.exists('staging/extracted')

    assert len(os.listdir('staging/extracted')) > 0
