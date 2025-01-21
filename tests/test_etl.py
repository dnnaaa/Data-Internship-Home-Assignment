import os
import pytest
from tasks.extract import extract_job

@pytest.fixture
def setup_environment(tmp_path):
    os.makedirs(tmp_path / 'source')
    with open(tmp_path / 'source/jobs.csv', 'w') as file:
        file.write('context\n{"job_title": "Engineer"}\n')
    return tmp_path

def test_extract_job(setup_environment):
    os.chdir(setup_environment)
    extract_job()
    extracted_files = os.listdir('staging/extracted')
    assert len(extracted_files) == 1
