# tests/test_extract.py
import pytest
from unittest.mock import mock_open, patch
import pandas as pd
import json
from dags.extract import JobsExtractor

class TestJobsExtractor:
    @pytest.fixture
    def mock_csv_data(self, sample_job_data):
        return pd.DataFrame({
            'context': [json.dumps(sample_job_data)]
        })

    def test_extract(self, mock_csv_data, setup_staging_directories):
        with patch('pandas.read_csv', return_value=mock_csv_data), \
             patch('builtins.open', mock_open()):
            extractor = JobsExtractor()
            result = extractor.extract()
            assert result == 1  # One record processed

    def test_extract_empty_file(self, setup_staging_directories):
        empty_df = pd.DataFrame({'context': []})
        with patch('pandas.read_csv', return_value=empty_df):
            extractor = JobsExtractor()
            result = extractor.extract()
            assert result == 0

    def test_extract_invalid_json(self, setup_staging_directories):
        invalid_df = pd.DataFrame({'context': ['{invalid json}']})
        with patch('pandas.read_csv', return_value=invalid_df):
            extractor = JobsExtractor()
            with pytest.raises(Exception):
                extractor.extract()
