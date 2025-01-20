# tests/test_load.py
import pytest
from unittest.mock import mock_open, patch
import json
from dags.load import SqliteLoader

class TestSqliteLoader:
    def test_load_job_data(self, mock_sqlite_hook):
        with patch('airflow.providers.sqlite.hooks.sqlite.SqliteHook', return_value=mock_sqlite_hook):
            loader = SqliteLoader()
            transformed_data = {
                "job": {
                    "title": "Software Engineer",
                    "industry": "Technology",
                    "description": "Python developer",
                    "employment_type": "FULL_TIME",
                    "date_posted": "2024-01-20"
                },
                "company": {"name": "TechCorp", "link": "https://linkedin.com/company/techcorp"},
                "education": {"required_credential": "Bachelor's"},
                "experience": {"months_of_experience": 36, "seniority_level": "MID_LEVEL"},
                "salary": {
                    "currency": "USD",
                    "min_value": 80000,
                    "max_value": 120000,
                    "unit": "YEAR"
                },
                "location": {
                    "country": "USA",
                    "locality": "San Francisco",
                    "region": "CA",
                    "postal_code": "94105",
                    "street_address": "123 Tech St",
                    "latitude": 37.7749,
                    "longitude": -122.4194
                }
            }
            
            loader._insert_job_data(transformed_data)
            assert mock_sqlite_hook.run.call_count == 6  # One call for each table

    def test_load_full_pipeline(self, sample_job_data, mock_sqlite_hook, setup_staging_directories):
        with patch('airflow.providers.sqlite.hooks.sqlite.SqliteHook', return_value=mock_sqlite_hook), \
             patch('os.listdir') as mock_listdir, \
             patch('builtins.open', mock_open(read_data=json.dumps(sample_job_data))):
            
            mock_listdir.return_value = ['job_1.json']
            loader = SqliteLoader()
            result = loader.load()
            assert result == 1

    def test_load_invalid_data(self, mock_sqlite_hook):
        with patch('airflow.providers.sqlite.hooks.sqlite.SqliteHook', return_value=mock_sqlite_hook):
            mock_sqlite_hook.run.side_effect = Exception("Database error")
            loader = SqliteLoader()
            
            with pytest.raises(Exception) as exc_info:
                loader._insert_job_data({
                    "job": {},
                    "company": {},
                    "education": {},
                    "experience": {},
                    "salary": {},
                    "location": {}
                })
            assert "Database error" in str(exc_info.value)
