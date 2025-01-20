# tests/test_transform.py
import pytest
from unittest.mock import mock_open, patch
import json
from dags.transform import JobsTransformer

class TestJobsTransformer:
    def test_transform_job_data(self, sample_job_data):
        transformer = JobsTransformer()
        transformed = transformer._transform_job_data(sample_job_data)
        
        assert transformed["job"]["title"] == "Software Engineer"
        assert transformed["company"]["name"] == "TechCorp"
        assert transformed["education"]["required_credential"] == "Bachelor's Degree"
        assert transformed["experience"]["months_of_experience"] == 36
        assert transformed["salary"]["min_value"] == 80000
        assert transformed["location"]["country"] == "USA"

    def test_transform_missing_data(self):
        minimal_data = {
            "title": "Developer",
            "hiringOrganization": {"name": "Company"}
        }
        
        transformer = JobsTransformer()
        transformed = transformer._transform_job_data(minimal_data)
        
        assert transformed["job"]["title"] == "Developer"
        assert transformed["job"]["industry"] == ""
        assert transformed["company"]["name"] == "Company"
        assert transformed["experience"]["months_of_experience"] == 0

    def test_transform_full_pipeline(self, sample_job_data, setup_staging_directories):
        with patch('os.listdir') as mock_listdir, \
             patch('builtins.open', mock_open(read_data=json.dumps(sample_job_data))):
            
            mock_listdir.return_value = ['job_1.json']
            transformer = JobsTransformer()
            result = transformer.transform()
            assert result == 1
