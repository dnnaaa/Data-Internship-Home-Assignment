import pytest
from unittest.mock import MagicMock
from dags.tasks.load import insert_into_job  # Remplacer 'your_module' par le nom de votre module

def test_insert_into_job():
    mock_cursor = MagicMock()
    job_data = {
        'title': 'Engineer',
        'industry': 'Engineering',
        'description': 'Job description here',
        'employment_type': 'Full-time',
        'date_posted': '2024-01-01'
    }
    insert_into_job(mock_cursor, job_data)
    mock_cursor.execute.assert_called_once()
