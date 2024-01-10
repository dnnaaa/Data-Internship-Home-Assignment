import json
import os
from unittest.mock import Mock, patch

import pytest
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

from your_module import load_to_database  # Assuming your function is in 'your_module.py'

@pytest.fixture
def mock_sqlite_hook():
    """Fixture for mocking SqliteHook."""
    hook = Mock(spec=SqliteHook)
    conn = Mock()
    cursor = Mock()
    conn.cursor.return_value = cursor
    hook.get_conn.return_value = conn
    return hook, conn, cursor

def test_load_to_database(mock_sqlite_hook):
    hook, conn, cursor = mock_sqlite_hook

    # Mock the SqliteHook to use the fixture
    with patch('your_module.SqliteHook', return_value=hook):

        # Mock os.listdir to return a list of filenames
        with patch('os.listdir', return_value=['file1.json', 'file2.json']):
            # Mock os.path.join to return the filepath
            with patch('os.path.join', side_effect=lambda dir, file: f'{dir}/{file}'):
                # Mock the open function to return a mock file object
                with patch('builtins.open', mock_open(read_data=json.dumps({
                    "job": {"title": "Test Job", "industry": "Tech", "description": "Test Desc", "employment_type": "Full-time", "date_posted": "2023-01-01"},
                    "company": {"name": "Test Company", "link": "http://testcompany.com"},
                    "education": {"required_credential": "Bachelor's Degree"},
                    "experience": {"months_of_experience": 24, "seniority_level": "Mid"},
                    "salary": {"currency": "USD", "min_value": 50000, "max_value": 70000, "unit": "yearly"},
                    "location": {"country": "USA", "locality": "New York", "region": "NY", "postal_code": "10001", "street_address": "123 Test St", "latitude": 40.7128, "longitude": -74.0060}
                }))):
                    # Call the function
                    load_to_database('test_dir')

    # Assert that the cursor.execute method was called the correct number of times
    assert cursor.execute.call_count == 6

    # Assert that the commit method was called once
    conn.commit.assert_called_once()

    # Assert that the cursor and connection are closed
    cursor.close.assert_called_once()
    conn.close.assert_called_once()
