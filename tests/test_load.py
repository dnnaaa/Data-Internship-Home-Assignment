import pytest
from unittest.mock import Mock, patch
from etl.load import save_to_database, load_to_database

def test_save_to_database():
    # Mock SQLiteHook and its methods
    mock_hook = Mock()
    mock_hook.get_conn.return_value = Mock()
    mock_cursor = mock_hook.get_conn.return_value.cursor.return_value
    mock_cursor.lastrowid = 1  

    # Mock transformed_data
    transformed_data = {
        "job": {
            "title": "web developer",
            "industry": "IT",
            "description": "exciting opportunities",
            "employment_type": "full time",
            "date_posted": "2024-06-01",
        },
        "company": {
            "name": "ITS",
            "link": "its.com",
        },
        "education": {
            "required_credential": "bac+5",
        },
        "experience": {
            "months_of_experience": 24,
            "seniority_level": "senior",
        },
        "salary": {
            "currency": "mad",
            "min_value": 9000,
            "max_value": 100000,
            "unit": "un",
        },
        "location": {
            "country": "morocco",
            "locality": "locality",
            "region": "tangier",
            "postal_code": "90000",
            "street_address": "boulevard",
            "latitude": 12997,
            "longitude": 1334,
        },
    }

  
    with patch('etl.load.SqliteHook', return_value=mock_hook):
        save_to_database(mock_hook, transformed_data)

    # Assertions
    mock_cursor.execute.assert_called()  # Check if execute method was called
    mock_cursor.execute.assert_any_call("INSERT INTO job (title, industry, description, employment_type, date_posted) VALUES (?, ?, ?, ?, ?)", (
        "web developer",
        "IT",
        "exciting opportunities",
        "full time",
        "2024-06-01",
    ))

# Run the test with pytest
# pytest -v test_your_module.py
