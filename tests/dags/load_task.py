import pytest
from unittest.mock import MagicMock, patch
from tasks.load import load_task

def test_load_task(tmpdir):
    # Mock data
    json_content = '''
    {
        "job": {
            "title": "Software Engineer",
            "industry": "IT",
            "description": "Develop software",
            "employment_type": "FULL_TIME",
            "date_posted": "2021-01-01"
        },
        "company": {
            "name": "Tech Corp",
            "link": "https://techcorp.com"
        },
        "education": {
            "required_credential": "Bachelor's Degree"
        },
        "experience": {
            "months_of_experience": 60,
            "seniority_level": "Senior"
        },
        "salary": {
            "currency": "USD",
            "min_value": 100000,
            "max_value": 150000,
            "unit": "YEAR"
        },
        "location": {
            "country": "US",
            "locality": "New York",
            "region": "NY",
            "postal_code": "10001",
            "street_address": "123 Main St",
            "latitude": 40.7128,
            "longitude": -74.0060
        }
    }
    '''
    input_dir = str(tmpdir)
    input_file = tmpdir.join("transformed_context_0.txt")
    input_file.write(json_content)


    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = [1]  

    with patch("airflow.providers.postgres.hooks.postgres.PostgresHook.get_conn", return_value=mock_conn):
        mock_conn.cursor.return_value = mock_cursor
        load_task()

        
        mock_cursor.execute.assert_any_call(
            """
            INSERT INTO job (title, industry, description, employment_type, date_posted)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id;
            """,
            ("Software Engineer", "IT", "Develop software", "FULL_TIME", "2021-01-01"),
        )
        mock_cursor.execute.assert_any_call(
            """
            INSERT INTO company (job_id, name, link)
            VALUES (%s, %s, %s)
            """,
            (1, "Tech Corp", "https://techcorp.com"),
        )
        mock_conn.commit.assert_called_once()