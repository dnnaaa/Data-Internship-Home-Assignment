import pytest
import json
import os
import sqlite3
from unittest.mock import patch, MagicMock, mock_open
from dags.load import load  
@pytest.fixture
def sample_json_data():
    return json.dumps({
        "job": {
            "title": "Senior Data Engineer",
            "industry": "Broadcast Media",
            "description": "A great job opportunity...",
            "employment_type": "FULL_TIME",
            "date_posted": "2021-07-09T17:39:28.000Z"
        },
        "company": {
            "name": "CyberCoders",
            "link": "https://www.linkedin.com/company/cybercoders"
        },
        "education": {
            "required_credential": "bachelor degree"
        },
        "experience": {
            "months_of_experience": 60,
            "seniority_level": "Senior"
        },
        "salary": {
            "currency": "$",
            "min_value": 120000,
            "max_value": 150000,
            "unit": "YEAR"
        },
        "location": {
            "country": "US",
            "locality": "Alexandria",
            "region": "VA",
            "postal_code": "22336",
            "street_address": "123 Main St",
            "latitude": 38.804665,
            "longitude": -77.04362
        }
    })

@patch("os.listdir", return_value=["0.json"])
@patch("builtins.open", new_callable=mock_open)
@patch("sqlite3.connect")
@patch("os.path.exists", return_value=True)
@patch("os.access", return_value=True)
def test_load(mock_access, mock_exists, mock_sqlite, mock_open, mock_listdir, sample_json_data):
    # Simuler la lecture du fichier JSON
    mock_open().read.return_value = sample_json_data

    # Simuler une connexion SQLite
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_sqlite.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    # Exécuter la fonction load()
    load()

    # Vérifier que la base SQLite est bien connectée
    mock_sqlite.assert_called_once_with('/opt/airflow/data/jobs.db')

    # Vérifier que les insertions SQL sont bien appelées
    assert mock_cursor.execute.call_count >= 6  # Vérifie qu'au moins 6 requêtes INSERT sont faites(les 6 tables)

    # Vérifier que commit() est bien appelé
    mock_conn.commit.assert_called_once()

    # Vérifier que close() est bien appelé à la fin
    mock_conn.close.assert_called_once()
