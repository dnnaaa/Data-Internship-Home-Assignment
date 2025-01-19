from unittest.mock import patch, MagicMock, mock_open
import json
from dags.tasks.load import load_data 
@patch("os.path.exists")
@patch("os.remove")
@patch("sqlite3.connect")
@patch("os.listdir")
@patch("builtins.open", new_callable=mock_open)
def test_load_data(mock_open_func, mock_listdir, mock_connect, mock_remove, mock_exists):
    # Simuler l'existence du fichier de base de données et la suppression si nécessaire
    mock_exists.return_value = True  
    mock_remove.return_value = None  

    # Simuler la connexion SQLite
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    # Simuler un fichier JSON transformé contenant des données pour l'insertion
    mock_file_path = "staging/transformed/0.json"  
    mock_listdir.return_value = [mock_file_path]  

    # Données simulées dans le fichier JSON
    mock_json_data = {
        "job": {
            "title": "Software Engineer",
            "industry": "Tech",
            "description": "Develop software.",
            "employment_type": "Full-time",
            "date_posted": "2025-01-01"
        },
        "company": {
            "name": "TechCompany",
            "link": "https://www.linkedin.com/company/techcompany"
        },
        "education": {
            "required_credential": "Bachelor's Degree"
        },
        "experience": {
            "months_of_experience": 24,
            "seniority_level": "Junior"
        },
        "salary": {
            "currency": "USD",
            "min_value": 60000,
            "max_value": 80000,
            "unit": "Year"
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

    # Configurer le mock pour ouvrir le fichier et retourner les données JSON
    mock_open_func.return_value.read.return_value = json.dumps(mock_json_data)

    load_data()

