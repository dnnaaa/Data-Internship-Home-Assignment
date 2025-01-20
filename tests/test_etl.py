from dags.etl.extract import extract_data
from dags.etl.transform import transform_data
from dags.etl.load import load_data
import pytest
import os
from unittest.mock import patch
import json
import sqlite3
@pytest.fixture
def mock_csv_data():
    """Mock CSV data to simulate reading from jobs.csv."""
    return [
        {"context": '{"job_title": "Software Engineer", "company_name": "TechCorp"}'},
        {"context": '{"job_title": "Data Scientist", "company_name": "DataCorp"}'}
    ]


@patch('your_module.etl.extract.pd.read_csv')  
def test_extract_data(mock_read_csv, mock_csv_data):
    mock_read_csv.return_value = mock_csv_data
    
    extract_data("source/jobs.csv", "staging/extracted")
    
    extracted_files = os.listdir("staging/extracted")
    assert len(extracted_files) == 2
    assert extracted_files[0].startswith("extracted_")  # Vérifier si les fichiers ont le bon format


@pytest.fixture
def mock_extracted_files():
    """Créer un fichier texte simulé pour les tests de transformation."""
    return [
        '{"job_title": "Software Engineer", "company_name": "TechCorp", "industry": "Tech"}',
        '{"job_title": "Data Scientist", "company_name": "DataCorp", "industry": "Data"}'
    ]


@patch('your_module.etl.transform.os.listdir')
@patch('your_module.etl.transform.open')
def test_transform_data(mock_open, mock_listdir, mock_extracted_files):
    mock_listdir.return_value = ['extracted_0.txt', 'extracted_1.txt']
    
    mock_open.return_value.__enter__.return_value.read.side_effect = mock_extracted_files
    
    transform_data("staging/extracted", "staging/transformed")
    
    transformed_files = os.listdir("staging/transformed")
    assert len(transformed_files) == 2
    assert transformed_files[0].endswith(".json")
    
    with open("staging/transformed/transformed_0.txt.json", 'r') as json_file:
        data = json.load(json_file)
        assert data['job']['title'] == 'Software Engineer'


@pytest.fixture
def mock_transformed_files():
    """Simuler des fichiers transformés pour les tests de chargement."""
    return [
        '{"job": {"title": "Software Engineer", "industry": "Tech"}, "company": {"name": "TechCorp", "link": "techcorp.com"}}',
        '{"job": {"title": "Data Scientist", "industry": "Data"}, "company": {"name": "DataCorp", "link": "datacorp.com"}}'
    ]


@patch('your_module.etl.load.sqlite3.connect')
def test_load_data(mock_connect, mock_transformed_files):
    mock_conn = mock_connect.return_value
    mock_cursor = mock_conn.cursor.return_value
    
    for i, transformed_file in enumerate(mock_transformed_files):
        file_path = f"staging/transformed/transformed_{i}.json"
        
        with open(file_path, 'w') as f:
            f.write(transformed_file)

    load_data("staging/transformed", "test.db")
    
    assert mock_cursor.execute.call_count > 0
    mock_cursor.execute.assert_any_call(
        'INSERT INTO job (title, industry, description, employment_type, date_posted) VALUES (?, ?, ?, ?, ?)',
        ('Software Engineer', 'Tech', None, None, None)
    )