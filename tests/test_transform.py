from unittest import TestCase
from unittest.mock import patch, mock_open
from pathlib import Path
from dags.tasks.transform import transform_data  

class TestTransformData(TestCase):
    @patch('builtins.open', new_callable=mock_open, read_data='{"title": "Job Title", "description": "<p>Some description</p>", "employmentType": "Full-time", "datePosted": "2025-01-01", "hiringOrganization": {"name": "Company Name", "sameAs": "https://company.com"}}')
    @patch('pathlib.Path.glob', return_value=[Path('/opt/airflow/staging/extracted/0.txt')])  
    def test_transform_data(self, mock_glob, mock_open):

        transform_data()

        extracted_file_path = Path('/opt/airflow/staging/extracted/0.txt')
        transformed_file_path = Path('/opt/airflow/staging/transformed/0.json')

        # Vérifier que le fichier d'extraction a été ouvert en mode lecture
        mock_open.assert_any_call(extracted_file_path, 'r', encoding='utf-8')

        # Vérifier que le fichier transformé a été ouvert en mode écriture
        mock_open.assert_any_call(transformed_file_path, 'w', encoding='utf-8')
