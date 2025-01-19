import pandas as pd
import json
from unittest.mock import patch, mock_open
from dags.tasks.extract import extract_data


@patch("dags.tasks.extract.pd.read_csv")
@patch("builtins.open", new_callable=mock_open)
@patch("os.path.join", side_effect=lambda *args: "/".join(args)) 
def test_extract_data(mock_path_join, mock_file, mock_read_csv):

    mock_csv_data = pd.DataFrame({
        'context': [
            '{"title": "Software Engineer", "industry": "Tech", "description": "Develop software.", '
            '"employmentType": "Full-time", "datePosted": "2025-01-01", '
            '"hiringOrganization": {"name": "TechCompany", "sameAs": "https://www.linkedin.com/company/techcompany"}}'
        ]
    })
    mock_read_csv.return_value = mock_csv_data

    extract_data()

    mock_read_csv.assert_called_once_with('/opt/airflow/source/jobs.csv')

    # Vérifier que les fichiers texte ont été écrits
    expected_output = {
        "title": "Software Engineer",
        "industry": "Tech",
        "description": "Develop software.",
        "employmentType": "Full-time",
        "datePosted": "2025-01-01",
        "hiringOrganization": {
            "name": "TechCompany",
            "sameAs": "https://www.linkedin.com/company/techcompany"
        }
    }

    # Extraire le contenu écrit dans le fichier
    written_content = "".join(call.args[0] for call in mock_file().write.call_args_list)

    # Convertir en JSON et comparer
    assert json.loads(written_content) == expected_output, "Le contenu écrit ne correspond pas au contenu attendu."

    print("Test réussi : Les données extraites et écrites sont correctes.")
