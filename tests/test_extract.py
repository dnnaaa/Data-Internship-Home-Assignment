import pytest
import pandas as pd
import os
from unittest.mock import patch, mock_open
from dags.extract import extract  

@patch("pandas.read_csv")
@patch("os.makedirs")
@patch("builtins.open", new_callable=mock_open)
def test_extract(mock_open, mock_makedirs, mock_read_csv):
    # Données factices simulant le CSV
    mock_df = pd.DataFrame({"context": ['{"job": "Engineer"}', '{"job": "Developer"}']})
    mock_read_csv.return_value = mock_df

    # Exécuter la fonction d'extraction
    extract()

    # Vérification que le répertoire est créé
    mock_makedirs.assert_called_once_with("/opt/airflow/data/staging/extracted", exist_ok=True)

    # Vérification que les fichiers sont créés avec le bon contenu
    expected_calls = [
        ("/opt/airflow/data/staging/extracted/0.txt", '{"job": "Engineer"}\n'),
        ("/opt/airflow/data/staging/extracted/1.txt", '{"job": "Developer"}\n'),
    ]
    handles = mock_open.return_value.__enter__.return_value
    calls = [call.args for call in handles.write.call_args_list]
    
    assert calls == [(expected[1],) for expected in expected_calls]

if __name__ == "__main__":
    pytest.main()