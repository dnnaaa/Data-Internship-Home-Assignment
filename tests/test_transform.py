import pytest
import json
import os
from unittest.mock import patch, mock_open
from dags.transform import transform  

@pytest.fixture
def sample_txt_data():
    return '''{"@context": "http://schema.org", "@type": "JobPosting", "datePosted": "2021-07-09T17:39:28.000Z", "description": "Job Title: Senior Data Engineer&lt;br&gt;Location: Alexandria, VA&lt;br&gt;Salary Range: $120k - $150k&lt;br&gt;Requirements: ETL/ELT, SQL, AWS/Google Cloud, Linux/Unix, Spark (preferred), NoSQL (preferred), Machine Learning concepts (preferred)&lt;br&gt;", "employmentType": "FULL_TIME", "hiringOrganization": {"@type": "Organization", "name": "CyberCoders", "sameAs": "https://www.linkedin.com/company/cybercoders"}, "industry": "Broadcast Media", "title": "Senior Data Engineer", "jobLocation": {"@type": "Place", "address": {"@type": "PostalAddress", "addressCountry": "US", "addressLocality": "Alexandria", "addressRegion": "VA", "postalCode": "22336"}, "latitude": 38.804665, "longitude": -77.04362}, "educationRequirements": {"@type": "EducationalOccupationalCredential", "credentialCategory": "bachelor degree"}, "experienceRequirements": {"@type": "OccupationalExperienceRequirements", "monthsOfExperience": 60}}'''

@patch("os.listdir", return_value=["0.txt"])
@patch("builtins.open", new_callable=mock_open, read_data='{"description": "Job Title: Senior Data Engineer&lt;br&gt;Salary Range: $120k - $150k"}')
@patch("os.makedirs")
def test_transform(mock_makedirs, mock_open, mock_listdir, sample_txt_data):
    # Simuler la lecture du fichier 0.txt
    mock_open().read.return_value = sample_txt_data

    # Exécuter la transformation
    transform()

    # Vérifier que le dossier de sortie est créé
    mock_makedirs.assert_called_once_with('/opt/airflow/data/staging/transformed', exist_ok=True)

    # Vérifier que le fichier JSON a été écrit
    mock_open.assert_any_call('/opt/airflow/data/staging/transformed/0.json', 'w')

    # Vérifier que les données transformées sont correctes
    written_data = json.loads(mock_open().write.call_args[0][0])  # Récupérer l'argument JSON écrit

    assert written_data["job"]["title"] == "Senior Data Engineer"
    assert written_data["job"]["industry"] == "Broadcast Media"
    assert written_data["job"]["employment_type"] == "FULL_TIME"
    assert written_data["experience"]["seniority_level"] == "Senior"
    assert written_data["salary"]["min_value"] == 120000
    assert written_data["salary"]["max_value"] == 150000
    assert written_data["location"]["locality"] == "Alexandria"
