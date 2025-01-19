import json
import os
from pathlib import Path
from bs4 import BeautifulSoup

def clean_description(description):

    # Suppression des balises HTML pour ne conserver que le texte brut
    soup = BeautifulSoup(description, 'html.parser')
    cleaned_description = soup.get_text()

    # Décodage des caractères spéciaux Unicode
    return cleaned_description.encode('utf-8').decode('unicode_escape')

def transform_data():

    # Dossier source des fichiers extraits
    extracted_dir = Path('/opt/airflow/staging/extracted')
    # Dossier de destination pour les fichiers transformés
    transformed_dir = Path('/opt/airflow/staging/transformed')

    # Créer le dossier de destination s'il n'existe pas
    transformed_dir.mkdir(parents=True, exist_ok=True)

    # Vérifier si des fichiers existent 
    if not any(extracted_dir.glob('*.txt')):
        raise FileNotFoundError("Aucun fichier extrait trouvé dans le dossier 'extracted'.")

    # Effectuer la transformation
    for file_path in extracted_dir.glob('*.txt'):
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)

            transformed_data = {
                "job": {
                    "title": data.get("title", ""),
                    "industry": data.get("industry", ""),
                    "description": clean_description(data.get("description", "")),  # Fonction pour nettoyer le texte
                    "employment_type": data.get("employmentType", ""),
                    "date_posted": data.get("datePosted", ""),
                },
                "company": {
                    "name": data.get("hiringOrganization", {}).get("name", ""),
                    "link": data.get("hiringOrganization", {}).get("sameAs", ""),
                },
                "education": {
                    "required_credential": data.get("educationRequirements", {}).get("credentialCategory", ""),
                },
                "experience": {
                    "months_of_experience": data.get("experienceRequirements", {}).get("monthsOfExperience", ""),
                    "seniority_level": data.get("experienceRequirements", {}).get("seniorityLevel", ""),  # Si cette clé est disponible
                },
                "salary": {
                    "currency": data.get("estimatedSalary", {}).get("currency", ""),
                    "min_value": data.get("estimatedSalary", {}).get("value", {}).get("minValue", ""),
                    "max_value": data.get("estimatedSalary", {}).get("value", {}).get("maxValue", ""),
                    "unit": data.get("estimatedSalary", {}).get("value", {}).get("unitText", ""),
                },
                "location": {
                    "country": data.get("jobLocation", {}).get("address", {}).get("addressCountry", ""),
                    "locality": data.get("jobLocation", {}).get("address", {}).get("addressLocality", ""),
                    "region": data.get("jobLocation", {}).get("address", {}).get("addressRegion", ""),
                    "postal_code": data.get("jobLocation", {}).get("address", {}).get("postalCode", ""),
                    "street_address": data.get("jobLocation", {}).get("address", {}).get("streetAddress", ""),
                    "latitude": data.get("jobLocation", {}).get("latitude", ""),
                    "longitude": data.get("jobLocation", {}).get("longitude", ""),
                },
            }

            # Sauvegarder le fichier transformé
            transformed_file_path = transformed_dir / f"{file_path.stem}.json"
            with open(transformed_file_path, 'w', encoding='utf-8') as transformed_file:
                json.dump(transformed_data, transformed_file, indent=4, ensure_ascii=False)

            print(f"Transformation réussie pour {file_path.stem}")

        except json.JSONDecodeError:
            print(f"Erreur de décodage JSON dans le fichier {file_path}.")
            continue  

        except Exception as e:
            print(f"Erreur lors du traitement du fichier {file_path}: {str(e)}")
            continue  
    print("Transformation terminée. Les fichiers transformés ont été enregistrés.")
