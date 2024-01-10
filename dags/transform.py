# tasks/transform_task.py
import os
import json
from airflow.decorators import task
from datetime import datetime
from bs4 import BeautifulSoup

@task()
def transform(extracted_dir, transformed_dir):
    """Transform job data and save each item to JSON files."""
    os.makedirs(transformed_dir, exist_ok=True)

    for filename in os.listdir(extracted_dir):
        if filename.endswith(".txt"):
            filepath = os.path.join(extracted_dir, filename)
            with open(filepath, 'r') as file:
                contenu = file.read()
                # Vérifier si le contenu est vide
                if not contenu.strip() or contenu.strip() == "nan":
                print(f"Le fichier {fichier_extrait} est vide. Ignoré.")
                continue
                extracted_data = json.load(contenu)

            cleaned_description = clean_description(extracted_data.get("description", ""))
            transformed_data = {
                "job": {
                    "title": extracted_data.get("title", ""),
                    "industry": extracted_data.get("industry", ""),
                    "description": cleaned_description,
                    "employment_type": extracted_data.get("employmentType", ""),
                    "date_posted": extracted_data.get("datePosted", ""),
                },
                "company": {
                    "name": extracted_data.get("hiringOrganization", {}).get("name", ""),
                    "link": extracted_data.get("hiringOrganization", {}).get("sameAs", ""),
                },
                "education": {
                    "required_credential": extracted_data.get("education", {}).get("required_credential", ""),
                },
                "experience": {
                    "months_of_experience": extracted_data.get("experience", {}).get("months_of_experience", ""),
                    "seniority_level": extracted_data.get("experience", {}).get("seniority_level", ""),
                },
                "salary": {
                    "currency": extracted_data.get("salary", {}).get("currency", ""),
                    "min_value": extracted_data.get("salary", {}).get("min_value", ""),
                    "max_value": extracted_data.get("salary", {}).get("max_value", ""),
                    "unit": extracted_data.get("salary", {}).get("unit", ""),
                },
                "location": {
                    "country": extracted_data.get("jobLocation", {}).get("address", {}).get("addressCountry", ""),
                    "locality": extracted_data.get("jobLocation", {}).get("address", {}).get("addressLocality", ""),
                    "region": extracted_data.get("jobLocation", {}).get("address", {}).get("addressRegion", ""),
                    "postal_code": extracted_data.get("jobLocation", {}).get("address", {}).get("postalCode", ""),
                    "street_address": extracted_data.get("jobLocation", {}).get("address", {}).get("streetAddress", ""),
                    "latitude": extracted_data.get("jobLocation", {}).get("latitude", ""),
                    "longitude": extracted_data.get("jobLocation", {}).get("longitude", ""),
                },
            }

            transformed_filename = f"{datetime.now().strftime('%Y%m%d%H%M%S%f')}.json"
            transformed_filepath = os.path.join(transformed_dir, transformed_filename)

            with open(transformed_filepath, 'w') as file:
                json.dump(transformed_data, file, indent=2)
    
    return transformed_dir

def clean_description(description):
    """Clean job description."""
    soup = BeautifulSoup(description_html, 'html.parser')
        return soup.get_text()
    return soup.get_text()



