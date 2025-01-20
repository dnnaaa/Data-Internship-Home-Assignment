import json
import os

def transform_data(input_directory, output_directory):
    """Transform extracted data into JSON and save to transformed directory."""
    
    # Créer le répertoire de sortie pour les fichiers transformés s'il n'existe pas
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # Itérer sur les fichiers extraits dans le répertoire d'entrée
    for file_name in os.listdir(input_directory):
        file_path = os.path.join(input_directory, file_name)
        
        # Lire le fichier texte
        with open(file_path, 'r') as file:
            context = json.loads(file.read())  # Charger le texte comme JSON
        
        # Transformer les données dans le format souhaité
        transformed_data = {
            "job": {
                "title": context.get("job_title"),
                "industry": context.get("industry"),
                "description": context.get("job_description"),
                "employment_type": context.get("employment_type"),
                "date_posted": context.get("date_posted"),
            },
            "company": {
                "name": context.get("company_name"),
                "link": context.get("company_link"),
            },
            "education": {
                "required_credential": context.get("required_credential"),
            },
            "experience": {
                "months_of_experience": context.get("months_of_experience"),
                "seniority_level": context.get("seniority_level"),
            },
            "salary": {
                "currency": context.get("salary_currency"),
                "min_value": context.get("salary_min_value"),
                "max_value": context.get("salary_max_value"),
                "unit": context.get("salary_unit"),
            },
            "location": {
                "country": context.get("country"),
                "locality": context.get("locality"),
                "region": context.get("region"),
                "postal_code": context.get("postal_code"),
                "street_address": context.get("street_address"),
                "latitude": context.get("latitude"),
                "longitude": context.get("longitude"),
            },
        }

        # Sauvegarder les données transformées dans un fichier JSON
        output_file_path = os.path.join(output_directory, f'transformed_{file_name}.json')
        with open(output_file_path, 'w') as json_file:
            json.dump(transformed_data, json_file, indent=4)
