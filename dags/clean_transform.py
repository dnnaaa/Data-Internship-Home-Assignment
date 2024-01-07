import os
import pandas as pd
import json

def clean_transform(**kwargs):

    extracted_path = './staging/extracted'
    transformed_path = './staging/transformed'

    for filename in os.listdir(extracted_path):
        with open(os.path.join(extracted_path, filename), 'r') as file:
            extracted_data = json.load(file)

        transformed_data = {
            "job": {
                "title": extracted_data.get('title', ''),
                "industry": extracted_data.get('industry', ''),
                "description": extracted_data.get('description', ''),
                "employment_type": extracted_data.get('employment_type', ''),
                "date_posted": extracted_data.get('date_posted', ''),
            },
            "company": {
                "name": extracted_data.get('company_name', ''),
                "link": extracted_data.get('company_linkedin_link', ''),
            },
            "education": {
                "required_credential": extracted_data.get('job_required_credential', ''),
            },
            "experience": {
                "months_of_experience": extracted_data.get('job_months_of_experience', ''),
                "seniority_level": extracted_data.get('seniority_level', ''),
            },
            "salary": {
                "currency": extracted_data.get('salary_currency', ''),
                "min_value": extracted_data.get('salary_min_value', ''),
                "max_value": extracted_data.get('salary_max_value', ''),
                "unit": extracted_data.get('salary_unit', ''),
            },
            "location": {
                "country": extracted_data.get('country', ''),
                "locality": extracted_data.get('locality', ''),
                "region": extracted_data.get('region', ''),
                "postal_code": extracted_data.get('postal_code', ''),
                "street_address": extracted_data.get('street_address', ''),
                "latitude": extracted_data.get('latitude', ''),
                "longitude": extracted_data.get('longitude', ''),
            },
        }

        with open(f'{transformed_path}/transformed_{filename[:-4]}.json', 'w') as file:
            json.dump(transformed_data, file)