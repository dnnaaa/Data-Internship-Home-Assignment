
import pandas as pd
import os
import json
import sqlite3
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@task()
def transform():
    """Clean and convert extracted elements to json."""
    
    extracted_files = os.listdir('staging/extracted')
    transformed_data = []

    for file_name in extracted_files:
        with open(f'staging/extracted/{file_name}', 'r',encoding='utf-8') as f:
            data = f.read()

            json_data = json.loads(data)

            transformed_data.append({
                "job": {
                    "title": json_data.get("title"),
                    "industry": json_data.get("industry"),
                    "description": clean_description(json_data.get("description")),
                    "employment_type": json_data.get("employmentType"),
                    "date_posted": json_data.get("datePosted"),
                },
                "company": {
                    "name": json_data.get("hiringOrganization",{}).get("name"),
                    "link": json_data.get("hiringOrganization",{}).get("sameAs"),
                },
                "education": {
                    "required_credential": json_data.get("educationRequirements",{}).get("credentialCategory"),
                },
                "experience": {
                    "months_of_experience": None,
                    "seniority_level": None,
                },
                "salary": {
                    "currency": json_data.get("estimatedSalary",{}).get("currency"),
                    "min_value": json_data.get("estimatedSalary",{}).get("value",{}).get("minValue"),
                    "max_value": json_data.get("estimatedSalary",{}).get("value",{}).get("maxValue"),
                    "unit": json_data.get("estimatedSalary",{}).get("value",{}).get("unitText"),
                },
                "location": {
                    "country": json_data.get("jobLocation",{}).get("address",{}).get("addressCountry"),
                    "locality": json_data.get("jobLocation",{}).get("address",{}).get("addressLocality"),
                    "region": json_data.get("jobLocation",{}).get("address",{}).get("addressRegion"),
                    "postal_code": json_data.get("jobLocation",{}).get("address",{}).get("postalCode"),
                    "street_address": json_data.get("jobLocation",{}).get("address",{}).get("streetAddress"),
                    "latitude": json_data.get("jobLocation",{}).get("latitude"),
                    "longitude": json_data.get("jobLocation",{}).get("longitude"),
                },
            })

            experience_requirements = json_data.get("experienceRequirements")

    if isinstance(experience_requirements, str):
        transformed_data["experience"]["months_of_experience"] = experience_requirements
    
    os.makedirs('staging/transformed', exist_ok=True)
    for i, data in enumerate(transformed_data):
        with open(f'staging/transformed/{i}.json', 'w') as f:
            json.dump(data, f)

def clean_description(description):
    cleaned_description = description  
    return cleaned_description
    