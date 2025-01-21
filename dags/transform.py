import os
import json

def transform_job():
    input_folder = 'staging/extracted'
    output_folder = 'staging/transformed'
    os.makedirs(output_folder, exist_ok=True)

    for file_name in os.listdir(input_folder):
        with open(os.path.join(input_folder, file_name), 'r') as file:
            context = json.loads(file.read())

        transformed_data = {
            "job": {
                "title": context.get("job_title"),
                "industry": context.get("job_industry"),
                "description": context.get("job_description"),
                "employment_type": context.get("job_employment_type"),
                "date_posted": context.get("job_date_posted"),
            },
            "company": {
                "name": context.get("company_name"),
                "link": context.get("company_linkedin_link"),
            },
            "education": {
                "required_credential": context.get("job_required_credential"),
            },
            "experience": {
                "months_of_experience": context.get("job_months_of_experience"),
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

        output_file = os.path.join(output_folder, file_name.replace('.txt', '.json'))
        with open(output_file, 'w') as out_file:
            json.dump(transformed_data, out_file, indent=4)
