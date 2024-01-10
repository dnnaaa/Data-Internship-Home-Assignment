import json
import os
import re

def transform_job_data(extracted_folder, transformed_folder):
    for filename in os.listdir(extracted_folder):
        if filename.endswith('.json'):
            input_path = os.path.join(extracted_folder, filename)
            with open(input_path, 'r') as file:
                extracted_data = json.load(file)

            cleaned_description = extracted_data['job']['description'].strip()

            transformed_data = {
                'job': {
                    'title': extracted_data['job']['title'],
                    'industry': extracted_data['job']['industry'],
                    'description': cleaned_description,
                    'employment_type': extracted_data['job']['employment_type'],
                    'date_posted': extracted_data['job']['date_posted'],
                },
                'company': {
                    'name': extracted_data['company']['name'],
                    'link': extracted_data['company']['linkedin_link'],
                },
                'education': {
                    'required_credential': extracted_data['job']['required_credential'],
                },
                'experience': {
                    'months_of_experience': extracted_data['job']['months_of_experience'],
                    'seniority_level': extracted_data['job']['seniority_level'],
                },
                'salary': {
                    'currency': extracted_data['salary']['currency'],
                    'min_value': extracted_data['salary']['min_value'],
                    'max_value': extracted_data['salary']['max_value'],
                    'unit': extracted_data['salary']['unit'],
                },
                'location': {
                    'country': extracted_data['location']['country'],
                    'locality': extracted_data['location']['locality'],
                    'region': extracted_data['location']['region'],
                    'postal_code': extracted_data['location']['postal_code'],
                    'street_address': extracted_data['location']['street_address'],
                    'latitude': extracted_data['location']['latitude'],
                    'longitude': extracted_data['location']['longitude'],
                },
            }

            output_path = os.path.join(transformed_folder, filename)

            with open(output_path, 'w') as file:
                json.dump(transformed_data, file, indent=4)
