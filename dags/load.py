import os
import json
import sqlite3

from create_tables import TABLES_CREATION_QUERY

def load_transformed_data(transformed_folder: str, sqlite_db_path: str) -> None:
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    cursor.execute(TABLES_CREATION_QUERY)

    for filename in os.listdir(transformed_folder):
        if filename.endswith('.json'):
            file_path = os.path.join(transformed_folder, filename)

            with open(file_path, 'r') as json_file:
                transformed_data = json.load(json_file)

            job = transformed_data['job']
            company = transformed_data['company']
            education = transformed_data['education']
            experience = transformed_data['experience']
            salary = transformed_data['salary']
            location = transformed_data['location']

            cursor.execute('''
                INSERT INTO jobs (
                    title,
                    industry,
                    description,
                    employment_type,
                    date_posted,
                    company_name,
                    company_link,
                    required_credential,
                    months_of_experience,
                    seniority_level,
                    currency,
                    min_value,
                    max_value,
                    unit,
                    country,
                    locality,
                    region,
                    postal_code,
                    street_address,
                    latitude,
                    longitude
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    job['title'],
                    job['industry'],
                    job['description'],
                    job['employment_type'],
                    job['date_posted'],
                    company['name'],
                    company['link'],
                    education['required_credential'],
                    experience['months_of_experience'],
                    experience['seniority_level'],
                    salary['currency'],
                    salary['min_value'],
                    salary['max_value'],
                    salary['unit'],
                    location['country'],
                    location['locality'],
                    location['region'],
                    location['postal_code'],
                    location['street_address'],
                    location['latitude'],
                    location['longitude']
                ))

        conn.commit()
        conn.close()