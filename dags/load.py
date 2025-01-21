import os
import json
import sqlite3

def load_job():
    input_folder = 'staging/transformed'
    db_file = 'jobs.db'
    connection = sqlite3.connect(db_file)
    cursor = connection.cursor()

    # Create table (example schema, adapt as needed)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        industry TEXT,
        description TEXT,
        employment_type TEXT,
        date_posted TEXT,
        company_name TEXT,
        company_link TEXT,
        required_credential TEXT,
        months_of_experience INTEGER,
        seniority_level TEXT,
        salary_currency TEXT,
        salary_min_value REAL,
        salary_max_value REAL,
        salary_unit TEXT,
        country TEXT,
        locality TEXT,
        region TEXT,
        postal_code TEXT,
        street_address TEXT,
        latitude REAL,
        longitude REAL
    )
    """)

    for file_name in os.listdir(input_folder):
        with open(os.path.join(input_folder, file_name), 'r') as file:
            job_data = json.load(file)

        # Insert into database
        job = job_data['job']
        company = job_data['company']
        education = job_data['education']
        experience = job_data['experience']
        salary = job_data['salary']
        location = job_data['location']

        cursor.execute("""
        INSERT INTO jobs (
            title, industry, description, employment_type, date_posted,
            company_name, company_link, required_credential,
            months_of_experience, seniority_level, salary_currency,
            salary_min_value, salary_max_value, salary_unit,
            country, locality, region, postal_code, street_address,
            latitude, longitude
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            job["title"], job["industry"], job["description"], job["employment_type"], job["date_posted"],
            company["name"], company["link"], education["required_credential"],
            experience["months_of_experience"], experience["seniority_level"], salary["currency"],
            salary["min_value"], salary["max_value"], salary["unit"],
            location["country"], location["locality"], location["region"], location["postal_code"], location["street_address"],
            location["latitude"], location["longitude"]
        ))

    connection.commit()
    connection.close()
