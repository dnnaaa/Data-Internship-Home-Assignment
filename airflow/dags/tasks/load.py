import os
import json
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

def load_data_to_sqlite():
    transformed_path = '/home/anas/Data-Internship-Home-Assignment/staging/transformed'

    """Load data to SQLite database from transformed files."""
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')  # Use Airflow's SQLite connection

    for filename in os.listdir(transformed_path):
        file_path = os.path.join(transformed_path, filename)
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Insert the job data into the 'job' table and get the job_id
        job_query = """
        INSERT INTO job (title, industry, description, employment_type, date_posted)
        VALUES (?, ?, ?, ?, ?)
        """
        sqlite_hook.run(job_query, parameters=( 
            data['job']['title'], 
            data['job']['industry'], 
            data['job']['description'], 
            data['job']['employment_type'], 
            data['job']['date_posted']
        ))

        # Get the last inserted job_id
        job_id = sqlite_hook.get_first("SELECT last_insert_rowid()")[0]

        # Insert data into the other tables (company, education, experience, salary, location)

        # Insert company data
        company_query = """
        INSERT INTO company (job_id, name, link)
        VALUES (?, ?, ?)
        """
        sqlite_hook.run(company_query, parameters=( 
            job_id,
            data['company']['name'],
            data['company']['link']
        ))

        # Insert education data
        education_query = """
        INSERT INTO education (job_id, required_credential)
        VALUES (?, ?)
        """
        sqlite_hook.run(education_query, parameters=( 
            job_id,
            data['education']['required_credential']
        ))

        # Insert experience data
        experience_query = """
        INSERT INTO experience (job_id, months_of_experience, seniority_level)
        VALUES (?, ?, ?)
        """
        sqlite_hook.run(experience_query, parameters=( 
            job_id,
            data['experience']['months_of_experience'],
            data['experience']['seniority_level']
        ))

        # Insert salary data
        salary_query = """
        INSERT INTO salary (job_id, currency, min_value, max_value, unit)
        VALUES (?, ?, ?, ?, ?)
        """
        sqlite_hook.run(salary_query, parameters=( 
            job_id,
            data['salary']['currency'],
            data['salary']['min_value'],
            data['salary']['max_value'],
            data['salary']['unit']
        ))

        # Insert location data
        location_query = """
        INSERT INTO location (job_id, country, locality, region, postal_code, street_address, latitude, longitude)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        sqlite_hook.run(location_query, parameters=( 
            job_id,
            data['location']['country'],
            data['location']['locality'],
            data['location']['region'],
            data['location']['postal_code'],
            data['location']['street_address'],
            data['location']['latitude'],
            data['location']['longitude']
        ))

    return "All data has been successfully loaded into the database."
