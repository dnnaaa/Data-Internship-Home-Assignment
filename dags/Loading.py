import json
import os
import glob
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.decorators import task

@task()
def load():
    """Load transformed data into the SQLite database."""
    transformed_dir = 'staging/transformed'
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')

    for file_path in glob.glob(f'{transformed_dir}/*.json'):
        with open(file_path, 'r') as file:
            data = json.loads(file.read())
            load_data_to_database(data, sqlite_hook)

def load_data_to_database(data, hook):
    """Load individual data entries to the database."""
    # Extract and insert data for the 'job' table
    job_id = insert_job(data['job'], hook)

    # Assuming foreign key constraints, insert data into other tables with the job_id
    insert_company(data['company'], job_id, hook)
    insert_education(data['education'], job_id, hook)
    insert_experience(data['experience'], job_id, hook)
    insert_salary(data['salary'], job_id, hook)
    insert_location(data['location'], job_id, hook)

def insert_job(job_data, hook):
    insert_query = """
    INSERT INTO job (title, industry, description, employment_type, date_posted)
    VALUES (?, ?, ?, ?, ?)
    """
    hook.run(insert_query, parameters=(
        job_data['title'],
        job_data['industry'],
        job_data['description'],
        job_data['employment_type'],
        job_data['date_posted']
    ))
    return hook.get_first("SELECT last_insert_rowid()")[0]

# Define similar functions for company, education, experience, salary, and location
# Example for the company table
def insert_company(company_data, job_id, hook):
    insert_query = """
    INSERT INTO company (job_id, name, link)
    VALUES (?, ?, ?)
    """
    hook.run(insert_query, parameters=(
        job_id,
        company_data['name'],
        company_data['link']
    ))

def insert_education(education_data, job_id, hook):
    insert_query = """
    INSERT INTO education (job_id, required_credential)
    VALUES (?, ?)
    """
    hook.run(insert_query, parameters=(
        job_id,
        education_data['required_credential']
    ))

def insert_experience(experience_data, job_id, hook):
    insert_query = """
    INSERT INTO experience (job_id, months_of_experience, seniority_level)
    VALUES (?, ?, ?)
    """
    hook.run(insert_query, parameters=(
        job_id,
        experience_data['months_of_experience'],
        experience_data['seniority_level']
    ))

def insert_salary(salary_data, job_id, hook):
    insert_query = """
    INSERT INTO salary (job_id, currency, min_value, max_value, unit)
    VALUES (?, ?, ?, ?, ?)
    """
    hook.run(insert_query, parameters=(
        job_id,
        salary_data['currency'],
        salary_data['min_value'],
        salary_data['max_value'],
        salary_data['unit']
    ))

def insert_location(location_data, job_id, hook):
    insert_query = """
    INSERT INTO location (job_id, country, locality, region, postal_code, street_address, latitude, longitude)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    hook.run(insert_query, parameters=(
        job_id,
        location_data['country'],
        location_data['locality'],
        location_data['region'],
        location_data['postal_code'],
        location_data['street_address'],
        location_data['latitude'],
        location_data['longitude']
    ))
