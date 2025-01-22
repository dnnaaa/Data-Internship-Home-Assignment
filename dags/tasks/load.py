import json
import os
import sqlite3
from airflow.decorators import task

@task()
def load(input_dir: str = 'staging/transformed', database_path: str = 'database/jobs.db'):
    """
    Load transformed data into SQLite database.

    Args:
        input_dir (str): Directory containing transformed JSON files.
        database_path (str): Path to the SQLite database file.
    """
    try:
        conn = sqlite3.connect(database_path)
        with conn:
            cursor = conn.cursor()
            transformed_files = os.listdir(input_dir)
            if not transformed_files:
                raise FileNotFoundError(f"No transformed files found in {input_dir}")

            for filename in transformed_files:
                process_file(filename, input_dir, cursor, conn)

    except sqlite3.Error as db_error:
        raise ConnectionError(f"Error connecting to the database: {db_error}")

    except Exception as e:
        print(f"Error: {str(e)}")

def process_file(filename, input_dir, cursor, conn):
    """
    Process each JSON file and insert data into SQLite database.

    Args:
        filename (str): Name of the JSON file.
        input_dir (str): Directory containing transformed JSON files.
        cursor (sqlite3.Cursor): SQLite cursor object.
        conn (sqlite3.Connection): SQLite connection object.
    """
    file_path = os.path.join(input_dir, filename)
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)

        job_id = insert_job_data(cursor, data)
        insert_company_data(cursor, job_id, data)
        insert_education_data(cursor, job_id, data)
        insert_experience_data(cursor, job_id, data)
        insert_salary_data(cursor, job_id, data)
        insert_location_data(cursor, job_id, data)

        conn.commit()
        print(f"Successfully processed {filename}")

    except json.JSONDecodeError:
        print(f"JSON decode error in file {filename}")
    except FileNotFoundError as fnf_error:
        print(f"File error for {filename}: {str(fnf_error)}")
    except sqlite3.Error as sql_error:
        print(f"Database error while processing {filename}: {str(sql_error)}")
        conn.rollback()
    except Exception as e:
        print(f"Error processing {filename}: {str(e)}")

def insert_job_data(cursor, data):
    """
    Insert job data into the database.

    Args:
        cursor (sqlite3.Cursor): SQLite cursor object.
        data (dict): JSON data containing job information.

    Returns:
        int: Last inserted job_id.
    """
    job_query = """
        INSERT INTO job (title, industry, description, employment_type, date_posted)
        VALUES (?, ?, ?, ?, ?)
    """
    job_data = (
        data['job']['title'],
        data['job']['industry'],
        data['job']['description'],
        data['job']['employment_type'],
        data['job']['date_posted']
    )
    cursor.execute(job_query, job_data)
    return cursor.lastrowid

def insert_company_data(cursor, job_id, data):
    """
    Insert company data into the database.

    Args:
        cursor (sqlite3.Cursor): SQLite cursor object.
        job_id (int): ID of the job associated with the company.
        data (dict): JSON data containing company information.
    """
    company_query = """
        INSERT INTO company (job_id, name, link)
        VALUES (?, ?, ?)
    """
    company_data = (job_id, data['company']['name'], data['company']['link'])
    cursor.execute(company_query, company_data)

def insert_education_data(cursor, job_id, data):
    """
    Insert education data into the database.

    Args:
        cursor (sqlite3.Cursor): SQLite cursor object.
        job_id (int): ID of the job associated with the education requirement.
        data (dict): JSON data containing education information.
    """
    education_query = """
        INSERT INTO education (job_id, required_credential)
        VALUES (?, ?)
    """
    education_data = (job_id, data['education']['required_credential'])
    cursor.execute(education_query, education_data)

def insert_experience_data(cursor, job_id, data):
    """
    Insert experience data into the database.

    Args:
        cursor (sqlite3.Cursor): SQLite cursor object.
        job_id (int): ID of the job associated with the experience requirement.
        data (dict): JSON data containing experience information.
    """
    experience_query = """
        INSERT INTO experience (job_id, months_of_experience, seniority_level)
        VALUES (?, ?, ?)
    """
    experience_data = (
        job_id,
        data['experience']['months_of_experience'],
        data['experience']['seniority_level']
    )
    cursor.execute(experience_query, experience_data)

def insert_salary_data(cursor, job_id, data):
    """
    Insert salary data into the database.

    Args:
        cursor (sqlite3.Cursor): SQLite cursor object.
        job_id (int): ID of the job associated with the salary information.
        data (dict): JSON data containing salary details.
    """
    salary_query = """
        INSERT INTO salary (job_id, currency, min_value, max_value, unit)
        VALUES (?, ?, ?, ?, ?)
    """
    salary_data = (
        job_id,
        data['salary']['currency'],
        data['salary']['min_value'],
        data['salary']['max_value'],
        data['salary']['unit']
    )
    cursor.execute(salary_query, salary_data)

def insert_location_data(cursor, job_id, data):
    """
    Insert location data into the database.

    Args:
        cursor (sqlite3.Cursor): SQLite cursor object.
        job_id (int): ID of the job associated with the location.
        data (dict): JSON data containing location information.
    """
    location_query = """
        INSERT INTO location (
            job_id, country, locality, region, postal_code,
            street_address, latitude, longitude
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    location_data = (
        job_id,
        data['location']['country'],
        data['location']['locality'],
        data['location']['region'],
        data['location']['postal_code'],
        data['location']['street_address'],
        data['location']['latitude'],
        data['location']['longitude']
    )
    cursor.execute(location_query, location_data)
