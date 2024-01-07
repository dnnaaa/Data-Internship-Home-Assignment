import sqlite3
import os
import json
from airflow.decorators import task

def insert_into_table(cursor, query, data):
    try:
        cursor.execute(query, data)
    except sqlite3.IntegrityError as e:
        print(f"Integrity Error: {e}")
    except sqlite3.OperationalError as e:
        print(f"Operational Error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

def insert_into_job(cursor, job_data):
    insert_into_table(cursor, """
        INSERT INTO job (title, industry, description, employment_type, date_posted)
        VALUES (?, ?, ?, ?, ?)
    """, (
        job_data['title'],
        job_data['industry'],
        job_data['description'],
        job_data['employment_type'],
        job_data['date_posted']
    ))
    return cursor.lastrowid

def insert_into_company(cursor, job_id, company_data):
    insert_into_table(cursor, """
        INSERT INTO company (job_id, name, link)
        VALUES (?, ?, ?)
    """, (
        job_id,
        company_data['name'],
        company_data['link']
    ))

def insert_into_education(cursor, job_id, education_data):
    insert_into_table(cursor, """
        INSERT INTO education (job_id, required_credential)
        VALUES (?, ?)
    """, (
        job_id,
        education_data['required_credential']
    ))

def insert_into_experience(cursor, job_id, experience_data):
    insert_into_table(cursor, """
        INSERT INTO experience (job_id, months_of_experience, seniority_level)
        VALUES (?, ?, ?)
    """, (
        job_id,
        experience_data['months_of_experience'],
        experience_data['seniority_level']
    ))

def insert_into_salary(cursor, job_id, salary_data):
    insert_into_table(cursor, """
        INSERT INTO salary (job_id, currency, min_value, max_value, unit)
        VALUES (?, ?, ?, ?, ?)
    """, (
        job_id,
        salary_data['currency'],
        salary_data['min_value'],
        salary_data['max_value'],
        salary_data['unit']
    ))

def insert_into_location(cursor, job_id, location_data):
    insert_into_table(cursor, """
        INSERT INTO location (job_id, country, locality, region, postal_code, street_address, latitude, longitude)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        job_id,
        location_data['country'],
        location_data['locality'],
        location_data['region'],
        location_data['postal_code'],
        location_data['street_address'],
        location_data['latitude'],
        location_data['longitude']
    ))

@task
def load(transformed_dir, db_file):
    with sqlite3.connect(db_file) as conn:
        for filename in os.listdir(transformed_dir):
            if filename.endswith('.txt'):
                file_path = os.path.join(transformed_dir, filename)
                with open(file_path, 'r', encoding='utf-8') as file:
                    for line in file:
                        record = json.loads(line.strip())
                        cursor = conn.cursor()
                        job_id = insert_into_job(cursor, record['job'])
                        insert_into_company(cursor, job_id, record['company'])
                        insert_into_education(cursor, job_id, record['education'])
                        insert_into_experience(cursor, job_id, record['experience'])
                        insert_into_salary(cursor, job_id, record['salary'])
                        insert_into_location(cursor, job_id, record['location'])
                        conn.commit()

