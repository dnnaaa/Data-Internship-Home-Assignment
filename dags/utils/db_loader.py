import sqlite3
import json
import os
from typing import Dict, Any



def safe_extract(data: Dict[str, Any], path: list, default=None) -> Any:
    """Safely navigate through nested dictionaries"""
    current = data
    for key in path:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current or default

def insert_related_data(conn: sqlite3.Connection, job_id: int, data: Dict[str, Any]):
    """Insert data into related tables"""
    cursor = conn.cursor()
    
    # Insert company data
    if 'company' in data:
        cursor.execute('''
            INSERT INTO company (job_id, name, link)
            VALUES (?, ?, ?)
        ''', (
            job_id,
            safe_extract(data, ['company', 'name']),
            safe_extract(data, ['company', 'link'])
        ))
    
    # Insert education data
    if 'education' in data:
        cursor.execute('''
            INSERT INTO education (job_id, required_credential)
            VALUES (?, ?)
        ''', (
            job_id,
            safe_extract(data, ['education', 'required_credential'])
        ))
    
    # Insert experience data
    if 'experience' in data:
        cursor.execute('''
            INSERT INTO experience (job_id, months_of_experience, seniority_level)
            VALUES (?, ?, ?)
        ''', (
            job_id,
            safe_extract(data, ['experience', 'months_of_experience'], 0),
            safe_extract(data, ['experience', 'seniority_level'])
        ))
    
    # Insert salary data
    if 'salary' in data:
        cursor.execute('''
            INSERT INTO salary (job_id, currency, min_value, max_value, unit)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            job_id,
            safe_extract(data, ['salary', 'currency']),
            safe_extract(data, ['salary', 'min_value'], 0),
            safe_extract(data, ['salary', 'max_value'], 0),
            safe_extract(data, ['salary', 'unit'])
        ))
    
    # Insert location data
    if 'location' in data:
        cursor.execute('''
            INSERT INTO location (
                job_id, country, locality, region, postal_code,
                street_address, latitude, longitude
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            job_id,
            safe_extract(data, ['location', 'country']),
            safe_extract(data, ['location', 'locality']),
            safe_extract(data, ['location', 'region']),
            safe_extract(data, ['location', 'postal_code']),
            safe_extract(data, ['location', 'street_address']),
            safe_extract(data, ['location', 'latitude']),
            safe_extract(data, ['location', 'longitude'])
        ))

def load_jobs_to_database(input_file='staging/transformed/all_jobs.json',db_path='staging/loaded/jobs_database.db'):
    # Configure paths
    # input_file = os.path.join('staging/transformed', 'all_jobs.json')
    # db_path = 'jobs_database.db'
    
    # Connect to SQLite database
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # Load transformed data
        with open(input_file, 'r') as f:
            jobs = json.load(f)

        # Process each job
        for job_data in jobs:
            try:
                # Start transaction
                conn.execute('BEGIN TRANSACTION')

                # Insert main job data
                cursor.execute('''
                    INSERT INTO job (
                        title, industry, description, 
                        employment_type, date_posted
                    ) VALUES (?, ?, ?, ?, ?)
                ''', (
                    safe_extract(job_data, ['job', 'title']),
                    safe_extract(job_data, ['job', 'industry']),
                    safe_extract(job_data, ['job', 'description']),
                    safe_extract(job_data, ['job', 'employment_type']),
                    safe_extract(job_data, ['job', 'date_posted'])
                ))
                
                # Get the auto-generated job ID
                job_id = cursor.lastrowid
                
                # Insert related data
                insert_related_data(conn, job_id, job_data)
                
                # Commit transaction
                conn.commit()

            except (sqlite3.IntegrityError, sqlite3.OperationalError) as e:
                print(f"Error inserting job: {e}")
                conn.rollback()
                continue
