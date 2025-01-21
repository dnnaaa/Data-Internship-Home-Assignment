import json
import os
import sqlite3
from airflow.decorators import task

@task()
def load():
    """
    Load data from JSON files in the staging directory and insert it into an SQLite database.
    Includes debug logs and error handling.
    """
    # Directory paths
    staging_dir = '/opt/airflow/data/staging/transformed'
    db_path = '/opt/airflow/data/jobs.db'

    # Ensure the staging directory exists
    if not os.path.exists(staging_dir):
        raise FileNotFoundError(f"Staging directory not found: {staging_dir}")

    # Check write permissions for the data directory
    if not os.access('/opt/airflow/data', os.W_OK):
        raise PermissionError("The '/opt/airflow/data' directory is not writable.")

    # Connect to SQLite database
    try:
        conn = sqlite3.connect(db_path)
    except sqlite3.OperationalError as e:
        raise RuntimeError(f"Failed to connect to SQLite database: {e}")

    cursor = conn.cursor()

    try:
        # Iterate through all files in the staging directory
        for filename in os.listdir(staging_dir):
            file_path = os.path.join(staging_dir, filename)

            # Skip non-JSON files
            if not filename.endswith('.json'):
                print(f"Skipping non-JSON file: {filename}")
                continue

            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON file {filename}: {e}")
                continue

            # Insert data into the SQLite database
            try:
                # Insert job information
                cursor.execute('''
                    INSERT INTO job (title, industry, description, employment_type, date_posted)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    data['job']['title'], data['job']['industry'], data['job']['description'], 
                    data['job']['employment_type'], data['job']['date_posted']
                ))
                job_id = cursor.lastrowid

                # Insert company information
                cursor.execute('''
                    INSERT INTO company (job_id, name, link)
                    VALUES (?, ?, ?)
                ''', (job_id, data['company']['name'], data['company']['link']))

                # Insert education information
                cursor.execute('''
                    INSERT INTO education (job_id, required_credential)
                    VALUES (?, ?)
                ''', (job_id, data['education']['required_credential']))

                # Insert experience information
                cursor.execute('''
                    INSERT INTO experience (job_id, months_of_experience, seniority_level)
                    VALUES (?, ?, ?)
                ''', (
                    job_id, data['experience']['months_of_experience'], 
                    data['experience']['seniority_level']
                ))

                # Insert salary information
                cursor.execute('''
                    INSERT INTO salary (job_id, currency, min_value, max_value, unit)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    job_id, data['salary']['currency'], data['salary']['min_value'], 
                    data['salary']['max_value'], data['salary']['unit']
                ))

                # Insert location information
                cursor.execute('''
                    INSERT INTO location (job_id, country, locality, region, postal_code, 
                                          street_address, latitude, longitude)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    job_id, data['location']['country'], data['location']['locality'], 
                    data['location']['region'], data['location']['postal_code'], 
                    data['location']['street_address'], data['location']['latitude'], 
                    data['location']['longitude']
                ))

            except KeyError as e:
                print(f"Missing key in JSON data for file {filename}: {e}")
                continue

        # Commit the changes to the database
        conn.commit()

    except Exception as e:
        print(f"An error occurred during the loading process: {e}")
        conn.rollback()

    finally:
        # Close the database connection
        conn.close()
        print("Data loading completed.")
