import json
import sqlite3
from pathlib import Path
from airflow.decorators import task

@task()
def load(transformed_dir, database_path):

    transformed_path = Path(transformed_dir)
    if not transformed_path.is_dir():
        raise FileNotFoundError(f"The directory '{transformed_dir}' does not exist.")

    db_path = Path(database_path)
    if not db_path.is_file():
        raise FileNotFoundError(f"The database '{database_path}' does not exist.")

    connection = sqlite3.connect(database_path)
    cursor = connection.cursor()

    for file_path in transformed_path.glob("*.json"):
        with file_path.open('r', encoding='utf-8') as file:
            data = json.load(file)

        cursor.execute("""
            INSERT INTO job (title, industry, description, employment_type, date_posted)
            VALUES (?, ?, ?, ?, ?)
        """, (
            data["job"]["title"],
            data["job"]["industry"],
            data["job"]["description"],
            data["job"]["employment_type"],
            data["job"]["date_posted"]
        ))
        job_id = cursor.lastrowid

        cursor.execute("""
            INSERT INTO company (job_id, name, link)
            VALUES (?, ?, ?)
        """, (
            job_id,
            data["company"]["name"],
            data["company"]["link"]
        ))

        cursor.execute("""
            INSERT INTO education (job_id, required_credential)
            VALUES (?, ?)
        """, (
            job_id,
            data["education"]["required_credential"]
        ))

        cursor.execute("""
            INSERT INTO experience (job_id, months_of_experience, seniority_level)
            VALUES (?, ?, ?)
        """, (
            job_id,
            data["experience"]["months_of_experience"],
            data["experience"]["seniority_level"]
        ))

        cursor.execute("""
            INSERT INTO salary (job_id, currency, min_value, max_value, unit)
            VALUES (?, ?, ?, ?, ?)
        """, (
            job_id,
            data["salary"]["currency"],
            data["salary"]["min_value"],
            data["salary"]["max_value"],
            data["salary"]["unit"]
        ))

        cursor.execute("""
            INSERT INTO location (job_id, country, locality, region, postal_code, street_address, latitude, longitude)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            job_id,
            data["location"]["country"],
            data["location"]["locality"],
            data["location"]["region"],
            data["location"]["postal_code"],
            data["location"]["street_address"],
            data["location"]["latitude"],
            data["location"]["longitude"]
        ))

    connection.commit()
    connection.close()

    print(f"Data successfully loaded into the database '{database_path}'.")