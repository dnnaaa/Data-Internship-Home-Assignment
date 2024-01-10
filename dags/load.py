# tasks/load_task.py
import os
import sqlite3
import json
from airflow.decorators import task
from datetime import datetime

@task()
def load(transformed_dir, sqlite_conn_id):
    """Load transformed job data into SQLite database."""
    conn = sqlite3.connect(sqlite_conn_id)
    cursor = conn.cursor()

    for filename in os.listdir(transformed_dir):
        if filename.endswith(".json"):
            filepath = os.path.join(transformed_dir, filename)
            with open(filepath, 'r') as file:
                transformed_data = json.load(file)

            job_data = transformed_data.get("job", {})
            company_data = transformed_data.get("company", {})
            education_data = transformed_data.get("education", {})
            experience_data = transformed_data.get("experience", {})
            salary_data = transformed_data.get("salary", {})
            location_data = transformed_data.get("location", {})

            # SQL insert statements
            insert_job_statement = """
            INSERT INTO job(title, industry, description, employment_type, date_posted)
            VALUES (?, ?, ?, ?, ?);
            """

            insert_company_statement = """
            INSERT INTO company(job_id, name, link)
            VALUES ((SELECT id FROM job WHERE title = ?), ?, ?);
            """

            insert_education_statement = """
            INSERT INTO education(job_id, required_credential)
            VALUES ((SELECT id FROM job WHERE title = ?), ?);
            """

            insert_experience_statement = """
            INSERT INTO experience(job_id, months_of_experience, seniority_level)
            VALUES ((SELECT id FROM job WHERE title = ?), ?, ?);
            """

            insert_salary_statement = """
            INSERT INTO salary(job_id, currency, min_value, max_value, unit)
            VALUES ((SELECT id FROM job WHERE title = ?), ?, ?, ?, ?);
            """

            insert_location_statement = """
            INSERT INTO location(job_id, country, locality, region, postal_code, street_address, latitude, longitude)
            VALUES ((SELECT id FROM job WHERE title = ?), ?, ?, ?, ?, ?, ?, ?);
            """

            # Execute SQL insert statements
            cursor.execute(insert_job_statement, (
                job_data.get("title", ""),
                job_data.get("industry", ""),
                job_data.get("description", ""),
                job_data.get("employment_type", ""),
                job_data.get("date_posted", ""),
            ))

            cursor.execute(insert_company_statement, (
                job_data.get("title", ""),
                company_data.get("name", ""),
                company_data.get("link", ""),
            ))

            cursor.execute(insert_education_statement, (
                job_data.get("title", ""),
                education_data.get("required_credential", ""),
            ))

            cursor.execute(insert_experience_statement, (
                job_data.get("title", ""),
                experience_data.get("months_of_experience", ""),
                experience_data.get("seniority_level", ""),
            ))

            cursor.execute(insert_salary_statement, (
                job_data.get("title", ""),
                salary_data.get("currency", ""),
                salary_data.get("min_value", ""),
                salary_data.get("max_value", ""),
                salary_data.get("unit", ""),
            ))

            cursor.execute(insert_location_statement, (
                job_data.get("title", ""),
                location_data.get("country", ""),
                location_data.get("locality", ""),
                location_data.get("region", ""),
                location_data.get("postal_code", ""),
                location_data.get("street_address", ""),
                location_data.get("latitude", ""),
                location_data.get("longitude", ""),
            ))

    conn.commit()
    conn.close()
