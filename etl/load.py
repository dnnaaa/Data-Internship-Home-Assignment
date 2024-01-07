import json
from airflow.decorators import task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from pathlib import Path

@task()
def load_to_database():
    """Task to load data to SQLite database."""
    transformed_folder = Path("staging/transformed")

    # Initialize SQLiteHook
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')

    for file_path in transformed_folder.glob("*.json"):
        with open(file_path, "r") as file:
            transformed_data = json.load(file)

        # Save the transformed data to the SQLite database
        save_to_database(sqlite_hook, transformed_data)



def save_to_database(sqlite_hook, transformed_data):
    """Save transformed data to SQLite database."""
    connection = sqlite_hook.get_conn()
    cursor = connection.cursor()

    try:
        # Insert records into the job table
        insert_job_query = """
            INSERT INTO job (title, industry, description, employment_type, date_posted)
            VALUES (?, ?, ?, ?, ?)
        """
        job_values = (
            transformed_data['job']['title'],
            transformed_data['job']['industry'],
            transformed_data['job']['description'],
            transformed_data['job']['employment_type'],
            transformed_data['job']['date_posted']
        )
        cursor.execute(insert_job_query, job_values)

        # Get the job_id of the inserted job record
        job_id = cursor.lastrowid

        # Insert records into the company table
        insert_company_query = """
            INSERT INTO company (job_id, name, link)
            VALUES (?, ?, ?)
        """
        company_values = (
            job_id,
            transformed_data['company']['name'],
            transformed_data['company']['link']
        )
        cursor.execute(insert_company_query, company_values)

        # Insert records into the education table
        insert_education_query = """
            INSERT INTO education (job_id, required_credential)
            VALUES (?, ?)
        """
        education_values = (
            job_id,
            transformed_data['education']['required_credential']
        )
        cursor.execute(insert_education_query, education_values)

        # Insert records into the experience table
        insert_experience_query = """
            INSERT INTO experience (job_id, months_of_experience, seniority_level)
            VALUES (?, ?, ?)
        """
        experience_values = (
            job_id,
            transformed_data['experience']['months_of_experience'],
            transformed_data['experience']['seniority_level']
        )
        cursor.execute(insert_experience_query, experience_values)

        # Insert records into the salary table
        insert_salary_query = """
            INSERT INTO salary (job_id, currency, min_value, max_value, unit)
            VALUES (?, ?, ?, ?, ?)
        """
        salary_values = (
            job_id,
            transformed_data['salary']['currency'],
            transformed_data['salary']['min_value'],
            transformed_data['salary']['max_value'],
            transformed_data['salary']['unit']
        )
        cursor.execute(insert_salary_query, salary_values)

        # Insert records into the location table
        insert_location_query = """
            INSERT INTO location (job_id, country, locality, region, postal_code, street_address, latitude, longitude)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        location_values = (
            job_id,
            transformed_data['location']['country'],
            transformed_data['location']['locality'],
            transformed_data['location']['region'],
            transformed_data['location']['postal_code'],
            transformed_data['location']['street_address'],
            transformed_data['location']['latitude'],
            transformed_data['location']['longitude']
        )
        cursor.execute(insert_location_query, location_values)

        # Commit the changes to the database
        connection.commit()
    except Exception as e:
        # Handle any exceptions that might occur during database operations
        connection.rollback()
        raise e
    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()
