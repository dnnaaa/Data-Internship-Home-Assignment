from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import json

def load_data(file_path):
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    connection = sqlite_hook.get_conn()
    cursor = connection.cursor()

    with open(file_path, 'r') as json_file:
        data = json.load(json_file)

        # Insert data into 'job' table
        job_data = data.get('job', {})
        cursor.execute("""
            INSERT INTO job (title, industry, description, employment_type, date_posted)
            VALUES (?, ?, ?, ?, ?)
        """, (
            job_data.get('title', ''),
            job_data.get('industry', ''),
            job_data.get('description', ''),
            job_data.get('employment_type', ''),
            job_data.get('date_posted', ''),
        ))

        # Get the last inserted job_id
        job_id = cursor.lastrowid

        # Insert data into 'company' table
        company_data = data.get('company', {})
        cursor.execute("""
            INSERT INTO company (job_id, name, link)
            VALUES (?, ?, ?)
        """, (
            job_id,
            company_data.get('name', ''),
            company_data.get('link', ''),
        ))

        # Insert data into 'education' table
        education_data = data.get('education', {})
        cursor.execute("""
            INSERT INTO education (job_id, required_credential)
            VALUES (?, ?)
        """, (
            job_id,
            education_data.get('required_credential', ''),
        ))

        # Insert data into 'experience' table
        experience_data = data.get('experience', {})
        cursor.execute("""
            INSERT INTO experience (job_id, months_of_experience, seniority_level)
            VALUES (?, ?, ?)
        """, (
            job_id,
            experience_data.get('months_of_experience', 0),
            experience_data.get('seniority_level', ''),
        ))


        # Insert data into 'salary' table
        salary_data = data.get('salary', {})
        cursor.execute("""
            INSERT INTO salary (job_id, currency, min_value, max_value, unit)
            VALUES (?, ?, ?, ?, ?)
        """, (
            job_id,
            salary_data.get('currency', ''),
            salary_data.get('min_value', 0),
            salary_data.get('max_value', 0),
            salary_data.get('unit', ''),
        ))

        # Insert data into 'location' table
        location_data = data.get('location', {})
        cursor.execute("""
            INSERT INTO location (job_id, country, locality, region, postal_code, street_address, latitude, longitude)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            job_id,
            location_data.get('country', ''),
            location_data.get('locality', ''),
            location_data.get('region', ''),
            location_data.get('postal_code', ''),
            location_data.get('street_address', ''),
            location_data.get('latitude', 0.0),
            location_data.get('longitude', 0.0),
        ))

    # Commit changes and close the connection
    connection.commit()
    connection.close()


def load_task():
    for i in range(7684) : 
        file_path = f"staging/transformed/transformed_file_{i}.json" 
        load_data(file_path)