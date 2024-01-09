from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import json
import os

def load_to_database(input_dir, sqlite_conn_id='sqlite_default'):
    sqlite_hook = SqliteHook(sqlite_conn_id=sqlite_conn_id)
    conn = sqlite_hook.get_conn()
    cursor = conn.cursor()

    for file_name in os.listdir(input_dir):
        file_path = os.path.join(input_dir, file_name)
        with open(file_path, 'r') as file:
            file_content = file.read()
            if not file_content.strip():
                print(f"Skipping empty file: {file_name}")
                continue

            try:
                data = json.loads(file_content)
            
                # Insert data into job table
                job_data = data['job']
                cursor.execute("""
                    INSERT INTO job (title, industry, description, employment_type, date_posted)
                    VALUES (?, ?, ?, ?, ?)
                """, (job_data['title'], job_data['industry'], job_data['description'],
                      job_data['employment_type'], job_data['date_posted']))
                job_id = cursor.lastrowid

                # Insert data into company table
                company_data = data['company']
                cursor.execute("""
                    INSERT INTO company (job_id, name, link)
                    VALUES (?, ?, ?)
                """, (job_id, company_data['name'], company_data['link']))

                # Insert data into education table
                education_data = data['education']
                cursor.execute("""
                    INSERT INTO education (job_id, required_credential)
                    VALUES (?, ?)
                """, (job_id, education_data['required_credential']))

                # Insert data into experience table
                experience_data = data['experience']
                cursor.execute("""
                    INSERT INTO experience (job_id, months_of_experience, seniority_level)
                    VALUES (?, ?, ?)
                """, (job_id, experience_data['months_of_experience'], experience_data['seniority_level']))

                # Insert data into salary table
                salary_data = data['salary']
                cursor.execute("""
                    INSERT INTO salary (job_id, currency, min_value, max_value, unit)
                    VALUES (?, ?, ?, ?, ?)
                """, (job_id, salary_data['currency'], salary_data['min_value'], 
                      salary_data['max_value'], salary_data['unit']))

                # Insert data into location table
                location_data = data['location']
                cursor.execute("""
                    INSERT INTO location (job_id, country, locality, region, postal_code, street_address, latitude, longitude)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (job_id, location_data['country'], location_data['locality'], 
                      location_data['region'], location_data['postal_code'], location_data['street_address'], 
                      location_data['latitude'], location_data['longitude']))

                conn.commit()
            except json.decoder.JSONDecodeError:
                print(f"Skipping invalid json file: {file_name}")
                continue

    cursor.close()
    conn.close()

