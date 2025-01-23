import os
import json
import logging
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

@task()
def load_task():
    """Load data to PostgreSQL database."""
    input_dir = "staging/transformed"
    
    # Use the PostgresHook to connect to PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id="postgres_db_connection")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    try:
        for file_name in os.listdir(input_dir):
            file_path = os.path.join(input_dir, file_name)
            logging.info(f"Processing file: {file_path}")

            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)

                # Insert data into job table and retrieve the job_id
                job_data = data["job"]
                cursor.execute("""
                    INSERT INTO job (title, industry, description, employment_type, date_posted)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id;
                """, (
                    job_data.get("title"),
                    job_data.get("industry"),
                    job_data.get("description"),
                    job_data.get("employment_type"),
                    job_data.get("date_posted")
                ))

                # Fetch the job_id of the newly inserted job
                job_id = cursor.fetchone()[0]

                # Insert related data into other tables
                company_data = data["company"]
                cursor.execute("""
                    INSERT INTO company (job_id, name, link)
                    VALUES (%s, %s, %s)
                """, (
                    job_id,
                    company_data.get("name"),
                    company_data.get("link")
                ))

                education_data = data["education"]
                cursor.execute("""
                    INSERT INTO education (job_id, required_credential)
                    VALUES (%s, %s)
                """, (
                    job_id,
                    education_data.get("required_credential")
                ))

                experience_data = data["experience"]
                cursor.execute("""
                    INSERT INTO experience (job_id, months_of_experience, seniority_level)
                    VALUES (%s, %s, %s)
                """, (
                    job_id,
                    experience_data.get("months_of_experience"),
                    experience_data.get("seniority_level")
                ))

                salary_data = data["salary"]
                cursor.execute("""
                    INSERT INTO salary (job_id, currency, min_value, max_value, unit)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    job_id,
                    salary_data.get("currency"),
                    salary_data.get("min_value"),
                    salary_data.get("max_value"),
                    salary_data.get("unit")
                ))

                location_data = data["location"]
                cursor.execute("""
                    INSERT INTO location (job_id, country, locality, region, postal_code, street_address, latitude, longitude)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    job_id,
                    location_data.get("country"),
                    location_data.get("locality"),
                    location_data.get("region"),
                    location_data.get("postal_code"),
                    location_data.get("street_address"),
                    location_data.get("latitude"),
                    location_data.get("longitude")
                ))

                logging.info(f"Successfully loaded data from {file_name} into PostgreSQL database.")

            except json.JSONDecodeError as e:
                logging.error(f"Invalid JSON in file {file_name}: {str(e)}")
            except KeyError as e:
                logging.error(f"Missing key in JSON data in file {file_name}: {str(e)}")
            except Exception as e:
                logging.error(f"An error occurred while processing file {file_name}: {str(e)}")

        conn.commit()
        logging.info("All files processed successfully.")

    except Exception as e:
        logging.error(f"An error occurred during the load task: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

    return "Load task completed successfully."