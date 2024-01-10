import os
import json
from airflow.providers.sqlite.hooks.sqlite import SqliteHook


def load_data_to_sqlite(transformed_dir):
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')

    for filename in os.listdir(transformed_dir):
        file_path = os.path.join(transformed_dir, filename)
        with open(file_path, 'r') as file:

            if os.stat(file_path).st_size == 0:
                print(f"Skipping empty file: {filename}")
                continue

            data = json.load(file)

            # Establish database connection
            connection = sqlite_hook.get_conn()
            cursor = connection.cursor()

            try:
                # Insert into 'job' table and get job_id
                job_data = data["job"]
                job_insert_stmt = """
                    INSERT INTO job (title, industry, description, employment_type, date_posted)
                    VALUES (?, ?, ?, ?, ?);
                """
                cursor.execute(job_insert_stmt, (job_data["title"], job_data["industry"], job_data["description"],
                                                 job_data["employment_type"], job_data["date_posted"]))
                job_id = cursor.lastrowid

                # Insert into 'company' table
                company_data = data["company"]
                company_insert_stmt = """
                    INSERT INTO company (job_id, name, link)
                    VALUES (?, ?, ?);
                """
                cursor.execute(company_insert_stmt, (job_id,
                               company_data["name"], company_data["link"]))

                # Insert into 'education' table
                education_data = data["education"]
                education_insert_stmt = """
                    INSERT INTO education (job_id, required_credential)
                    VALUES (?, ?);
                """
                cursor.execute(education_insert_stmt,
                               (job_id, education_data["required_credential"]))

                # Insert into 'experience' table
                experience_data = data["experience"]
                experience_insert_stmt = """
                    INSERT INTO experience (job_id, months_of_experience, seniority_level)
                    VALUES (?, ?, ?);
                """
                cursor.execute(experience_insert_stmt, (
                    job_id, experience_data["months_of_experience"], experience_data["seniority_level"]))

                # Insert into 'salary' table
                salary_data = data["salary"]
                salary_insert_stmt = """
                    INSERT INTO salary (job_id, currency, min_value, max_value, unit)
                    VALUES (?, ?, ?, ?, ?);
                """
                cursor.execute(salary_insert_stmt, (job_id, salary_data["currency"], salary_data["min_value"],
                                                    salary_data["max_value"], salary_data["unit"]))

                # Insert into 'location' table
                location_data = data["location"]
                location_insert_stmt = """
                    INSERT INTO location (job_id, country, locality, region, postal_code, street_address, latitude, longitude)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """
                cursor.execute(location_insert_stmt, (job_id, location_data["country"], location_data["locality"],
                                                      location_data["region"], location_data["postal_code"],
                                                      location_data["street_address"], location_data["latitude"], location_data["longitude"]))

                # Commit the transaction
                connection.commit()

            except Exception as e:
                # Rollback in case of error
                connection.rollback()
                raise e

            finally:
                # Close the cursor and connection
                cursor.close()
                connection.close()

    return "Data is saved in Database"
