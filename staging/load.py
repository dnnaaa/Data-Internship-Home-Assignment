import os
import json
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

def load():
    """Load the transformed data from staging/transformed and save it to the SQLite database."""
    staging_dir = "staging/transformed"

    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    transformed_files = os.listdir(staging_dir)

    for file_name in transformed_files:
        file_path = os.path.join(staging_dir, file_name)

        with open(file_path, "r") as f:
            data = json.load(f)

        # Load data to the SQLite database
        # Insert the data into the appropriate tables using the SQLiteHook

        # Example:
        job_data = data['job']
        company_data = data['company']
        # ...

        # Insert job data
        job_id = sqlite_hook.insert_rows(
            table='job',
            rows=[(
                job_data['title'],
                job_data['industry'],
                job_data['description'],
                job_data['employment_type'],
                job_data['date_posted']
            )]
        )

        # Insert company data
        company_id = sqlite_hook.insert_rows(
            table='company',
            rows=[(
                job_id,
                company_data['name'],
                company_data['link']
            )]
        )

       