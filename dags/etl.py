from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.operators.python_operator import PythonOperator
import os
import sqlite3
import logging
import json
from bs4 import BeautifulSoup
import html

TABLES_CREATION_QUERY = """CREATE TABLE IF NOT EXISTS job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(225),
    industry VARCHAR(225),
    description TEXT,
    employment_type VARCHAR(125),
    date_posted DATE
);

CREATE TABLE IF NOT EXISTS company (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    name VARCHAR(225),
    link TEXT,
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS education (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    required_credential VARCHAR(225),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS salary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    currency VARCHAR(3),
    min_value NUMERIC,
    max_value NUMERIC,
    unit VARCHAR(12),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS location (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    country VARCHAR(60),
    locality VARCHAR(60),
    region VARCHAR(60),
    postal_code VARCHAR(25),
    street_address VARCHAR(225),
    latitude NUMERIC,
    longitude NUMERIC,
    FOREIGN KEY (job_id) REFERENCES job(id)
)
"""
dag_path = os.getcwd()

def extract():
    """Extract data from jobs.csv."""
    import pandas as pd
    data = pd.read_csv(f"{dag_path}/source/jobs.csv")
    extracted_dir = f"{dag_path}/staging/extracted"
    # Iterate through the 'context' column and save each value as a text file
    for index, context_value in enumerate(data['context']):
        # Define the filename based on the index
        filename = os.path.join(extracted_dir, f'extracted_{index}.txt')
    
        # Write the context value to the text file
        with open(filename, 'w', encoding='utf-8') as file:
            file.write(str(context_value))


def clean_description(description):
    if description is None:
        return None
    decoded_text = html.unescape(description)
    # Use BeautifulSoup to remove HTML tags
    soup = BeautifulSoup(decoded_text, 'html.parser')
    cleaned_description = soup.get_text()
    return cleaned_description

def transform_schema(data):
    transformed_data = {
        "job": {
            "title": data.get("title"),
            "industry": data.get("industry"),
            "description": clean_description(data.get("description")),
            "employment_type": data.get("employmentType"),
            "date_posted": data.get("datePosted"),
        },
        "company": {
            "name": data.get("hiringOrganization", {}).get("name"), 
            "link": data.get("hiringOrganization", {}).get("sameAs"),
        },
        "education": {
            "required_credential": data.get("educationRequirements", {}).get("credentialCategory"),
        },
        "salary": {
            "currency": data.get("estimatedSalary", {}).get("currency"),
            "min_value": data.get("estimatedSalary", {}).get("value", {}).get("minValue"),
            "max_value": data.get("estimatedSalary", {}).get("value", {}).get("maxValue"),
            "unit": data.get("estimatedSalary", {}).get("value", {}).get("unitText"),
        },
        "location": {
            "country": data.get("jobLocation", {}).get("address", {}).get("addressCountry"),
            "locality": data.get("jobLocation", {}).get("address", {}).get("addressLocality"),
            "region": data.get("jobLocation", {}).get("address", {}).get("addressRegion"),
            "postal_code": data.get("jobLocation", {}).get("address", {}).get("postalCode"),
            "street_address": data.get("jobLocation", {}).get("address", {}).get("streetAddress"),
            "latitude": data.get("jobLocation", {}).get("latitude"),
            "longitude": data.get("jobLocation", {}).get("longitude"),
        },
    }
    return transformed_data

#transform task

def transform():
    """Clean and convert extracted elements to json."""
    input_folder=f"{dag_path}/staging/extracted"
    output_folder=f"{dag_path}/staging/transformed"
    for filename in os.listdir(input_folder):
        if filename.endswith(".txt"):
            input_file_path = os.path.join(input_folder, filename)
            output_file_path = os.path.join(output_folder, f"{os.path.splitext(filename)[0]}.json")

            # Check if the file is not empty
            if os.path.getsize(input_file_path) == 0:
                print(f"Warning: Skipping empty file: {filename}")
                continue

            with open(input_file_path, 'r', encoding='utf-8') as file:
                try:
                    # Read the JSON data from the text file
                    json_data = json.loads(file.read())
                except json.JSONDecodeError:
                    print(f"Error: Invalid JSON format in file: {filename}")
                    continue

                
                # Transform the schema
                transformed_data = transform_schema(json_data)

                # Save the transformed data to a new JSON file in the output folder
                with open(output_file_path, 'w', encoding='utf-8') as output_file:
                    json.dump(transformed_data, output_file, ensure_ascii=False, indent=4)

def insert_data(cursor, data):
    # Insert data into 'job' table
    job_data = data['job']
    cursor.execute(
        """INSERT INTO job (title, industry, description, employment_type, date_posted)
           VALUES (?, ?, ?, ?, ?)""",
        (job_data['title'], job_data['industry'], job_data['description'],
         job_data['employment_type'], job_data['date_posted'])
    )
    job_id = cursor.lastrowid  # Retrieve the job_id after insertion

    # Insert data into 'company' table
    company_data = data['company']
    cursor.execute(
        """INSERT INTO company (job_id, name, link)
           VALUES (?, ?, ?)""",
        (job_id, company_data['name'], company_data['link'])
    )
    company_id = cursor.lastrowid 
    # Insert data into 'education' table
    education_data = data['education']
    cursor.execute(
        """INSERT INTO education (job_id, required_credential)
           VALUES (?, ?)""",
        (job_id, education_data['required_credential'])
    )
    education_id = cursor.lastrowid 
    # Insert data into 'education' table
    salary_data = data['salary']
    cursor.execute(
        """INSERT INTO salary (job_id, currency,min_value, max_value, unit)
           VALUES (?, ?, ?, ?, ?)""",
        (job_id, salary_data['currency'], salary_data['min_value'], salary_data['max_value'], salary_data['unit'])
    )
    salary_id = cursor.lastrowid 
    # Insert data into 'location' table
    location_data = data['location']
    cursor.execute(
        """INSERT INTO location (job_id, country, locality, region, postal_code, street_address, latitude, longitude)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (job_id, location_data['country'], location_data['locality'], location_data['region'],
         location_data['postal_code'], location_data['street_address'],
         location_data['latitude'], location_data['longitude'])
    )
    location_id = cursor.lastrowid 

def load():
    database_path = f"{dag_path}/load/data.db"
    json_folder_path=f"{dag_path}/staging/transformed"
    import sqlite3
    # Connect to SQLite database
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    # Execute the CREATE TABLE query
    cursor.executescript(TABLES_CREATION_QUERY)
    
    # Iterate over all JSON files in the directory
    for filename in os.listdir(json_folder_path):
        if filename.endswith('.json'):
            file_path = os.path.join(json_folder_path, filename)

            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)

            # Call the insert_data function to perform database insertion
            insert_data(cursor, data)

    # Commit the changes after processing all files
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()
    
DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15)
}

dag = DAG(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule="@daily",
    start_date=datetime(2024, 1, 7),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)

# Define the tasks
extract_data_task = PythonOperator(
    task_id='extract_data_task',
    python_callable=extract,
    dag=dag,
)

create_tables = SqliteOperator(
    task_id="create_tables",
    sqlite_conn_id="sqlite_default",
    sql=TABLES_CREATION_QUERY
)

transform_data_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform,
    dag=dag,
)
# Define the PythonOperator task
load_data_to_sqlite_task = PythonOperator(
    task_id='load_data_to_sqlite',
    python_callable=load,
    provide_context=False,
    dag=dag,
)

create_tables >> extract_data_task >> transform_data_task >> load_data_to_sqlite_task
