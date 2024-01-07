from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from bs4 import BeautifulSoup
import os
import json
import html
import re
import pandas as pd
import json

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

CREATE TABLE IF NOT EXISTS experience (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    months_of_experience INTEGER,
    seniority_level VARCHAR(25),
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

@task()
def extract():
    """Extract data from jobs.csv."""
    src = "../source/jobs.csv"
    df = pd.read_csv(src)
    context_data = df['context']

    o_dir = "../staging/extracted"
    os.makedirs(o_dir, exist_ok=True)

    for index, data in enumerate(context_data):
        try:
            json_string = str(data)

            if not json_string.strip():
                print(f"Warning: JSON string in row {index} is empty. Skipping.")
                continue

            json_data = json.loads(json_string)

            filename = f"{o_dir}/extracted_{index}.json"

            with open(filename, 'w', encoding='utf-8') as file:
                json.dump(json_data, file, ensure_ascii=False, indent=2)

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON in row {index}: {e}")
        except Exception as e:
            print(f"Error processing row {index}: {e}")


def clean_job_description(job_description):
    """Clean job description by removing HTML tags."""
    soup = BeautifulSoup(job_description, 'html.parser')

    for tag in soup(['br', 'ul', 'li']):
        tag.extract()

    cleaned_text = soup.get_text(separator=' ', strip=True)

    return cleaned_text

@task()
def transform():
    """Clean and convert extracted elements to JSON."""
    extract_directory = "../staging/extracted"
    transform_directory = "../staging/transformed"
    os.makedirs(transform_directory, exist_ok=True)

    for filename in os.listdir(extract_directory):
        if filename.endswith(".json"):
            input_filepath = os.path.join(extract_directory, filename)
            output_filepath = os.path.join(transform_directory, f"transformed_{filename}")

            with open(input_filepath, 'r', encoding='utf-8') as file:
                try:
                    json_data = json.load(file)

                    job_description = json_data.get("description", "")
                    cleaned_description = clean_job_description(job_description)

                    transformed_data = {
                        "job": {
                            "title": json_data.get("title", ""),
                            "industry": json_data.get("industry", ""),
                            "description": cleaned_description,
                            "employment_type": json_data.get("employmentType", ""),
                            "date_posted": json_data.get("datePosted", ""),
                        },
                        "company": {
                            "name": json_data.get("hiringOrganization", {}).get("name", ""),
                            "link": json_data.get("hiringOrganization", {}).get("sameAs", ""),
                        },
                        "education": {
                            "required_credential": json_data.get("educationRequirements", {}).get("credentialCategory", ""),
                        },
                        "experience": {
                            "months_of_experience": (
                                json_data.get("experienceRequirements", {})
                                if isinstance(json_data.get("experienceRequirements", {}), dict)
                                else {}
                            ).get("monthsOfExperience", ""),
                        },
                        "salary": {
                            "currency": json_data.get("salary_currency", ""),
                            "min_value": json_data.get("salary_min_value", ""),
                            "max_value": json_data.get("salary_max_value", ""),
                            "unit": json_data.get("salary_unit", ""),
                        },
                        "location": {
                            "country": json_data.get("jobLocation", {}).get("address", {}).get("addressCountry", ""),
                            "locality": json_data.get("jobLocation", {}).get("address", {}).get("addressLocality", ""),
                            "region": json_data.get("jobLocation", {}).get("address", {}).get("addressRegion", ""),
                            "postal_code": json_data.get("jobLocation", {}).get("address", {}).get("postalCode", ""),
                            "street_address": json_data.get("jobLocation", {}).get("address", {}).get("streetAddress", ""),
                            "latitude": json_data.get("jobLocation", {}).get("latitude", ""),
                            "longitude": json_data.get("jobLocation", {}).get("longitude", ""),
                        },
                    }

                    with open(output_filepath, 'w', encoding='utf-8') as output_file:
                        json.dump(transformed_data, output_file, ensure_ascii=False, indent=2)

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON in file {input_filepath}: {e}")
                except Exception as e:
                    print(f"Error processing file {input_filepath}: {e}")
                
@task()
def load():
    """Load data to sqlite database."""
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')

    transformed_data_directory = "../staging/transformed"

    for root, dirs, files in os.walk(transformed_data_directory):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                with open(file_path, 'r', encoding='utf-8') as json_file:
                    data = json.load(json_file)

                job_data = data.get('job', {})
                company_data = data.get('company', {})
                education_data = data.get('education', {})
                experience_data = data.get('experience', {})
                salary_data = data.get('salary', {})
                location_data = data.get('location', {})

                job_id = sqlite_hook.insert_rows(table="job", rows=[job_data])[0]
                company_data['job_id'] = job_id
                sqlite_hook.insert_rows(table="company", rows=[company_data])
                education_data['job_id'] = job_id
                sqlite_hook.insert_rows(table="education", rows=[education_data])
                experience_data['job_id'] = job_id
                sqlite_hook.insert_rows(table="experience", rows=[experience_data])
                salary_data['job_id'] = job_id
                sqlite_hook.insert_rows(table="salary", rows=[salary_data])
                location_data['job_id'] = job_id
                sqlite_hook.insert_rows(table="location", rows=[location_data])

DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

@dag(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)

def etl_dag():
    create_tables = SqliteOperator(
        task_id="create_tables",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY
    )

    create_tables >> extract() >> transform() >> load()

etl_dag()

