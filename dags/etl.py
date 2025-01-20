from datetime import timedelta, datetime
import json
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List

from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

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

def ensure_directory(path: str) -> None:
    """Ensure directory exists."""
    Path(path).mkdir(parents=True, exist_ok=True)

@task()
def extract() -> List[str]:
    """Extract data from jobs.csv."""
    # Create staging directory
    extracted_path = "staging/extracted"
    ensure_directory(extracted_path)
    
    # Read CSV file
    df = pd.read_csv("source/jobs.csv")
    
    extracted_files = []
    for index, row in df.iterrows():
        file_path = f"{extracted_path}/job_{index}.txt"
        with open(file_path, 'w') as f:
            f.write(row['context'])
        extracted_files.append(file_path)
    
    return extracted_files

def extract_seniority(title: str) -> str:
    """Extract seniority level from job title."""
    title_lower = title.lower()
    if "senior" in title_lower or "sr" in title_lower:
        return "senior"
    elif "principal" in title_lower:
        return "principal"
    elif "junior" in title_lower or "jr" in title_lower:
        return "junior"
    return "mid-level"

def transform_job_data(job_data: Dict[str, Any]) -> Dict[str, Any]:
    """Transform job data into desired schema."""
    return {
        "job": {
            "title": job_data.get("title", ""),
            "industry": job_data.get("industry", ""),
            "description": job_data.get("description", ""),
            "employment_type": job_data.get("employmentType", ""),
            "date_posted": job_data.get("datePosted", ""),
        },
        "company": {
            "name": job_data.get("hiringOrganization", {}).get("name", ""),
            "link": job_data.get("hiringOrganization", {}).get("sameAs", ""),
        },
        "education": {
            "required_credential": job_data.get("educationRequirements", {}).get("credentialCategory", ""),
        },
        "experience": {
            "months_of_experience": job_data.get("experienceRequirements", {}).get("monthsOfExperience", 0),
            "seniority_level": extract_seniority(job_data.get("title", "")),
        },
        "salary": {
            "currency": job_data.get("estimatedSalary", {}).get("currency", ""),
            "min_value": job_data.get("estimatedSalary", {}).get("value", {}).get("minValue", 0),
            "max_value": job_data.get("estimatedSalary", {}).get("value", {}).get("maxValue", 0),
            "unit": job_data.get("estimatedSalary", {}).get("value", {}).get("unitText", ""),
        },
        "location": {
            "country": job_data.get("jobLocation", {}).get("address", {}).get("addressCountry", ""),
            "locality": job_data.get("jobLocation", {}).get("address", {}).get("addressLocality", ""),
            "region": job_data.get("jobLocation", {}).get("address", {}).get("addressRegion", ""),
            "postal_code": job_data.get("jobLocation", {}).get("address", {}).get("postalCode", ""),
            "street_address": job_data.get("jobLocation", {}).get("address", {}).get("streetAddress", ""),
            "latitude": job_data.get("jobLocation", {}).get("latitude", 0),
            "longitude": job_data.get("jobLocation", {}).get("longitude", 0),
        },
    }

@task()
def transform(extracted_files: List[str]) -> List[str]:
    """Clean and convert extracted elements to json."""
    transformed_path = "staging/transformed"
    ensure_directory(transformed_path)
    
    transformed_files = []
    for file_path in extracted_files:
        with open(file_path, 'r') as f:
            job_data = json.loads(f.read())
        
        transformed_data = transform_job_data(job_data)
        output_path = f"{transformed_path}/{Path(file_path).stem}.json"
        
        with open(output_path, 'w') as f:
            json.dump(transformed_data, f, indent=2)
        
        transformed_files.append(output_path)
    
    return transformed_files

@task()
def load(transformed_files: List[str]) -> None:
    """Load data to sqlite database."""
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    
    for file_path in transformed_files:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Insert job data and get job_id
        job_query = """
            INSERT INTO job (title, industry, description, employment_type, date_posted)
            VALUES (?, ?, ?, ?, ?)
        """
        job_values = (
            data['job']['title'],
            data['job']['industry'],
            data['job']['description'],
            data['job']['employment_type'],
            data['job']['date_posted']
        )
        job_id = sqlite_hook.run(job_query, parameters=job_values)
        
        # Insert company data
        company_query = """
            INSERT INTO company (job_id, name, link)
            VALUES (?, ?, ?)
        """
        sqlite_hook.run(company_query, parameters=(
            job_id,
            data['company']['name'],
            data['company']['link']
        ))
        
        # Insert education data
        education_query = """
            INSERT INTO education (job_id, required_credential)
            VALUES (?, ?)
        """
        sqlite_hook.run(education_query, parameters=(
            job_id,
            data['education']['required_credential']
        ))
        
        # Insert experience data
        experience_query = """
            INSERT INTO experience (job_id, months_of_experience, seniority_level)
            VALUES (?, ?, ?)
        """
        sqlite_hook.run(experience_query, parameters=(
            job_id,
            data['experience']['months_of_experience'],
            data['experience']['seniority_level']
        ))
        
        # Insert salary data if available
        if data['salary'].get('currency'):
            salary_query = """
                INSERT INTO salary (job_id, currency, min_value, max_value, unit)
                VALUES (?, ?, ?, ?, ?)
            """
            sqlite_hook.run(salary_query, parameters=(
                job_id,
                data['salary']['currency'],
                data['salary']['min_value'],
                data['salary']['max_value'],
                data['salary']['unit']
            ))
        
        # Insert location data
        location_query = """
            INSERT INTO location (
                job_id, country, locality, region, postal_code,
                street_address, latitude, longitude
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        sqlite_hook.run(location_query, parameters=(
            job_id,
            data['location']['country'],
            data['location']['locality'],
            data['location']['region'],
            data['location']['postal_code'],
            data['location']['street_address'],
            data['location']['latitude'],
            data['location']['longitude']
        ))

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
    """ETL pipeline"""
    
    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id="sqlite_default",  # Changed from sqlite_conn_id to conn_id
        sql=TABLES_CREATION_QUERY,
        conn_type="sqlite"  # Added conn_type
    )
    
    # Get the extracted files
    extracted_files = extract()
    
    # Transform the extracted files
    transformed_files = transform(extracted_files)
    
    # Load the transformed files
    loaded = load(transformed_files)
    
    # Set up the task dependencies
    create_tables >> extracted_files >> transformed_files >> loaded

# Create the DAG instance
dag = etl_dag()