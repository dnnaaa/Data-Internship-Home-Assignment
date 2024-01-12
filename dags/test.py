from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import sqlite3

import pandas as pd 
import json
from bs4 import BeautifulSoup
import html
import os

SOURCE_CSV = "source/jobs.csv"
EXTRACTED_DIR = "staging/extracted"
TRANSFORMED_DIR = "staging/transformed"
DATABASE_FILE = "etl_database.db"


def extract():
    """Extract data from jobs.csv."""
    print("starting extraction...")
    df = pd.read_csv(SOURCE_CSV)
    df.dropna(axis=0, inplace=True)
    context_data = df['context']
        
    for i, data in enumerate(context_data):
        with open(f"{EXTRACTED_DIR}/extracted_{i}.txt", 'w') as file:
            data[:10]
            file.write(data)  
        file.close() 
    print("done!")

def transform():
    """Clean and convert extracted elements to json."""
    print("starting transformation...")
    os.makedirs(TRANSFORMED_DIR, exist_ok=True)

    for file_name in os.listdir(EXTRACTED_DIR):
        if file_name.endswith(".txt"):
            with open(os.path.join(EXTRACTED_DIR, file_name), 'r') as file:
                data = json.loads(file.read())

            transformed_data = {
                "job": {
                    "title": data.get("title", ""),
                    "industry": data.get("industry", ""),
                    "description": clean_description(data.get("description", "")),
                    "employment_type": data.get("employmentType", ""),
                    "date_posted": data.get("datePosted", ""),
                },
                "company": {
                    "name": data.get("hiringOrganization", {}).get("name", ""),
                    "link": data.get("hiringOrganization", {}).get("sameAs", ""),
                },
                "education": {
                    "required_credential": data.get("educationRequirements", {}).get("credentialCategory", ""),
                },
                "experience": {
                    "months_of_experience": data.get('experienceRequirements', {}).get('monthsOfExperience', '') if isinstance(data.get('experienceRequirements', {}), dict) else '',
                    "seniority_level": None,
                },
                "salary": {
                    "currency": data.get("estimatedSalary", {}).get("currency", ""),
                    "min_value": data.get("estimatedSalary", {}).get("value", {}).get("minValue", ""),
                    "max_value": data.get("estimatedSalary", {}).get("value", {}).get("maxValue", ""),
                    "unit": data.get("estimatedSalary", {}).get("value", {}).get("unitText", ""),
                },
                "location": {
                    "country": data.get("jobLocation", {}).get("address", {}).get("addressCountry", ""),
                    "locality": data.get("jobLocation", {}).get("address", {}).get("addressLocality", ""),
                    "region": data.get("jobLocation", {}).get("address", {}).get("addressRegion", ""),
                    "postal_code": data.get("jobLocation", {}).get("address", {}).get("postalCode", ""),
                    "street_address": data.get("jobLocation", {}).get("address", {}).get("streetAddress", ""),
                    "latitude": data.get("jobLocation", {}).get("latitude", ""),
                    "longitude": data.get("jobLocation", {}).get("longitude", ""),
                },
            }

            transformed_json = json.dumps(transformed_data, indent=2)

            with open(f"{TRANSFORMED_DIR}/transformed_{file_name.split('_')[1].split('.')[0]}.json", 'w') as file:
                
                file.write(transformed_json)
    print("done!")

def clean_description(description):
    decoded_text = html.unescape(description)
    soup = BeautifulSoup(decoded_text, 'html.parser')
    cleaned_description = soup.get_text(separator=' ')
    
    return cleaned_description


def load():
    """Load data to sqlite database."""
    print("starting loading...")
    db_path = 'sqlite_default.db'  
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Define SQLite insert queries 
    job_query = """
        INSERT INTO job (title, industry, description, employment_type, date_posted)
        VALUES (?, ?, ?, ?, ?)
    """
    company_query = """
        INSERT INTO company (name, link)
        VALUES (?, ?)
    """
    education_query = """
        INSERT INTO education (required_credential)
        VALUES (?)
    """
    experience_query = """
        INSERT INTO experience (months_of_experience, seniority_level)
        VALUES (?, ?)
    """
    salary_query = """
        INSERT INTO salary (currency, min_value, max_value, unit)
        VALUES (?, ?, ?, ?)
    """
    location_query = """
        INSERT INTO location (country, locality, region, postal_code, street_address, latitude, longitude)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    for file_name in os.listdir(TRANSFORMED_DIR):
        if file_name.endswith(".json"):
            with open(os.path.join(TRANSFORMED_DIR, file_name), 'r') as file:
                data = json.loads(file.read())

            try:
                # Execute the query
                cursor.execute(job_query, (
                    data["job"]["title"],
                    data["job"]["industry"],
                    data["job"]["description"],
                    data["job"]["employment_type"],
                    data["job"]["date_posted"],
                ))

                cursor.execute(company_query, (
                    data["company"]["name"],
                    data["company"]["link"],
                ))

                cursor.execute(education_query, (
                    data["education"]["required_credential"],
                ))

                cursor.execute(experience_query, (
                    data["experience"]["months_of_experience"],
                    data["experience"]["seniority_level"],
                ))

                cursor.execute(salary_query, (
                    data["salary"]["currency"],
                    data["salary"]["min_value"],
                    data["salary"]["max_value"],
                    data["salary"]["unit"],
                ))

                cursor.execute(location_query, (
                    data["location"]["country"],
                    data["location"]["locality"],
                    data["location"]["region"],
                    data["location"]["postal_code"],
                    data["location"]["street_address"],
                    data["location"]["latitude"],
                    data["location"]["longitude"],
                ))

                conn.commit()

            except sqlite3.Error as e:
                print(f"SQLite error: {e}")
                conn.rollback()

    conn.close()
    print("done!")

extract()
transform()
load()