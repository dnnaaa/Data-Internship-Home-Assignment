import os
import json
import sqlite3
from airflow.decorators import task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

@task()
def load():
    """Load data to sqlite database."""
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    transformed_dir = 'staging/transformed'

    
    conn = sqlite3.connect(sqlite_hook)
    cursor = conn.cursor()

   
    TABLES_CREATION_QUERY = """
    CREATE TABLE IF NOT EXISTS job (
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

  
    cursor.executescript(TABLES_CREATION_QUERY)

    def insert_into_table(table_name, data):
        columns = ', '.join(data.keys())
        placeholders = ', '.join('?' * len(data))
        values = tuple(data.values())
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.execute(insert_query, values)
        conn.commit()

   
    for filename in os.listdir(transformed_dir):
        if filename.endswith('.json'):
            with open(os.path.join(transformed_dir, filename), 'r') as file:
                data = json.load(file)
                
                job_data = data.get('job', {})
                insert_into_table('job', job_data)
                
                
                company_data = data.get('company', {})
                if company_data:
                    company_data['job_id'] = cursor.lastrowid
                    insert_into_table('company', company_data)
                
                
                education_data = data.get('education', {})
                if education_data:
                    education_data['job_id'] = cursor.lastrowid
                    insert_into_table('education', education_data)
                
                
                experience_data = data.get('experience', {})
                if experience_data:
                    experience_data['job_id'] = cursor.lastrowid
                    insert_into_table('experience', experience_data)
                
                #
                salary_data = data.get('salary', {})
                if salary_data:
                    salary_data['job_id'] = cursor.lastrowid
                    insert_into_table('salary', salary_data)
                
               
                location_data = data.get('location', {})
                if location_data:
                    location_data['job_id'] = cursor.lastrowid
                    insert_into_table('location', location_data)

    
    conn.close()
