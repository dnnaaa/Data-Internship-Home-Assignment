from datetime import timedelta, datetime
import re
import json
import pandas as pd
import os 
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

TABLES_CREATION_QUERY = [
    """CREATE TABLE IF NOT EXISTS job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(225),
    industry VARCHAR(225),
    description TEXT,
    employment_type VARCHAR(125),
    date_posted DATE
);""",

"""CREATE TABLE IF NOT EXISTS company (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    name VARCHAR(225),
    link TEXT,
    FOREIGN KEY (job_id) REFERENCES job(id)
);""",

"""CREATE TABLE IF NOT EXISTS education (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    required_credential VARCHAR(225),
    FOREIGN KEY (job_id) REFERENCES job(id)
);""",

"""CREATE TABLE IF NOT EXISTS experience (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    months_of_experience INTEGER,
    seniority_level VARCHAR(25),
    FOREIGN KEY (job_id) REFERENCES job(id)
);""",

"""CREATE TABLE IF NOT EXISTS salary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    currency VARCHAR(3),
    min_value NUMERIC,
    max_value NUMERIC,
    unit VARCHAR(12),
    FOREIGN KEY (job_id) REFERENCES job(id)
);""",

"""CREATE TABLE IF NOT EXISTS location (
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
);"""]

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
    start_date=datetime(2024, 1, 6),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)

def etl_dag():
    """ETL pipeline"""
    create_tables = create_tables_task()
    extract_data = extract_task()
    transform_data = transform_task()
    check_tables= check_tables_task()
    load_data = load_task()
    
    create_tables >> extract_data >> transform_data >> check_tables >> load_data

def create_tables_task():
    return SqliteOperator(
        task_id="create_tables",
        sqlite_conn_id="sqlite",
        sql=TABLES_CREATION_QUERY
    )


@task()
def extract_task():
    """Extract data from jobs.csv."""
    import pandas as pd
    # Read CSV file
    df = pd.read_csv('source/jobs.csv')
    
    for index, row in df.iterrows():
        context_data = str(row['context'])  # Convert to string
        
        with open(f'staging/extracted/{index}.txt', 'w') as file:
            file.write(context_data)
            
            
@task()
def transform_task():
    """Clean and convert extracted elements to json."""
    for filename in os.listdir('staging/extracted'):
        with open(f'staging/extracted/{filename}', 'r') as file:
            file_content = file.read()
            print(f'File content before JSON load: {file_content}')  

            if file_content == '{}' or file_content.lower() == 'nan':
                continue  
        
            try:
                data = json.loads(file_content)
            except json.decoder.JSONDecodeError as e:
                print(f'Error loading JSON from file {filename}: {str(e)}') 
                continue  
            
        # Clean job description et transform schema
        transformed_data = {
            'job': {
                'title': data.get('title', ''),
                'industry': data.get('industry', ''),
                'description': clean_description(data.get('description', '')),
                'employment_type': data.get('employmentType', ''),
                'date_posted': data.get('datePosted', ''),
            },
            'company': {
                'name': data.get('hiringOrganization', {}).get('name', ''),
                'link': data.get('hiringOrganization', {}).get('sameAs', ''),
            },
            'education': {
                'required_credential': data.get('educationRequirements', {}).get('credentialCategory', ''),
            },
            'experience': {
                'months_of_experience': (
                    data.get('experienceRequirements', {}).get('monthsOfExperience', '')
                    if isinstance(data.get('experienceRequirements'), dict) 
                    else ''
                ),
                'seniority_level': (
                    data.get('experienceRequirements', {}).get('seniority_level', '')
                    if isinstance(data.get('experienceRequirements'), dict) 
                    else ''
                ),
            },
            'salary': {
                'currency': data.get('estimatedSalary', {}).get('currency', ''),
                'min_value': data.get('estimatedSalary', {}).get('value', {}).get('minValue', ''),
                'max_value': data.get('estimatedSalary', {}).get('value', {}).get('maxValue', ''),
                'unit': data.get('estimatedSalary', {}).get('value', {}).get('unitText', ''),
            },
            'location': {
                'country': data.get('jobLocation', {}).get('address', {}).get('addressCountry', ''),
                'locality': data.get('jobLocation', {}).get('address', {}).get('addressLocality', ''),
                'region': data.get('jobLocation', {}).get('address', {}).get('addressRegion', ''),
                'postal_code': data.get('jobLocation', {}).get('address', {}).get('postalCode', ''),
                'street_address': data.get('jobLocation', {}).get('address', {}).get('streetAddress', ''),
                'latitude': data.get('jobLocation', {}).get('latitude', ''),
                'longitude': data.get('jobLocation', {}).get('longitude', ''),
            },
        }
        # Nouveau nom du fichier transformé
        output_filename = f'{filename.split(".")[0]}.json'
            
        # Sauvegardez les données transformées dans des fichiers JSON
        output_file_path = os.path.join('staging/transformed', output_filename)
            
        with open(output_file_path, 'w') as outfile:
            json.dump(transformed_data, outfile, indent=2)


def clean_description(description):
    # Remove HTML tags
    description = re.sub(r'<.*?>', '', description)

    # Remove special characters and numbers
    description = re.sub(r'[^A-Za-z\s]', '', description)

    # Normalize spaces
    description = re.sub(r'\s+', ' ', description).strip()

    # Convert to lowercase
    description = description.lower()

    return description

@task()
def check_tables_task():
    """Show all tables in the SQLite database."""
    hook = SqliteHook(sqlite_conn_id="sqlite")
    conn = hook.get_conn()
    cursor = conn.cursor()

    query = "SELECT name FROM sqlite_master WHERE type='table';"
    cursor.execute(query)

    tables = cursor.fetchall()
    print("Tables in the database:")
    for table in tables:
        print(table[0])

    conn.close()
    
    
@task()
def load_task():
    sqlite_hook = SqliteHook(sqlite_conn_id="sqlite") 

    transformed_folder = 'staging/transformed'
    for filename in os.listdir(transformed_folder):
        file_path = os.path.join(transformed_folder, filename)
        if os.path.getsize(file_path) > 0:
            with open(file_path, 'r') as file:
                transformed_data = json.load(file)

            # Load data into company table job
            job_params = (
                transformed_data['job']['title'],
                transformed_data['job']['industry'],
                transformed_data['job']['description'],
                transformed_data['job']['employment_type'],
                transformed_data['job']['date_posted'],
            )
            sqlite_hook.run(
                "INSERT INTO job (title, industry, description, employment_type, date_posted) "
                "VALUES (?, ?, ?, ?, ?)",
                parameters=job_params,
            )

            # Load data into company table 
            company_params = (
                transformed_data['job']['title'],
                transformed_data['company']['name'],
                transformed_data['company']['link'],
            )
            sqlite_hook.run(
                "INSERT INTO company (job_id, name, link) "
                "VALUES ((SELECT id FROM job WHERE title = ?), ?, ?)",
                parameters=company_params,
            )

            # Load data into education table 
            education_params = (
                transformed_data['job']['title'],
                transformed_data['education']['required_credential'],
            )
            sqlite_hook.run(
                "INSERT INTO education (job_id, required_credential) "
                "VALUES ((SELECT id FROM job WHERE title = ?), ?)",
                parameters=education_params,
            )

            # Load data into experience table 
            experience_params = (
                transformed_data['job']['title'],
                transformed_data['experience']['months_of_experience'],
                transformed_data['experience']['seniority_level'],
            )
            sqlite_hook.run(
                "INSERT INTO experience (job_id, months_of_experience, seniority_level) "
                "VALUES ((SELECT id FROM job WHERE title = ?), ?, ?)",
                parameters=experience_params,
            )

            # Load data into salary table 
            salary_params = (
                transformed_data['job']['title'],
                transformed_data['salary']['currency'],
                transformed_data['salary']['min_value'],
                transformed_data['salary']['max_value'],
                transformed_data['salary']['unit'],
            )
            sqlite_hook.run(
                "INSERT INTO salary (job_id, currency, min_value, max_value, unit) "
                "VALUES ((SELECT id FROM job WHERE title = ?), ?, ?, ?, ?)",
                parameters=salary_params,
            )

            # Load data into location table 
            location_params = (
                transformed_data['job']['title'],
                transformed_data['location']['country'],
                transformed_data['location']['locality'],
                transformed_data['location']['region'],
                transformed_data['location']['postal_code'],
                transformed_data['location']['street_address'],
                transformed_data['location']['latitude'],
                transformed_data['location']['longitude'],
            )
            sqlite_hook.run(
                "INSERT INTO location (job_id, country, locality, region, postal_code, street_address, latitude, longitude) "
                "VALUES ((SELECT id FROM job WHERE title = ?), ?, ?, ?, ?, ?, ?, ?)",
                parameters=location_params,
            )

        else:
            print(f"Le fichier {filename} est vide.")


etl_dag()
