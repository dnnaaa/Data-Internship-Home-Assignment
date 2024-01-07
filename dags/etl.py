"""
DAG: etl_dag

Description:
    Ce DAG est conçu pour extraire, transformer et charger (ETL) les données des offres d'emploi LinkedIn à partir d'un fichier CSV dans une base de données SQLite.

Tables Créées:
    - Job Title: Informations sur l'offre d'emploi
    - company: Informations sur l'entreprise liée à l'offre d'emploi
    - education: Informations sur les exigences en matière de formation pour l'offre d'emploi
    - experience: Informations sur l'expérience requise pour l'offre d'emploi
    - salary: Informations sur la rémunération associée à l'offre d'emploi
    - location: Informations sur l'emplacement de l'offre d'emploi

Tâches:
    1. create_tables: Crée les tables dans la base de données SQLite en utilisant le schéma défini dans TABLES_CREATION_QUERY.
    2. extract: Extrait les données du fichier CSV des offres d'emploi.
    3. transform: Nettoie et convertit les données extraites en format JSON.
    4. load: Charge les données transformées dans la base de données SQLite.

Dépendances:
    - La tâche create_tables doit être exécutée avant extract, transform et load.
    - extract fournit des données à transform.
    - transform fournit des données à load.

Paramètres du DAG:
    - dag_id: etl_dag
    - description: ETL LinkedIn job posts
    - tags: ["etl"]
    - schedule: Tâche planifiée quotidiennement ("@daily").
    - start_date: Date de début du DAG (1er janvier 2024).
    - catchup: Désactivé pour éviter l'exécution rétroactive des tâches.

"""
import pandas as pd
import json
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

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
    # Chemin vers le fichier CSV
     data = 'C:\\Users\\HP\\Desktop\\Parcours master\\datascience\\Data-Internship-Home-Assignment-main\\source\\jobs.csv'

    # Lecture du fichier CSV
    df = pd.read_csv(data)

    # Récupération des données de la colonne 'context'
    extracted_data = df['context'].tolist()

    return extracted_data



@task()
def transform(extracted_data):
    """Clean and convert extracted elements to json."""
    transformed_data = []

    for item in extracted_data:
        json_data = json.loads(item)
        transformed_item = {
            "Job Title": json_data.get("title", "").upper(),
            "industry": json_data.get("industry", "").upper(),
            "description": json_data.get("description", "").upper(),
            "employment_type": json_data.get("employmentType", "").upper(),
            "date_posted": json_data.get("datePosted", ""),
            "location": json_data.get("jobLocation", {}).get("addressLocality", "").upper(),
            "salary_range": json_data.get("salary", {}).get("minValue", 0),
        }
        transformed_data.append(transformed_item)

    transformed_json = json.dumps(transformed_data)
    return transformed_json

@task()
def load(transformed_data):
    """Load data to sqlite database."""
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    connection = sqlite_hook.get_conn()
    cursor = connection.cursor()

    for item in transformed_data:
        cursor.execute(
            "INSERT INTO job (title, industry, description, employment_type, date_posted) VALUES (?, ?, ?, ?, ?)",
            (item["title"], item["industry"], item["description"], item["employment_type"], item["date_posted"])
        )

    connection.commit()
    cursor.close()
    connection.close()

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

    create_tables = SqliteOperator(
        task_id="create_tables",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY
    )

    create_tables >> extract() >> transform() >> load()

etl_dag()
