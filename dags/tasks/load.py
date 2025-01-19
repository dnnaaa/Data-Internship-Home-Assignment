import json
import sqlite3
import os

def load_data():
    db_path = '/opt/airflow/airflow.db'
    
    # Supprimer la base de données si elle existe déjà
    if os.path.exists(db_path):
        os.remove(db_path)
        print("Base de données existante supprimée.")
    
    # Connexion à la base de données SQLite
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Création des tables nécessaires 
    cursor.execute('''CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        industry TEXT,
        description TEXT,
        employment_type TEXT,
        date_posted TEXT,
        company_name TEXT,
        company_link TEXT,
        required_credential TEXT,
        months_of_experience INTEGER,
        seniority_level TEXT,
        salary_currency TEXT,
        salary_min_value INTEGER,
        salary_max_value INTEGER,
        salary_unit TEXT,
        country TEXT,
        locality TEXT,
        region TEXT,
        postal_code TEXT,
        street_address TEXT,
        latitude REAL,
        longitude REAL
    )''')
    
    # Dossier contenant les fichiers transformés
    transformed_path = '/opt/airflow/staging/transformed'
    
    # Charger chaque fichier transformé dans la base de données
    for filename in os.listdir(transformed_path):
        file_path = os.path.join(transformed_path, filename)
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Insertion des données transformées dans la table 'jobs'
        cursor.execute('''
            INSERT INTO jobs (
                title, industry, description, employment_type, date_posted,
                company_name, company_link, required_credential, months_of_experience,
                seniority_level, salary_currency, salary_min_value, salary_max_value, salary_unit,
                country, locality, region, postal_code, street_address, latitude, longitude
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data['job']['title'], data['job']['industry'], data['job']['description'], data['job']['employment_type'], data['job']['date_posted'],
            data['company']['name'], data['company']['link'], data['education']['required_credential'], data['experience']['months_of_experience'],
            data['experience']['seniority_level'], data['salary']['currency'], data['salary']['min_value'], data['salary']['max_value'], data['salary']['unit'],
            data['location']['country'], data['location']['locality'], data['location']['region'], data['location']['postal_code'],
            data['location']['street_address'], data['location']['latitude'], data['location']['longitude']
        ))
    
    # Commit des transactions et fermeture de la connexion
    conn.commit()
    conn.close()
    print("Les données ont été importées avec succès dans la base de données.")
