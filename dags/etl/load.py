import sqlite3
import json
import os

def load_data(input_directory, db_path):
    """Load transformed data into SQLite database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Créer les tables si elles n'existent pas déjà
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS job (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title VARCHAR(225),
            industry VARCHAR(225),
            description TEXT,
            employment_type VARCHAR(125),
            date_posted DATE
        );
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS company (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER,
            name VARCHAR(225),
            link TEXT,
            FOREIGN KEY (job_id) REFERENCES job(id)
        );
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS education (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER,
            required_credential VARCHAR(225),
            FOREIGN KEY (job_id) REFERENCES job(id)
        );
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS experience (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER,
            months_of_experience INTEGER,
            seniority_level VARCHAR(25),
            FOREIGN KEY (job_id) REFERENCES job(id)
        );
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS salary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER,
            currency VARCHAR(3),
            min_value NUMERIC,
            max_value NUMERIC,
            unit VARCHAR(12),
            FOREIGN KEY (job_id) REFERENCES job(id)
        );
    ''')

    cursor.execute('''
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
        );
    ''')

    # Charger les fichiers transformés dans la base de données
    for file_name in os.listdir(input_directory):
        if file_name.endswith('.json'):
            file_path = os.path.join(input_directory, file_name)
            
            with open(file_path, 'r') as json_file:
                data = json.load(json_file)

            # Insérer les données dans la table 'job'
            cursor.execute('''
                INSERT INTO job (title, industry, description, employment_type, date_posted)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                data['job']['title'],
                data['job']['industry'],
                data['job']['description'],
                data['job']['employment_type'],
                data['job']['date_posted']
            ))
            job_id = cursor.lastrowid

            # Insérer les données de la compagnie
            cursor.execute('''
                INSERT INTO company (job_id, name, link)
                VALUES (?, ?, ?)
            ''', (
                job_id,
                data['company']['name'],
                data['company']['link']
            ))

            # Insérer les données de l'éducation
            cursor.execute('''
                INSERT INTO education (job_id, required_credential)
                VALUES (?, ?)
            ''', (
                job_id,
                data['education']['required_credential']
            ))

            # Insérer les données de l'expérience
            cursor.execute('''
                INSERT INTO experience (job_id, months_of_experience, seniority_level)
                VALUES (?, ?, ?)
            ''', (
                job_id,
                data['experience']['months_of_experience'],
                data['experience']['seniority_level']
            ))

            # Insérer les données du salaire
            cursor.execute('''
                INSERT INTO salary (job_id, currency, min_value, max_value, unit)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                job_id,
                data['salary']['currency'],
                data['salary']['min_value'],
                data['salary']['max_value'],
                data['salary']['unit']
            ))

            # Insérer les données de la localisation
            cursor.execute('''
                INSERT INTO location (job_id, country, locality, region, postal_code, street_address, latitude, longitude)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                job_id,
                data['location']['country'],
                data['location']['locality'],
                data['location']['region'],
                data['location']['postal_code'],
                data['location']['street_address'],
                data['location']['latitude'],
                data['location']['longitude']
            ))

    conn.commit()
    conn.close()
