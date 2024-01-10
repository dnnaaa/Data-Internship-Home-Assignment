import json
import sqlite3
import os


def loadDATA():
    # Chemin du répertoire des fichiers transformés
    dossier_transforme = 'staging/transformed'

    # Chemin de la base de données SQLite
    chemin_base_de_donnees = 'ma_base_de_donnees.db'

    # Connexion à la base de données SQLite
    conn = sqlite3.connect(chemin_base_de_donnees)
    cursor = conn.cursor()

    # Création de la table si elle n'existe pas
    cursor.execute(TABLES_CREATION_QUERY)

    conn.commit()

    # Parcourir tous les fichiers dans le dossier transformé
    for fichier_transforme in os.listdir(dossier_transforme):
        chemin_fichier_transforme = os.path.join(dossier_transforme, fichier_transforme)

        # Lire le contenu du fichier transformé
        with open(chemin_fichier_transforme, 'r', encoding='utf-8') as fichier:
            donnees_transformees = fichier.read()

        # Insérer les données dans la base de données
    cursor.execute('''
        INSERT INTO job (
            title, industry, description, employment_type, date_posted,
              country,
            locality, region, postal_code, street_address, latitude, longitude
        )
        VALUES (?, ?, ?, ?, ?)
    ''', (
        donnees_transformees['job']['title'],
        donnees_transformees['job']['industry'],
        donnees_transformees['job']['description'],
        donnees_transformees['job']['employment_type'],
        donnees_transformees['job']['date_posted'],
        
    ))

    cursor.execute('''
        INSERT INTO company (
            company,link
        )
        VALUES (?, ?)
    ''', (
        donnees_transformees['company']['name'],
        donnees_transformees['company']['link'],
        
    ))
    cursor.execute('''
        INSERT INTO education (
            required_credential
        )
        VALUES (?)
    ''', (
        donnees_transformees['education']['required_credential'],
    ))
    cursor.execute('''
        INSERT INTO experience (
            months_of_experience, seniority_level
        )
        VALUES (?, ?)
    ''', (
        donnees_transformees['experience']['months_of_experience'],
        donnees_transformees['experience']['seniority_level'],
        
    ))
    cursor.execute('''
        INSERT INTO salary (
            currency,min_value, max_value,unit
        )
        VALUES (?, ?, ?)
    ''', (
        donnees_transformees['salary']['currency'],
        donnees_transformees['salary']['min_value'],
        donnees_transformees['salary']['max_value'],
        donnees_transformees['salary']['unit'],
        
    ))
    cursor.execute('''
        INSERT INTO salary (
            country,locality,region,postal_code,street_address,latitude,longitude,
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (
        donnees_transformees['location']['country'],
        donnees_transformees['location']['locality'],
        donnees_transformees['location']['region'],
        donnees_transformees['location']['postal_code'],
        donnees_transformees['location']['street_address'],
        donnees_transformees['location']['latitude'],
        donnees_transformees['location']['longitude']
        
    ))
    conn.commit()

# Fermer la connexion à la base de données
conn.close()
