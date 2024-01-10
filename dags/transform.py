import json
import os
from bs4 import BeautifulSoup  # Assurez-vous d'avoir installé la bibliothèque BeautifulSoup

def transformDATA():
    # Fonction pour nettoyer la description du travail (supprimer les balises HTML)
    def nettoyer_description(description_html):
        soup = BeautifulSoup(description_html, 'html.parser')
        return soup.get_text()
    # Chemin du répertoire des fichiers extrait
    dossier_extrait = 'staging/extracted'

    # Chemin du répertoire de sortie des fichiers transformés
    dossier_transformé = 'staging/transformed'
    os.makedirs(dossier_transformé, exist_ok=True)
    # Parcourir tous les fichiers dans le dossier extrait
    for fichier_extrait in os.listdir(dossier_extrait):
        chemin_fichier_extrait = os.path.join(dossier_extrait, fichier_extrait)
        print(os.path.splitext(fichier_extrait)[0])
        # Lire le contenu du fichier extrait
        with open(chemin_fichier_extrait, 'r', encoding='utf-8') as fichier:
            contenu = fichier.read()
            # Vérifier si le contenu est vide
            if not contenu.strip() or contenu.strip() == "nan":
                print(f"Le fichier {fichier_extrait} est vide. Ignoré.")
                continue
            # Convertir le contenu du fichier extrait en JSON
            données_extraites = json.loads(contenu)
            # Nettoyer la description du travail
            #description_propre = nettoyer_description(données_extraites['description'])
            description_propre = nettoyer_description(données_extraites.get('description', ''))
            # Créer le nouveau schéma de données transformé
            données_transformées = {
                "job": {
                    "title": données_extraites.get('title', ''),
                    "industry": données_extraites.get('industry', ''),
                    "description": description_propre,
                    "employment_type": données_extraites.get('employmentType', ''),
                    "date_posted": données_extraites.get('datePosted', ''),
                    },
                "company": {
                    "name": données_extraites.get('hiringOrganization', {}).get('name', ''),
                    "link": données_extraites.get('hiringOrganization', {}).get('sameAs', ''),
                    },
                "education": {
                    "required_credential": données_extraites.get('educationRequirements', {}).get('credentialCategory', ''),
                    },
                "experience": {
                    "months_of_experience": données_extraites.get('experienceRequirements', {}).get('monthsOfExperience', ''),
                    "seniority_level": données_extraites.get('experienceRequirements', {}).get('monthsOfExperience', ''),
                    },
                "salary": {
                    "currency": données_extraites.get('estimatedSalary', {}).get('currency', {}),
                    "min_value": données_extraites.get('estimatedSalary', {}).get('value', {}).get('minValue', ''),
                    "max_value": données_extraites.get('estimatedSalary', {}).get('value', {}).get('maxValue', ''),
                    "unit": données_extraites.get('estimatedSalary', {}).get('value', {}).get('unitText', ''),
                    },
                "location": {
                    "country": données_extraites.get('jobLocation', {}).get('address', {}).get('addressCountry', ''),
                    "locality": données_extraites.get('jobLocation', {}).get('address', {}).get('addressLocality', ''),
                    "region": données_extraites.get('jobLocation', {}).get('address', {}).get('addressRegion', ''),
                    "postal_code": données_extraites.get('jobLocation', {}).get('address', {}).get('postalCode', ''),
                    "street_address": données_extraites.get('jobLocation', {}).get('address', {}).get('streetAddress', ''),
                    "latitude": données_extraites.get('jobLocation', {}).get('latitude', ''),
                    "longitude": données_extraites.get('jobLocation', {}).get('longitude', ''),
                    },
                }

            # Créer le nom du fichier de sortie
            nom_fichier_transformé = os.path.splitext(fichier_extrait)[0] + '_transformed.json'
            chemin_fichier_transformé = os.path.join(dossier_transformé, nom_fichier_transformé)

            # Enregistrer les données transformées au format JSON dans un nouveau fichier
            with open(chemin_fichier_transformé, 'w', encoding='utf-8') as fichier_transformé:
                json.dump(données_transformées, fichier_transformé, indent=2)
    


