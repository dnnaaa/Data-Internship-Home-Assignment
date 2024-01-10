import pandas as pd
import os

def extract():
    # Chemin du fichier CSV source
    chemin_fichier_csv = '../source/jobs.csv'
    # Charger la DataFrame depuis le fichier CSv
    df = pd.read_csv(chemin_fichier_csv)
    
    # le répertoire de destination 
    dossier_destination = 'staging/extracted'
    os.makedirs(dossier_destination, exist_ok=True)
    # Nom du fichier de sortie (base du chemin du fichier source)
    nom_base_fichier_sortie = os.path.splitext(os.path.basename(chemin_fichier_csv))[0]
    # Extraire la colonne 'contexte' et sauvegarder chaque élément dans un fichier texte
    for index, row in df.iterrows():
        contexte = row['context']
        # Nom du fichier de sortie (en utilisant l'index comme suffixe)
        nom_fichier_sortie = f"{nom_base_fichier_sortie}_extracted_{index}.txt"
    
        # Chemin complet du fichier de sortie
        chemin_fichier_sortie = os.path.join(dossier_destination, nom_fichier_sortie)
    
        # Écrire le contenu de la colonne 'contexte' dans le fichier texte
        with open(chemin_fichier_sortie, 'w', encoding='utf-8') as fichier_sortie:
            fichier_sortie.write(str(contexte))


