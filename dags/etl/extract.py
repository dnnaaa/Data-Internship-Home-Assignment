import pandas as pd
import os

def extract_data(source_file, output_directory):
    """Extract data from the jobs.csv file."""
    # Lire le fichier CSV
    df = pd.read_csv(source_file)
    
    # Extraire la colonne 'context' qui contient les données JSON
    context_data = df['context']
    
    # Créer le répertoire de sortie s'il n'existe pas
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    
    # Enregistrer chaque élément de 'context' dans un fichier texte
    for i, context in enumerate(context_data):
        file_path = os.path.join(output_directory, f'extracted_{i}.txt')
        with open(file_path, 'w') as file:
            file.write(context)
