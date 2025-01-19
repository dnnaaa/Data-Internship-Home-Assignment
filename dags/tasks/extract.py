import pandas as pd
import json
import html


def clean_json(json_str):
    """
    Nettoie et convertit une chaîne de caractères JSON en objet Python, en gérant les entités HTML et les erreurs.
    """
    # Vérification si la chaîne est vide ou de type non string
    if not isinstance(json_str, str) or not json_str.strip():
        return None  # Retourner None si ce n'est pas une chaîne ou si elle est vide
    
    # Décoder les entités HTML comme &lt;, &amp;, etc.
    clean_str = html.unescape(json_str)
    
    try:
        # Essayer de convertir la chaîne nettoyée en JSON
        return json.loads(clean_str)
    except json.JSONDecodeError:
        # Retourner None si la chaîne ne peut pas être décodée en JSON
        return None


def extract_data():
    """
    Extrait les données depuis un fichier CSV, nettoie la colonne 'context' et enregistre les résultats dans des fichiers texte.
    """
    # Lecture du fichier CSV
    df = pd.read_csv('/opt/airflow/source/jobs.csv')
    
    # Vérification que la colonne 'context' existe
    if 'context' not in df.columns:
        raise ValueError("La colonne 'context' n'existe pas dans le fichier CSV.")
    
    # Nettoyage des données dans la colonne 'context'
    df['context_cleaned'] = df['context'].apply(clean_json)
    
    # Sauvegarder les objets JSON extraits dans des fichiers texte
    for index, row in df.iterrows():
        context_data = row['context_cleaned']
        if context_data:
            # Sauvegarder chaque enregistrement dans un fichier texte, un par un
            with open(f'/opt/airflow/staging/extracted/{index}.txt', 'w') as f:
                json.dump(context_data, f)

    print(f"Extraction terminée. {len(df)} fichiers extraits.")
