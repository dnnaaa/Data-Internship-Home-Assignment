import os
import json
import logging
import pandas as pd

def extract_data():
    csv_path = '/home/anas/Data-Internship-Home-Assignment/source/jobs.csv'
    output_folder = '/home/anas/Data-Internship-Home-Assignment/staging/extracted'
    """Extract data from jobs.csv and process 'context' column."""
    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)

    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        logging.error(f"File not found: {csv_path}")
        return []

    logging.info(f"Number of records in the CSV: {df.shape[0]}")

    extracted_files = []

    # Process the 'context' column and save as text files
    for index, row in df.iterrows():
        context_data = row['context']
        
        # Ensure context_data is a string before attempting to load it as JSON
        if isinstance(context_data, float):
            # Handle cases where context_data is a float (not a valid JSON)
            logging.warning(f"Skipping invalid context data at row {index}: {context_data}")
            continue

        try:
            # Try to parse context_data as JSON
            context_json = json.loads(context_data)
        except (json.JSONDecodeError, TypeError) as e:
            # Handle cases where JSON decoding fails
            logging.error(f"Failed to parse context data at row {index}: {context_data} - Error: {e}")
            continue
        
        # Save the JSON data as a text file
        output_file_path = os.path.join(output_folder, f'job_{index}.txt')
        try:
            with open(output_file_path, 'w') as output_file:
                json.dump(context_json, output_file, indent=4)
            logging.info(f"Saved: {output_file_path}")
            extracted_files.append(output_file_path)
        except Exception as e:
            logging.error(f"Failed to save file {output_file_path}: {e}")

    return extracted_files
