import pandas as pd
import os
from airflow.decorators import task

@task()
def extract():
    """Extract data from jobs.csv and save context column data to text files."""
    # Ensure the source file exists
    source_file = 'source/jobs.csv'
    if not os.path.exists(source_file):
        raise FileNotFoundError(f"The file {source_file} does not exist.")

    # Read the DataFrame
    df = pd.read_csv(source_file)

    # Ensure the directory for extracted data exists
    extracted_dir = 'staging/extracted'
    os.makedirs(extracted_dir, exist_ok=True)

    # Extract context column data and save each item to a text file
    for index, row in df.iterrows():
        context_data = row.get('context', '')  # Using .get to avoid KeyError if 'context' column is missing
        if context_data:  # Ensure there is data to write
            file_path = os.path.join(extracted_dir, f'context_{index}.txt')
            with open(file_path, 'w') as file:
                file.write(str(context_data))
