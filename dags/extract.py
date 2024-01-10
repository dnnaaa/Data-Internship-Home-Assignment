from airflow.decorators import task

import pandas as pd
import os

source_file_path = 'source/jobs.csv'
extracted_data_folder_path = 'staging/extracted'

@task()
def extract():
    """Extract data from jobs.csv."""

    # Create the 'extracted_data_folder_path' folder if it does not exist
    os.makedirs(extracted_data_folder_path, exist_ok=True)

    # Load the DataFrame from the CSV file
    data = pd.read_csv(source_file_path)

    # Drop rows with missing values
    data_without_missing_values = data.dropna()

    # Extract the 'context' column and save each element in a text file
    for index, row in data_without_missing_values.iterrows():
        context = str(row['context'])
        file_name = f"{extracted_data_folder_path}/extracted_{index}.txt"

        with open(file_name, 'w', encoding='utf-8') as file:
            file.write(context)