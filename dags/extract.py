from airflow.decorators import task
import pandas as pd 
import os

@task()
def extract(source_file, staging_dir):
    """Extract data from jobs.csv."""
    # Read the CSV file
    df = pd.read_csv(source_file)

    # Ensure the 'staging/extracted' directory exists
    os.makedirs(staging_dir, exist_ok=True)

    
    # Extract the context column data and save each item to a text file
    for index, row in df.iterrows():
        context_data = row['context']
        
        # Generate a unique filename based on the index or any other identifier
        filename = f'context_{index}.txt'
        
        # Save context_data to 'staging/extracted' as a text file
        file_path = os.path.join(staging_dir, filename)
        
        with open(file_path, 'w') as file:
            file.write(context_data)
        
        

    return staging_dir

