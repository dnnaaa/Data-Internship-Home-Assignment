import pandas as pd
import os
from airflow.decorators import task

@task()
def extract_task():
    """Extract data from jobs.csv."""
    # Update the input path to the container path
    input_path = '/usr/local/airflow/dags/jobs.csv'

    # Read the CSV file
    df = pd.read_csv(input_path)

    # Define the output directory for the extracted context files
    output_dir = "staging/extracted"
    os.makedirs(output_dir, exist_ok=True)

    # Iterate over the 'context' column and save each row as a text file
    for idx, context in enumerate(df['context']):
        output_path = os.path.join(output_dir, f"context_{idx}.txt")
        with open(output_path, 'w') as f:
            f.write(str(context))
        # Assuming context contains float or non-string data

