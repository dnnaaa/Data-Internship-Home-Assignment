from datetime import timedelta
import os
import pandas as pd
from airflow.decorators import task

@task()
def extract():
    """Extract data from jobs.csv."""
    data = pd.read_csv('source/jobs.csv')
    
    extracted_dir = 'staging/extracted'
    os.makedirs(extracted_dir, exist_ok=True)

    
    for index, item in enumerate(data['context']):
        with open(f'{extracted_dir}/data_{index}.txt', 'w') as file:
            file.write(str(item))

