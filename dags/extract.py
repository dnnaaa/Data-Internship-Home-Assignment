import pandas as pd
import os
from airflow.decorators import task

@task()
def extract():
    """Extract data from jobs.csv."""
    df = pd.read_csv('/opt/airflow/data/source/jobs.csv')
    os.makedirs('/opt/airflow/data/staging/extracted', exist_ok=True)
    for index, row in df.iterrows():
        with open(f'/opt/airflow/data/staging/extracted/{index}.txt', 'w') as f:
            f.write(str(row['context']))

            