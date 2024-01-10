import pandas as pd
import os
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@task()
def extract():
    """Extract data from jobs.csv."""
    data = pd.read_csv('source/jobs.csv')
    data = data.dropna()
    extracted_dir = 'staging/extracted'
    
    for index, row in data.iterrows():
        context = str(row['context'])
        file_name = f"{index}.txt"
        file_path = os.path.join(extracted_dir, file_name)
        
        with open(file_path, 'w',encoding='utf-8') as file:
            file.write(context)