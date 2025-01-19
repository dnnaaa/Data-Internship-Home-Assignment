from airflow import DAG
from airflow.operators.python import PythonOperator
from tasks.extract import extract_data
from tasks.transform import transform_data
from tasks.load import load_data
from datetime import datetime

# Arguments par défaut
default_args = {
    'owner': 'airflow', 
    'start_date': datetime(2025, 1, 1),  
    'retries': 1,  
    'catchup': False  
}

# Création du DAG
with DAG(
    'etl_pipline', 
    default_args=default_args,  
    schedule_interval=None,  
    catchup=False  
) as dag:
    
    # Tâche d'extraction
    extract_task = PythonOperator(
        task_id='extract_data',  
        python_callable=extract_data,  
    )
    
    # Tâche de transformation
    transform_task = PythonOperator(
        task_id='transform_data',  
        python_callable=transform_data,  
    )
    
    # Tâche de chargement
    load_task = PythonOperator(
        task_id='load_data', 
        python_callable=load_data,  
    )
    
    # Ordre d'exécution
    extract_task >> transform_task >> load_task