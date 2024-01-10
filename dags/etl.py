from datetime import timedelta, datetime
import glob
import pandas as pd
import os
import json
import sqlite3
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from extract import extract 
from transform import transform
from load import load
from create_tables import create_tables


DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

@dag(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)
def etl_dag():
    """ETL pipeline"""

    
    create_tables_tasks =create_tables()
    create_tables_tasks >> extract() >> transform() >> load()
    

etl_dag()
