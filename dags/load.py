import pandas as pd
import os
import json
import sqlite3
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

transformed_data_folder_path = 'staging/transformed'


@task()
def load():
    """Load data to sqlite database."""
    # Connection to database
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    conn = sqlite_hook.get_conn()

    try:
        # Loop through JSON files in the folder
        for filename in os.listdir(transformed_data_folder_path):
            if filename.endswith('.json'):
                file_path = os.path.join(transformed_data_folder_path, filename)

                # Read JSON file
                with open(file_path, 'r', encoding='utf-8') as json_file:
                    json_data = json.load(json_file)

                # Extract relevant data from 'job' section
                job_data = json_data.get('job', {})
                title = job_data.get('title', None)
                industry = job_data.get('industry', None)
                description = job_data.get('description', None)
                employment_type = job_data.get('employment_type', None)
                date_posted = job_data.get('date_posted', None)

                # Insert data into 'job' table
                sql_insert = """
                INSERT INTO job (title, industry, description, employment_type, date_posted)
                VALUES (?, ?, ?, ?, ?)
                """

                with conn:
                    cursor = conn.execute(sql_insert, (title, industry, description, employment_type, date_posted))

                    # Retrieve the automatically generated ID (the last inserted row)
                    job_id = cursor.lastrowid

                    if job_id is not None:
                        # Tables
                        tables_to_process = ['company', 'education', 'experience', 'salary', 'location']

                        for table_name in tables_to_process:
                            table_data = json_data.get(table_name, {})

                            # The fields to extract for each table
                            fields_to_extract = {
                                'company': ['name', 'link'],
                                'education': ['required_credential'],
                                'experience': ['months_of_experience', 'seniority_level'],
                                'salary': ['currency', 'min_value', 'max_value', 'unit'],
                                'location': ['country', 'locality', 'region', 'postal_code', 'street_address', 'latitude', 'longitude']
                            }

                            # Extract the values of the specified fields for this table
                            values = [table_data.get(field, None) for field in fields_to_extract[table_name]]

                            # Insert the data into the specified table
                            if any(values):
                                target_fields = ['job_id'] + fields_to_extract[table_name]
                                target_values = [job_id] + values
                                sql_insert_table = f"""
                                INSERT INTO {table_name} ({', '.join(target_fields)})
                                VALUES ({', '.join(['?' for _ in target_fields])})
                                """
                                conn.execute(sql_insert_table, target_values)

    except Exception as e:
        print(f"Error: {str(e)}")

    finally:
        pass