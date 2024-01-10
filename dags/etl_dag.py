from datetime import timedelta, datetime
from airflow.decorators import dag
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.operators.python import PythonOperator
from extract import extract_task
from transform import transform_task
from load import load_task


TABLES_CREATION_QUERY ={"job" : """CREATE TABLE IF NOT EXISTS job (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                title VARCHAR(225),
                                industry VARCHAR(225),
                                description TEXT,
                                employment_type VARCHAR(125),
                                date_posted DATE );""" , 
                        "company" : """CREATE TABLE IF NOT EXISTS company (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                job_id INTEGER,
                                name VARCHAR(225),
                                link TEXT,
                                FOREIGN KEY (job_id) REFERENCES job(id) );""" , 

                        "education" : """CREATE TABLE IF NOT EXISTS education (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                job_id INTEGER,
                                required_credential VARCHAR(225),
                                FOREIGN KEY (job_id) REFERENCES job(id)  );""" , 

                        "experience" : """CREATE TABLE IF NOT EXISTS experience (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                job_id INTEGER,
                                months_of_experience INTEGER,
                                seniority_level VARCHAR(25),
                                FOREIGN KEY (job_id) REFERENCES job(id) );""" ,

                        "salary" : """CREATE TABLE IF NOT EXISTS salary (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                job_id INTEGER,
                                currency VARCHAR(3),
                                min_value NUMERIC,
                                max_value NUMERIC,
                                unit VARCHAR(12),
                                FOREIGN KEY (job_id) REFERENCES job(id) );""" , 

                        "location" : """CREATE TABLE IF NOT EXISTS location (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                job_id INTEGER,
                                country VARCHAR(60),
                                locality VARCHAR(60),
                                region VARCHAR(60),
                                postal_code VARCHAR(25),
                                street_address VARCHAR(225),
                                latitude NUMERIC,
                                longitude NUMERIC,
                                FOREIGN KEY (job_id) REFERENCES job(id) );""" }

DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

@dag(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)
def etl_dag():
    """ETL pipeline"""

    location_table = SqliteOperator(
        task_id="location_table",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY['location'])

    salary_table = SqliteOperator(
        task_id="salary_table",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY['salary'])
    

    experience_table = SqliteOperator(
        task_id="experience_table",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY['experience'])

    education_table = SqliteOperator(
        task_id="education_table",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY['education'])

    company_table = SqliteOperator(
        task_id="company_table",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY['company'])

    job_table = SqliteOperator(
        task_id="job_table",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY['job'])

    extract_job = PythonOperator(
        task_id="extract",
        python_callable=extract_task)

    transform_job = PythonOperator(
        task_id="transform",
        python_callable=transform_task)

    load_job = PythonOperator(
        task_id="loady",
        python_callable=load_task)

    [location_table, salary_table, experience_table, education_table, company_table, job_table] \
     >> extract_job >> transform_job >> load_job

etl_dag()
