from datetime import timedelta, datetime
import sys
sys.path.append("/workspaces/Data-Internship-Home-Assignment/")
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from etl_process_operations.operations import TABLES_CREATION_QUERY, extract_data, transform_data, load_data

@task()
def extract():
    """Extract data from jobs.csv."""
    extract_data()

@task()
def transform():
    """Clean and convert extracted elements to json."""
    transform_data()

@task()
def load():
    """Load data to sqlite database."""
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    
    #create a connection from the hook
    sql_connection=sqlite_hook.get_conn()
    
    #loading the data into the sqlite default db
    load_data(sql_connection)

    #execute a query to check either the the loading is done successfully
    print(sql_connection.execute("select title from job").fetchall()[0:3])

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
    
    #create each table separately
    tables_creation_queries=TABLES_CREATION_QUERY.split(";") #the statement are separated using semicolone (;)
    
    # Here is a list to store the tasks for table creation
    create_tables =[]
    tables=["job", "company","education", "experience", "salary", "location"]
    for table, query in zip(tables, tables_creation_queries):
        create_tab=SqliteOperator(
            task_id=f"create_tables_{table}",
            sqlite_conn_id="sqlite_default",
            sql=f"{query};"
        )
        #add the task to the list
        create_tables.append(create_tab)
    create_tables >> extract() >> transform() >> load()
etl_dag()
