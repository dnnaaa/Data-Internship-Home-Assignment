# tasks/create_tables_task.py
from airflow.decorators import task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

@task()
def create_tables(sqlite_conn_id, tables_creation_query):
    """Create tables in the SQLite database."""
    sqlite_hook = SqliteHook(sqlite_conn_id=sqlite_conn_id)
    connection = sqlite_hook.get_conn()
    cursor = connection.cursor()

    # Split the query into individual statements
    statements = tables_creation_query.split(';')
    for statement in statements:
        if statement.strip():  # Ensure the statement is not empty
            cursor.execute(statement)

    connection.commit()
    connection.close()
