from airflow.providers.sqlite.hooks.sqlite import SqliteHook

class DatabaseHandler:
    def __init__(self, sqlite_conn_id='sqlite_default'):
        self.sqlite_hook = SqliteHook(sqlite_conn_id)

    def execute_query(self, query):
        self.sqlite_hook.run(query)
