from airflow.decorators import task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from utils.logger import get_logger
from utils.db_loader import load_jobs_to_database

logger = get_logger(__name__)

@task()
def load_data():
    """Load transformed data into SQLite database."""
    load_jobs_to_database()
    print("Data successfully loaded into normalized SQLite database")