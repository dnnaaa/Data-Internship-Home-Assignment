from airflow.decorators import task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from utils.logger import get_logger
from utils.db_loader import load_table_data

logger = get_logger(__name__)

@task()
def load_data():
    """Load transformed data into SQLite database."""
    