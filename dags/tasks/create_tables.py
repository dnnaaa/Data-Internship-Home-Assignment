from airflow.decorators import task
from utils.logger import get_logger
from utils.tables_creator import connect_and_create_tables

logger = get_logger(__name__)

@task()
def create_schema():
    """Extract data from jobs.csv.
    
    Returns:
        pd.DataFrame: Raw data extracted from jobs.csv
    """

    connect_and_create_tables()