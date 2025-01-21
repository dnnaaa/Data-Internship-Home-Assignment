from airflow.decorators import task
from utils.logger import get_logger
from utils.data_extractor import extract_from_csv

logger = get_logger(__name__)

@task()
def extract_data():
    """Extract data from jobs.csv.
    
    Returns:
        pd.DataFrame: Raw data extracted from jobs.csv
    """

    extract_from_csv()