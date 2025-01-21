from airflow.decorators import task
from utils.logger import get_logger
from utils.data_cleaner import clean_job_data

logger = get_logger(__name__)

@task()
def transform_data():
    """Clean and transform extracted data.
    
    Returns:
        dict: Transformed data ready for loading
    """