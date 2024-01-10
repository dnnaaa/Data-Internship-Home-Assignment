from datetime import timedelta

TABLES_CREATION_QUERY = {
    "job": """
        CREATE TABLE IF NOT EXISTS job (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title VARCHAR(225),
            industry VARCHAR(225),
            description TEXT,
            employment_type VARCHAR(125),
            date_posted DATE
        );
    """,
    "company": """
        CREATE TABLE IF NOT EXISTS company (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER,
            name VARCHAR(225),
            link TEXT,
            FOREIGN KEY (job_id) REFERENCES job(id)
        );
    """,
    "education": """
        CREATE TABLE IF NOT EXISTS education (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER,
            required_credential VARCHAR(225),
            FOREIGN KEY (job_id) REFERENCES job(id)
        );
    """,
    "experience": """
        CREATE TABLE IF NOT EXISTS experience (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER,
            months_of_experience INTEGER,
            seniority_level VARCHAR(25),
            FOREIGN KEY (job_id) REFERENCES job(id)
        );
    """,
    "salary": """
        CREATE TABLE IF NOT EXISTS salary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER,
            currency VARCHAR(3),
            min_value NUMERIC,
            max_value NUMERIC,
            unit VARCHAR(12),
            FOREIGN KEY (job_id) REFERENCES job(id)
        );
    """,
    "location": """
        CREATE TABLE IF NOT EXISTS location (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER,
            country VARCHAR(60),
            locality VARCHAR(60),
            region VARCHAR(60),
            postal_code VARCHAR(25),
            street_address VARCHAR(225),
            latitude NUMERIC,
            longitude NUMERIC,
            FOREIGN KEY (job_id) REFERENCES job(id)
        );
    """
}

CSV_FILE_PATH = 'source/jobs.csv'
TEST_FILE_PATH = 'source/test.csv'

DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

EXTRACTION_PATH = 'staging/extracted'
TRANSFORMATION_PATH = 'staging/transformed'
