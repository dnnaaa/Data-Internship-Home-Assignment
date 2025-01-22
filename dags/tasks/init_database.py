import sqlite3
from pathlib import Path
from airflow.decorators import task
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@task()
def init_database(
    sql_path: str = "sqlQueries/create_tables.sql",
    db_path: str = "database/jobs.db"
):
    """
    Initialize SQLite database with tables defined in SQL file.
    
    Args:
        sql_path: Path to the SQL file containing CREATE TABLE statements
        db_path: Path where the SQLite database should be created
    """
    try:
        # Check if SQL file exists
        if not Path(sql_path).exists():
            raise FileNotFoundError(f"SQL file not found at: {sql_path}")
            
        # Read SQL statements from file
        logger.info(f"Reading SQL from {sql_path}")
        with open(sql_path, 'r') as sql_file:
            sql_content = sql_file.read()
            
        # Ensure the database directory exists
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initializing database at {db_path}")
        
        # Connect to SQLite database (creates it if it doesn't exist)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Split the SQL into individual statements and execute each one
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        for statement in statements:
            try:
                cursor.execute(statement)
                logger.info(f"Successfully executed: {statement[:50]}...")
            except sqlite3.Error as e:
                logger.error(f"Error executing statement: {statement[:50]}...")
                logger.error(f"Error details: {str(e)}")
                raise
        
        # Commit the changes
        conn.commit()
        logger.info("Database initialization completed successfully")
        
        # Verify tables were created
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        logger.info(f"Created tables: {[table[0] for table in tables]}")
        
        return True
        
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {str(e)}")
        raise
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise
        
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")