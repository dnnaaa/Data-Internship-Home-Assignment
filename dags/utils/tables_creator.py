import sqlite3
from utils.constants import TABLES_CREATION_QUERY

def create_database_schema(conn: sqlite3.Connection) -> None:
    """Create all tables with relationships"""
    cursor = conn.cursor()
    cursor.executescript(TABLES_CREATION_QUERY)
    conn.commit()