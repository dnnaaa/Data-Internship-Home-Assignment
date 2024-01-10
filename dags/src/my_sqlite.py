import sqlite3
import src.queries as queries

class MySQLite:
    def __init__(self) -> None:
        pass

    def create_connection(self, db_path: str) -> sqlite3.Connection:
        """
        Creates a SQLite database at the specified path.

        Args:
            db_path: The file path where the SQLite database will be created.

        Returns:
            sqlite3.Connection: The connection object to the created SQLite database.
        """
        conn = sqlite3.connect(db_path)

        return conn

    def create_tables(self, conn: sqlite3.Connection) -> None:
        """
        Create the tables for storing the transformed data.

        The function creates several tables to store different aspects of job data, such as
        job details, company information, education requirements, experience requirements,
        salary data, and location details. Each table includes an 'id' column as a primary key
        and foreign keys where necessary to maintain relationships between tables.

        Args:
            db_path: The file path where the SQLite database will be created.

        Returns:
            sqlite3.Connection: The connection object to the created SQLite database.
        """
        cursor = conn.cursor()
        
        for query in queries.TABLES_CREATION_QUERIES:
            cursor.execute(query)
            conn.commit()
        
        return conn

    def insert_data_to_db(self, conn: sqlite3.Connection, data: dict) -> None:
        """
        Inserts data into a SQLite database based on the provided JSON structure.

        This function handles the insertion of data into multiple tables including job,
        company, education, experience, salary, and location. The data dictionary is expected
        to contain keys that correspond to the columns of these tables.

        Args:
            conn: The connection object to the SQLite database.
            data: The data to be inserted into the database. The dictionary
                        should contain keys and values matching the table columns.

        Returns:
            None: This function does not return anything. It inserts data into the database.
        """
        cursor = conn.cursor()

        # Insert data into the 'job' table and retrieve the job_id
        cursor.execute(
            """
            INSERT INTO job (title, industry, description, employment_type, date_posted)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                data["job"]["title"],
                data["job"]["industry"],
                data["job"]["description"], 
                data["job"]["employment_type"],
                data["job"]["date_posted"]
            )
        )
        job_id = cursor.lastrowid

        # Insert data into the "company" table
        cursor.execute(
            """
            INSERT INTO company (job_id, name, link) 
            VALUES (?, ?, ?)
            """,
            (
                job_id,
                data["company"]["name"],
                data["company"]["link"]
            )
        )

        # Insert data into the "education" table
        cursor.execute(
            """
            INSERT INTO education (job_id, required_credential) 
            VALUES (?, ?)
            """,
            (
                job_id,
                data["education"]["required_credential"]
            )
        )

        # Insert data into the "experience" table
        cursor.execute(
            """
            INSERT INTO experience (job_id, months_of_experience, seniority_level) 
            VALUES (?, ?, ?)
            """,
            (
                job_id,
                data["experience"]["months_of_experience"],
                data["experience"]["seniority_level"]
            )
        )

        # Insert data into the "salary" table
        cursor.execute(
            """
            INSERT INTO salary (job_id, currency, min_value, max_value, unit) 
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                job_id,
                data["salary"]["currency"],
                data["salary"]["min_value"], 
                data["salary"]["max_value"],
                data["salary"]["unit"]
            )
        )

        # Insert data into the "location" table
        cursor.execute(
            """
            INSERT INTO location (job_id, country, locality, region, postal_code, street_address, latitude, longitude) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                data["location"]["country"],
                data["location"]["locality"],
                data["location"]["region"],
                data["location"]["postal_code"],
                data["location"]["street_address"],
                data["location"]["latitude"],
                data["location"]["longitude"]
            )
        )

        conn.commit()

