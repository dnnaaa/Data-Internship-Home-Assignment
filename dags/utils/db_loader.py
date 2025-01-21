def load_table_data(hook, table_name, data):
    """Load data into specified table using the provided database hook.
    
    Args:
        hook: SQLite hook instance
        table_name (str): Name of the target table
        data (pd.DataFrame): Data to be loaded
    """
    connection = hook.get_conn()
    cursor = connection.cursor()
    
    try:
        data.to_sql(
            table_name,
            connection,
            if_exists='append',
            index=False
        )
        connection.commit()
    finally:
        cursor.close()
        connection.close()