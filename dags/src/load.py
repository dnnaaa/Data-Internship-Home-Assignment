import os
# from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import src.constants as constants
import src.io_utils as io_utils
import src.my_sqlite as my_sqlite_class

def load_data():
    my_sqlite = my_sqlite_class.MySQLite()
    conn = my_sqlite.create_connection(constants.DB_PATH)
    my_sqlite.create_tables(conn)

    data = read_load_data()
    my_sqlite.insert_data_to_db(conn, data)

def read_load_data() -> dict:
    """
    Reads the data to load.
    
    Args:
        None
    
    Returns:
        dictionary of data for each table.
    """
    json_files = os.listdir(constants.OUTPUT_DIR_TRANSFORMED)
    json_files_len = len(json_files)
    print(f"loading {json_files_len} json files")

    data_dictionary = {
        "job": [],
        "company": [],
        "education": [],
        "experience": [],
        "salary": [],
        "location": []
    }
    
    cols_dictionary = {}
    for i, file_name in enumerate(json_files):
        print(file_name)
        file_path = os.path.join(constants.OUTPUT_DIR_TRANSFORMED, file_name)
        data = io_utils.read_json(file_path)

        for k in data_dictionary.keys():
            data_dictionary[k].append(
                tuple(data[k].values())
            )

            if i == 0:
                cols_dictionary[k] = list(data[k].keys())
        
        if i > 0 and i % 100 == 0:
            print(f"loaded {i+1}/{json_files_len} files")

    return data_dictionary

    # for k in data_dictionary.keys():
    #     print(f"Inserting data into table {k}")
    #     sqlite_hook.insert_rows(table=k, rows=data_dictionary[k], target_fields=cols_dictionary[k])