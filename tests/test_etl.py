import os
import json
import pytest
import sqlite3
import pandas as pd

df = pd.read_csv('source/jobs.csv', index_col=0)
df = df.dropna().drop_duplicates()
df.drop(df[df['context'] == '{}'].index, inplace=True)
df.reset_index(drop=True, inplace=True)
number_of_expected_files = len(df)

def test_extract_task():

    staging_path = 'staging/extracted'
    
    # Check if the number of files in the folder is equal to number_of_expected_files
    files = os.listdir(staging_path)
    assert len(files) == number_of_expected_files, \
        f"Extract Test failed: The number of files {len(files)} is different than what is expected ({number_of_expected_files})."

    # Check if all files have the ".txt" extension
    assert all(file.endswith('.txt') for file in files), \
        "Extract Test failed: Not all files are text as expected ."

    print(f"Extract Test passed: The number of files is {number_of_expected_files} and all of them are text files as expected.")


def test_transform_task():

    transform_path = 'staging/transformed'
    
    # Check if the number of files in the folder is equal to number_of_expected_files
    files = os.listdir(transform_path)
    assert len(files) == number_of_expected_files, \
        f"Transform Test failed: The number of files {len(files)} is different than what is expected ({number_of_expected_files})."

    # Check if all files have the ".json" extension
    assert all(file.endswith('.json') for file in files), \
        "Transform Test failed: Not all files are json as expected ."

    # Check if the structure of each transformed file is correct
    for i in range(3):
        with open(f"staging/transformed/transformed_file_{i}.json", "r") as f:
            transformed_data = json.load(f)
        # test fields in the json file
        assert set(transformed_data.keys()) == set(["job", "company", "education", "experience", "salary", "location"]), \
            "Transform Test failed: the structure is not respected."
        # test subfields of the job subfield 
        assert set(transformed_data['job'].keys()) == set(["title", "industry", "description", "employment_type", "date_posted"]), \
            "Transform Test failed: the structure is not respected."
        # test subfields of the company subfield         
        assert set(transformed_data['company'].keys()) == set(["name", "link"]), \
            "Transform Test failed: the structure is not respected."
        # test subfields of the education subfield 
        assert set(transformed_data['education'].keys()) == set(["required_credential"]), \
            "Transform Test failed: the structure is not respected."
        # test subfields of the experience subfield 
        assert set(transformed_data['experience'].keys()) == set(["months_of_experience", "seniority_level"]), \
            "Transform Test failed: the structure is not respected."
        # test subfields of the salary subfield 
        assert set(transformed_data['salary'].keys()) == set(["currency", "min_value", "max_value", "unit"]), \
            "Transform Test failed: the structure is not respected."
        # test subfields of the location subfield 
        assert set(transformed_data['location'].keys()) == set(["country", "locality", "region", "postal_code", "street_address", "latitude", "longitude"]), \
            "Transform Test failed: the structure is not respected."

    print(f"Transform Tests passed .")


def test_load_task():

    # Connect to the SQLite database
    conn = sqlite3.connect('sqlite_default.db')
    cursor = conn.cursor()

    try:
        # Check if we have 6 tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [i[0] for i in cursor.fetchall()[1:]]
        assert len(tables) == 6, "Load Test failed: Expected 6 tables in the database."
        print("load test1 passed: we find 6 tables in the database")

        # Check if the total number of rows in each table is equal to number_of_expected_files
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table};")
            row_count = cursor.fetchone()[0]
            assert row_count == number_of_expected_files, f"Load Test failed: Expected {number_of_expected_files} rows in table {table[0]}."

        print("Load test2 passed: the number of rows in all tables is as expected")

        # Check columns of the job table 
        cursor.execute("PRAGMA table_info(job);")
        job_columns = [column[1] for column in cursor.fetchall()]
        expected_job_columns = ["id", "title", "industry", "description", "employment_type", "date_posted"]
        assert job_columns == expected_job_columns, "Load Test failed: Columns of the job table are not as expected."
        print("Load test3 passed: Columns of the job table are as expected.")

        # Check columns of the company table 
        cursor.execute("PRAGMA table_info(company);")
        job_columns = [column[1] for column in cursor.fetchall()]
        expected_job_columns = ["id", "job_id", "name", "link"]
        assert job_columns == expected_job_columns, "Load Test failed: Columns of the company table are not as expected."
        print("Load test4 passed: Columns of the company table are as expected.")

        # Check columns of the education table 
        cursor.execute("PRAGMA table_info(education);")
        job_columns = [column[1] for column in cursor.fetchall()]
        expected_job_columns = ["id", "job_id", "required_credential"]
        assert job_columns == expected_job_columns, "Load Test failed: Columns of the education table are not as expected."
        print("Load test5 passed: Columns of the education table are as expected.")

        # Check columns of the experience table 
        cursor.execute("PRAGMA table_info(experience);")
        job_columns = [column[1] for column in cursor.fetchall()]
        expected_job_columns = ["id", "job_id", "months_of_experience", "seniority_level"]
        assert job_columns == expected_job_columns, "Load Test failed: Columns of the experience table are not as expected."
        print("Load test6 passed: Columns of the experience table are as expected.")

        # Check columns of the salary table 
        cursor.execute("PRAGMA table_info(salary);")
        job_columns = [column[1] for column in cursor.fetchall()]
        expected_job_columns = ["id", "job_id", "currency", "min_value", "max_value", "unit"]
        assert job_columns == expected_job_columns, "Load Test failed: Columns of the salary table are not as expected."
        print("Load test7 passed: Columns of the salary table are as expected.")

        # Check columns of the location table 
        cursor.execute("PRAGMA table_info(location);")
        job_columns = [column[1] for column in cursor.fetchall()]
        expected_job_columns = ["id", "job_id", "country", "locality", "region", "postal_code", "street_address","latitude", "longitude"]
        assert job_columns == expected_job_columns, "Load Test failed: Columns of the location table are not as expected."
        print("Load test8 passed: Columns of the location table are as expected.")

        print("Load Tests passed.")
    
    finally:
        # Close the database connection
        conn.close()


# Run the test function
test_extract_task()
test_transform_task()
test_load_task()



