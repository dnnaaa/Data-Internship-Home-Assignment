## DNA Engineering Data Assignment
# Zahra EL HADDI
# Implementation of an ETL pipelien using Airflow for extracting data from a csv file, cleaning and processing data and storing the processed data to a sqlite database 

## Table of content
- [Overview](#overview)
- [Starting Airflow](#starting-airflow)
- [Code Refactoring](#code-refactoring)
- [Unit Testing](#unit-testing)
- [Git Best Practices](#git-best-practices)
- [Conclusion](#conclusion)


## Overview
This project addresses the following objectives:
- Refactor the ETL code into clean, modular tasks.
- Implement the ETL tasks (Extract, Transform, Load).
- Unit test the pipeline.
- Showcase successful execution with evidence of the pipeline's results.

The project is structured as follows:

Data-Internship-Home-Assignment/
├── dags/
│   ├── etl_dag.py            # Main DAG script that orchestrates the ETL process
│   └── tasks/
│       ├── __init__.py       # Task module initializer
│       └── init_database.py  # Task for creating SQLite database and the tables
│       ├── extract.py        # Task for extracting data from source (e.g., CSV)
│       ├── transform.py      # Task for transforming the extracted data
│       └── load.py           # Task for loading data into SQLite database
│
├── source/
│   └── jobs.csv             # Source CSV file containing job data to be processed
|
├── staging/
│   ├── extracted/           # Directory where the extracted files are saved in TXT format
│   └── transformed/         # Directory for saving the transformed files in JSON format
|
├── sqlQueries/
│   └── __init__.py          # SQL module initializer
│   └── create_tables.sql    # SQL script to create the necessary tables in the SQLite database
|
├── database/
│   └── jobs.db              # SQLite database created by the init_database task
|
├── schemas/
│   └── __init__.py          # Schema module initializer
│   └── job_schema.py        # Python file defining the schema of the job
|
├── tests/
│   └── __init__.py          # Tests module initializer
│   ├── test_extract.py      # Unit test for the extract task
│   ├── test_transform.py    # Unit test for the transform task
│   └── test_load.py         # Unit test for the load task
│   └── conftest.py          # Configuration file for fixtures used in tests
|
└── requirements.txt         # Project dependencies (Added pytest, git-flow, sqlite3, bs4)

## Starting Airflow
![Starting Airflow Scheduler and Webserver using the command airflow standalone](result_images/start_airflow.png)


## Code Refactoring
The original ETL pipeline was a single, monolithic script (dags/etl.py). This made it difficult to read, maintain, and scale. The code has been refactored into modular components:

*DAG Script*: etl_dag.py orchestrates the tasks.
![Refactored Airflow Dag Executed Successfully](result_images/Dag_execution.png)

*Task Scripts*: Each task (Extract, Transform, Load) has its own Python file in tasks/.
*Database Initialization*: A separate script, init_database.py, handles database creation.
By adhering to clean code principles, the project is now modular, readable, and easy to maintain.

**ETL Tasks**
**Extract Job**
Objective: Extract data from source/jobs.csv, isolate the context column, and save each item as a text file in staging/extracted/.
Result: The extracted files were successfully created.

![extracted files stored into staging/extracted successfully](result_images/extracted.png)


**Transform Job**
Objective: 
Read the text files from staging/extracted/.
Clean the job descriptions.
Transform the data into the specified schema.
Save the transformed data as JSON files in staging/transformed/.
Result: Transformed JSON files were successfully created with the desired schema.

![Transformed files stored into staging/transformed successfully](result_images/transformed.png)


**Load Job**
Objective: Load the transformed JSON files from staging/transformed/ into a SQLite database.
Result: The SQLite tables were successfully populated.

![Data Inserted into SQlite tables successfully](result_images/DataInsertedTosqliteTables.png)


## Unit Testing
All tasks were unit tested using pytest. The tests ensure:

The Extract job correctly extracts data from the CSV file.
The Transform job produces JSON files matching the specified schema.
The Load job inserts data accurately into the SQLite database.

To run the tests:
pytest tests/


## Git Best Practices
**Commit Messages**:
Used descriptive commit messages following the format: <type>: <description>.
Examples:
feat: add extraction task for ETL pipeline
fix: resolve schema validation in transform job
test: add unit tests for load task
*Small, Logical Commits*: Ensured each commit was small and focused on a single change.
*Branching Strategy*: Used GitFlow for branching.


## Conclusion
This assignment posed some challenges in configuring the Linux environment on Windows using Ubuntu (WSL), but it provided a valuable learning experience as I worked through them. It was a rewarding opportunity to write code and enjoy the process. 
Thank you!

