"""this module will contains the main function for the etl process"""
import json
import os

HOME_DIR = "/workspaces/Data-Internship-Home-Assignment"

TABLES_CREATION_QUERY = """CREATE TABLE IF NOT EXISTS job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(225),
    industry VARCHAR(225),
    description TEXT,
    employment_type VARCHAR(125),
    date_posted DATE
);

CREATE TABLE IF NOT EXISTS company (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    name VARCHAR(225),
    link TEXT,
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS education (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    required_credential VARCHAR(225),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS experience (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    months_of_experience INTEGER,
    seniority_level VARCHAR(25),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS salary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    currency VARCHAR(3),
    min_value NUMERIC,
    max_value NUMERIC,
    unit VARCHAR(12),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

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
)
"""

#JOB_TABLES_CREATION = """CREATE TABLE IF NOT EXISTS job (
#    id INTEGER PRIMARY KEY AUTOINCREMENT,
#    title VARCHAR(225),
#    industry VARCHAR(225),
#    description TEXT,
#    employment_type VARCHAR(125),
#    date_posted DATE
#);
#"""

#COMPANY_TABLES_CREATION = """CREATE TABLE IF NOT EXISTS company (
#    id INTEGER PRIMARY KEY AUTOINCREMENT,
#    job_id INTEGER,
#    name VARCHAR(225),
#    link TEXT,
#    FOREIGN KEY (job_id) REFERENCES job(id)
#);
#"""

#EDUCATION_TABLES_CREATION = """CREATE TABLE IF NOT EXISTS education (
#    id INTEGER PRIMARY KEY AUTOINCREMENT,
#    job_id INTEGER,
#    required_credential VARCHAR(225),
#    FOREIGN KEY (job_id) REFERENCES job(id)
#);
#"""

#EXPERIENCE_TABLES_CREATION = """CREATE TABLE IF NOT EXISTS experience (
#    id INTEGER PRIMARY KEY AUTOINCREMENT,
#    job_id INTEGER,
#    months_of_experience INTEGER,
#    seniority_level VARCHAR(25),
#    FOREIGN KEY (job_id) REFERENCES job(id)
#);
#"""

#SALARY_TABLES_CREATION = """CREATE TABLE IF NOT EXISTS salary (
#    id INTEGER PRIMARY KEY AUTOINCREMENT,
#    job_id INTEGER,
#    currency VARCHAR(3),
#    min_value NUMERIC,
#    max_value NUMERIC,
#    unit VARCHAR(12),
#    FOREIGN KEY (job_id) REFERENCES job(id)
#);
#"""

#LOCATION_TABLES_CREATION = """CREATE TABLE IF NOT EXISTS location (
#    id INTEGER PRIMARY KEY AUTOINCREMENT,
#    job_id INTEGER,
#    country VARCHAR(60),
#    locality VARCHAR(60),
##    region VARCHAR(60),
#    postal_code VARCHAR(25),
#    street_address VARCHAR(225),
#    latitude NUMERIC,
#    longitude NUMERIC,
#    FOREIGN KEY (job_id) REFERENCES job(id)
#)
#"""

#tables_creation_queries = (JOB_TABLES_CREATION, COMPANY_TABLES_CREATION, EDUCATION_TABLES_CREATION, EXPERIENCE_TABLES_CREATION, SALARY_TABLES_CREATION, LOCATION_TABLES_CREATION)

def is_valid_json(string):
    try:
        json.loads(string)
        return True
    except Exception:
        return False

def extract_data(data_path = f"{HOME_DIR}/source/jobs.csv", extract_folder = f"{HOME_DIR}/staging/extracted"):
    import pandas as pd
    """
        this function extract the context column data from data_path
        and save each record in a text file in the extract_folder
    """
    print("extracting starts ...")
    df = pd.read_csv(data_path)
    for idx, item in enumerate(df.context):
        #checking the content of the file is a valid json string
        if item and is_valid_json(item):
            with open(f"{extract_folder}/item_{idx + 1}.txt", "w") as context_item:
                context_item.write(json.dumps(json.loads(item),indent=4))
    print("extracting ends !")

def transform_data(src = f"{HOME_DIR}/staging/extracted", transform_folder = f"{HOME_DIR}/staging/transformed"):
    """
        this function transforms the data in the src
        folder and saves it to transform_folder
    """
    print("transform strats ...")
    #loop through the src folder which contains the extracted context item from the jobs.csv dataset
    for item_file in os.listdir(src):
        if item_file.endswith(".txt"):
            #constructing the fullpath to the desired item file
            file_path = f"{src}/{item_file}"
            #using the context manager to close the file after ending automatically
            with open(file_path, "r") as extracted_item:
                #reading the content of the item file
                item = extracted_item.read()
                if item and is_valid_json(item):
                    #initiating the item schema when the string is valid
                    item_schema = {
                                    "job": {
                                        "title": None,
                                        "industry": None,
                                        "description": None,
                                        "employment_type": None,
                                        "date_posted": None,
                                            },
                                    "company": {
                                        "name": None,
                                        "link": None,
                                            },
                                    "education": {
                                        "required_credential": None,
                                            },
                                    "experience": {
                                        "months_of_experience": None,
                                        "seniority_level": None,
                                            },
                                    "salary": {
                                        "currency": None,
                                        "min_value": None,
                                        "max_value": None,
                                        "unit": None,
                                            },
                                    "location": {
                                        "country": None,
                                        "locality": None,
                                        "region": None,
                                        "postal_code": None,
                                        "street_address": None,
                                        "latitude": None,
                                        "longitude": None,
                                            },
                                    }
                    #converting the string to a json disctionary object
                    item_json = json.loads(item)
                    title = item_json.get("title")
                    #filling the job key dictionnary
                    item_schema["job"]["title"] = title
                    item_schema["job"]["industry"] = item_json.get("industry")
                    item_schema["job"]["description"] = item_json.get("description")
                    item_schema["job"]["employment_type"] = item_json.get("employmentType")
                    item_schema["job"]["date_posted"] = item_json.get("datePosted")
                    #filling the company key dictionnary
                    company = item_json.get("hiringOrganization")
                    #check that the company exists first
                    if company:
                        item_schema["company"]["name"] = company.get("name")
                        item_schema["company"]["link"] = company.get("sameAs")
                    #filling the company key dictionnary
                    education = item_json.get("educationRequirements")
                    #check that the education exists first
                    if education:
                        item_schema["education"]["required_credential"] = education.get("credentialCategory")
                    #filling the experience key dictionnary
                    experience = item_json.get("experienceRequirements")
                    #check that the experience exists first
                    if experience and isinstance(experience, dict):
                        item_schema["experience"]["months_of_experience"] = experience.get("monthsOfExperience")
                    if title:
                        item_schema["experience"]["seniority_level"] = title.strip().split(" ")[0]
                    #filling the experience key dictionnary
                    salary=item_json.get("estimatedSalary")
                    #check that the experience exists first
                    if salary:
                        item_schema["salary"]["currency"] = salary.get("currency")
                        value = salary.get("value")
                        if value:
                            item_schema["salary"]["min_value"] = value.get("minValue")
                            item_schema["salary"]["max_value"] = value.get("maxValue")
                            item_schema["salary"]["unit"] = value.get("unitText")
                    #filling the experience key dictionnary
                    location=item_json.get("jobLocation")
                    #check that the location exists first
                    if location:
                        address = location.get("address")
                        #check for the address
                        if address:
                            item_schema["location"]["country"] = address.get("addressCountry")
                            item_schema["location"]["locality"] = address.get("addressLocality")
                            item_schema["location"]["region"] = address.get("addressRegion")
                            item_schema["location"]["postal_code"] = address.get("postalCode")
                            item_schema["location"]["street_address"] = address.get("streetAddress")
                        item_schema["location"]["longitude"] = location.get("longitude")
                        item_schema["location"]["latitude"] = location.get("latitude")
                        with open(f"{transform_folder}/{item_file}", "w") as transformed_item:
                            transformed_item.write(json.dumps(item_schema,indent=4))
    print("transforming ends !")

def load_data(sql_connection, src = f"{HOME_DIR}/staging/transformed"):
    """
        This function will create a set of
        tables in the sqlite database then loading the cleaned data into it
    """
    print("loading starts ...")
    #getting a cursor from the sqlite2 connection
    cursor = sql_connection.cursor()
    #create the tables
    #for query in tables_creation_queries:
        #cursor.execute(query)
    #loop through each file after transformation
    for file_name in os.listdir(src):
        if file_name.endswith(".txt"):
            #the full file path
            file_path = f"{src}/{file_name}"
            #insert query for job table
            insert_job_query = """
                                INSERT INTO job (title, industry, description, employment_type, date_posted)
                                VALUES (?, ?, ?, ?, ?);
                                """
            insert_company_query = """
                                    INSERT INTO company (job_id, name, link)
                                    VALUES (?, ?, ?);
                                    """
            insert_education_query = """
                                    INSERT INTO education (job_id, required_credential)
                                    VALUES (?, ?);
                                    """
            insert_experience_query = """
                                    INSERT INTO experience (job_id, months_of_experience, seniority_level)
                                    VALUES (?, ?, ?);
                                    """
            insert_salary_query = """
                                    INSERT INTO salary (job_id, currency, min_value, max_value, unit)
                                    VALUES (?, ?, ?, ?, ?);
                                    """
            insert_location_query = """
                                    INSERT INTO location (job_id, country, locality, region, postal_code, street_address, latitude, longitude)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                                    """
            with open(file_path, "r") as clean_data:
                json_item = json.loads(clean_data.read())
                #getting job values form the json_item
                job_values = (json_item["job"]["title"], json_item["job"]["industry"], json_item["job"]["description"], json_item["job"]["employment_type"], json_item["job"]["date_posted"])
                #execute the query
                cursor.execute(insert_job_query, job_values)
                last_inserted_job_id = cursor.lastrowid
                #getting company values form the json_item
                campany_values = (last_inserted_job_id, json_item["company"]["name"], json_item["company"]["link"])
                #execute the query
                cursor.execute(insert_company_query, campany_values)
                #getting education values form the json_item
                education_values = (last_inserted_job_id , json_item["education"]["required_credential"] )
                #execute the query
                cursor.execute(insert_education_query, education_values)
                #getting experience values form the json_item
                experience_values = (last_inserted_job_id , json_item["experience"]["months_of_experience"], json_item["experience"]["seniority_level"])
                #execute the query
                cursor.execute(insert_experience_query, experience_values)
                #getting salary values form the json_item
                salary_values = (last_inserted_job_id , json_item["salary"]["currency"], json_item["salary"]["min_value"], json_item["salary"]["max_value"], json_item["salary"]["unit"])
                #execute the query
                cursor.execute(insert_salary_query, salary_values)
                #getting location values form the json_item
                location_values = (last_inserted_job_id , json_item["location"]["country"], json_item["location"]["locality"], json_item["location"]["region"], json_item["location"]["postal_code"], json_item["location"]["street_address"], json_item["location"]["latitude"], json_item["location"]["longitude"])
                #execute the query
                cursor.execute(insert_location_query, location_values)
    cursor.close()
    print("loading ends !")
