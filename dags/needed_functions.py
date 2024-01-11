import pandas as pd
import os 
import json
from html.parser import HTMLParser
from bs4 import BeautifulSoup
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from datetime import timedelta



# this function to parse job description as an HTML

def clean_html(encoded_text):
    class MyHTMLParser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.result = []

        def handle_data(self, data):
            self.result.append(data)

    parser = MyHTMLParser()
    parser.feed(encoded_text)
    return ''.join(parser.result)


# this function to clean job description

def clean(text):
    html = clean_html(text)
    soup = BeautifulSoup(html,'html.parser')
    text = soup.get_text(separator='\n', strip=True)
    return text


# this function for creating seniority level based on months of experience

def seniority_level(months_of_experience):
    seniority_levels = {
        (0,23): 'Junior',
        (24,59): 'Mid-Level',
        (60,119): 'Senior',
        (120,180): 'Lead',
    }
    for level_range, seniority_level in seniority_levels.items():
        if level_range[0] <= months_of_experience <= level_range[1]:
            return seniority_level
        if months_of_experience > 180:
            return 'Lead'
    
    return 'Not specified'





### this function transforms the data to our desired schema 


def transform_data(json_data):
    target = {}
    try:
        target["location"]={
        "country" : json_data['jobLocation']['address']['addressCountry'],
        "locality": json_data['jobLocation']['address']['addressLocality'],
        "region" : json_data['jobLocation']['address']['addressLocality'],
        "postal_code": json_data['jobLocation']['address']['postalCode'],
        "street_address": json_data['jobLocation']['address']['streetAddress'],
        "latitude": json_data['jobLocation']['latitude'],
        "longitude": json_data['jobLocation']['longitude'],
        }
    except: 
        target["location"] = {
        "country" : None,
        "locality": None,
        "region" : None,
        "postal_code": None,
        "street_address": None,
        "latitude": None,
        "longitude": None,
        }

    try :
        target["salary"] = {
            "currency": json_data['estimatedSalary']['currency'],
            "min_value": json_data['estimatedSalary']['value']['minValue'],
            "max_value": json_data['estimatedSalary']['value']['maxValue'] ,
            "unit": json_data['estimatedSalary']['value']['unitText'] ,
        }


    except :
        target['salary'] = {
            "currency": None,
            "min_value": None,
            "max_value": None,
            "unit":None
        }
        
    try:
        target['job'] = {
            "title":json_data['title'],
            "industry":json_data['industry'],
            "description": clean(json_data['description']),
            "employment_type": json_data['employmentType'],
            "date_posted": json_data['datePosted']
        }
    except:
        target['job'] = {
            "title":None,
            "industry":None,
            "description": None,
            "employment_type": None,
            "date_posted": None
        }
    
    try:
        target['company'] = {
            "name":json_data['hiringOrganization']['name'],
            "link":json_data['hiringOrganization']['sameAs']
        }
        
    except:
        target['company'] =  {
            "name":None,
            "link":None
        }
    try:
        target['education'] = {
            "required_credential":json_data['educationRequirements']['credentialCategory']
        }
    except:
        target['education']= {
            "required_credential":None
        }
    
    try: 
        target['experience'] = {
        "months_of_experience":json_data['experienceRequirements']['monthsOfExperience'],
        "seniority_level":seniority_level(json_data['experienceRequirements']['monthsOfExperience'])
    }
    except:
        target['experience'] = {
        "months_of_experience":None,
        "seniority_level":None
    }
    
    
    return target



## this is the function that we will call under the transfrom task

def transform_task_function():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    path_to_staging_extracted = dir_path+'/../staging/extracted'
    files_list = os.listdir(path_to_staging_extracted)

    transformed_file_number = 1

    for file in files_list : 

        path_to_extracted_file = path_to_staging_extracted+f'/{file}'
        path_to_transformed_file = dir_path +f'/../staging/transformed/{transformed_file_number}.json'
        
        with open(path_to_extracted_file) as f:
            json_data = json.load(f)
            target = transform_data(json_data)
        
        with open(path_to_transformed_file,'w') as f:
            json.dump(target,f)
        transformed_file_number +=1
    print(transformed_file_number)



# just a simple function to transfrom a dictionnary to tuple

def dict_to_tuple(data,id):
    return (None,)+(id,)+tuple(data.values())



# this function extract the rows to be inserted on the sql tables from the json file

def rows_to_insert(data,id):
    company_row = dict_to_tuple(data['company'],id)
    education_row = dict_to_tuple(data['education'],id)
    experience_row = dict_to_tuple(data['experience'],id)
    salary_row = dict_to_tuple(data['salary'],id)
    location_row =dict_to_tuple(data['location'],id)
    return company_row,education_row,experience_row,salary_row,location_row
    
    
# this function will be called under load() task to load() data to the db
    
def load_data_to_db():
    current_directory = os.path.dirname(os.path.realpath(__file__))
    path_to_staging_transformed = current_directory+'/../staging/transformed'
    transformed_files_list = os.listdir(path_to_staging_transformed)
    id=1
    for file in transformed_files_list : 
        
        path_to_transformed_file = path_to_staging_transformed+f'/{file}'
        
        try:
            with open(path_to_transformed_file) as f:
                data = json.load(f)
        except:
            print("Couldn't open file ")
        
        
        job_row=(None,)+tuple(data['job'].values())
        company_row,education_row,experience_row,salary_row,location_row=rows_to_insert(data,id)
        
        sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
        sqlite_hook.insert_rows(table='job', rows=[job_row])
        sqlite_hook.insert_rows(table='education', rows=[education_row])
        sqlite_hook.insert_rows(table='company', rows=[company_row])
        sqlite_hook.insert_rows(table='experience', rows=[experience_row])
        sqlite_hook.insert_rows(table='salary', rows=[salary_row])
        sqlite_hook.insert_rows(table='location', rows=[location_row])    
        id+=1
        
    

# this function is for the extraction of jobs data and save the to staging/extracted 
# directory

def extract_data():
    """Extract data from jobs.csv."""
    
    # Get the directory of the current script (etl.py)
    current_directory = os.path.dirname(os.path.realpath(__file__))
    
    # we specify the path to jobs.csv and staging/extracted  
    path_to_jobs_csv = os.path.join(current_directory, "../source/jobs.csv")
    path_to_staging_extracted = os.path.join(current_directory,"../staging/extracted")
    
    # read jobs.csv and store it inside pandas dataframe
    data = pd.read_csv(path_to_jobs_csv)
    data.drop_duplicates()
    df_context = data['context']
    file_number=1
    
    # looping in df_context list and storing each row inside a text file
    for item in df_context :
        if type(item)==type("string"):
            file_name = f"{path_to_staging_extracted}/file{file_number}.txt"
            with open(file_name,"+w") as file:
                file.write(item)
        file_number+=1
        



# the sql query to be executed 
TABLES_CREATION_QUERY=["""CREATE TABLE IF NOT EXISTS job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(225),
    industry VARCHAR(225),
    description TEXT,
    employment_type VARCHAR(125),
    date_posted DATE
);""" , 

"""CREATE TABLE IF NOT EXISTS company (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    name VARCHAR(225),
    link TEXT,
    FOREIGN KEY (job_id) REFERENCES job(id)
);""" ,

"""CREATE TABLE IF NOT EXISTS education (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    required_credential VARCHAR(225),
    FOREIGN KEY (job_id) REFERENCES job(id)
);""",

"""CREATE TABLE IF NOT EXISTS experience (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    months_of_experience INTEGER,
    seniority_level VARCHAR(25),
    FOREIGN KEY (job_id) REFERENCES job(id)
);""",

"""CREATE TABLE IF NOT EXISTS salary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    currency VARCHAR(3),
    min_value NUMERIC,
    max_value NUMERIC,
    unit VARCHAR(12),
    FOREIGN KEY (job_id) REFERENCES job(id)
);""",

"""CREATE TABLE IF NOT EXISTS location (
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
"""]



DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}
