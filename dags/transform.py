import json
import re
import html
from bs4 import BeautifulSoup

def cleaned_description(text) : 
    # Decode HTML entities
    decoded_text = html.unescape(text)
    # Use BeautifulSoup to parse HTML
    soup = BeautifulSoup(decoded_text, "html.parser")
    # Extract text content
    cleaned_text = soup.get_text(separator=' ', strip=True)
    return cleaned_text

def seniority_level(months_of_experience) :
    if months_of_experience < 12: return "junior Level"
    elif 12 <= months_of_experience < 36: return "Intermediate Level"
    elif 36 <= months_of_experience < 60: return "Experienced Level"
    else: return "Senior Level"

def transform(extracted_data) :
    transformed_data = {"job":{}, 'company':{}, 'education':{}, 'experience':{}, "salary":{}, "location":{}}
    # job infos : 
    transformed_data['job']['title'] = extracted_data['title']
    transformed_data['job']['industry'] = extracted_data['industry']
    transformed_data['job']['description'] = cleaned_description(extracted_data['description'])
    transformed_data['job']['employment_type'] = extracted_data['employmentType']
    transformed_data['job']['date_posted'] = extracted_data['datePosted']
    # company infos : 
    transformed_data['company']['name'] = extracted_data['hiringOrganization']["name"]
    transformed_data['company']['link'] = extracted_data['hiringOrganization']["sameAs"]
    # education infos : 
    try : transformed_data['education']['required_credential'] = extracted_data['educationRequirements']['credentialCategory']
    except : transformed_data['education']['required_credential'] = None
    # experience infos : 
    try : 
        transformed_data['experience']['months_of_experience'] = int(extracted_data['experienceRequirements']['monthsOfExperience'])
        transformed_data['experience']['seniority_level'] = seniority_level(extracted_data['experienceRequirements']['monthsOfExperience'])
    except : 
        transformed_data['experience']['months_of_experience'] = None
        transformed_data['experience']['seniority_level'] = None
    # salary infos : 
    try : transformed_data['salary']['currency'] = extracted_data['estimatedSalary']['currency']
    except : 
        try : transformed_data['salary']['currency'] = extracted_data['baseSalary']['currency']
        except : transformed_data['salary']['currency'] = None

    try : transformed_data['salary']['min_value'] = int(extracted_data['estimatedSalary']['minValue'])
    except : 
        try : transformed_data['salary']['min_value'] = int(extracted_data['baseSalary']['minValue'])
        except : transformed_data['salary']['min_value'] = None

    try : transformed_data['salary']['max_value'] = int(extracted_data['estimatedSalary']['maxValue'])
    except : 
        try : transformed_data['salary']['max_value'] = int(extracted_data['baseSalary']['maxValue'])
        except : transformed_data['salary']['max_value'] = None

    try : transformed_data['salary']['unit'] = extracted_data['estimatedSalary']['unitText']
    except : 
        try : transformed_data['salary']['unit'] = extracted_data['baseSalary']['unitText']
        except : transformed_data['salary']['unit'] = None

    # location infos :
    transformed_data['location']['country'] = extracted_data['jobLocation']['address']["addressCountry"]
    transformed_data['location']['locality'] = extracted_data['jobLocation']['address']["addressLocality"]
    transformed_data['location']['region'] = extracted_data['jobLocation']['address']["addressRegion"]
    try : transformed_data['location']['postal_code'] = extracted_data['jobLocation']['address']["postalCode"]
    except :  transformed_data['location']['postal_code'] = None
    transformed_data['location']['street_address'] = extracted_data['jobLocation']['address']["streetAddress"]
    transformed_data['location']['latitude'] = float(extracted_data['jobLocation']["latitude"])
    transformed_data['location']['longitude'] = float(extracted_data['jobLocation']["longitude"])

    return transformed_data

def transform_task() :
    for i in range(7684) : 
        with open(f"staging/extracted/file_{i}.txt", "r") as f : 
            extracted_data = json.load(f)
        
        transformed_data = transform(extracted_data)
        
        with open(f"staging/transformed/transformed_file_{i}.json", "w") as f2 : 
            json.dump(transformed_data, f2, indent=4)