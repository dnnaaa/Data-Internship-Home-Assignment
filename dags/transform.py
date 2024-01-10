from airflow.decorators import task

from bs4 import BeautifulSoup
from html import unescape

import json
import os


extracted_data_folder_path = 'staging/extracted'
transformed_data_folder_path = 'staging/transformed'

def clean_text(text):
    if text is not None:
        # Decode HTML entities (for example, &lt; becomes <)
        decoded_text = unescape(text)

        # Remove HTML tags
        soup = BeautifulSoup(decoded_text, 'html.parser')
        cleaned_text = soup.get_text(separator=' ')

        return cleaned_text
    else:
        return None

def transform_json_data(json_data):
    # Create a new dictionary with the selected and renamed keys
    transformed_json_data = {
        "job": {
            "title": json_data.get("title", None),
            "industry": json_data.get("industry", None),
            "description": json_data.get("description", None),
            "employment_type": json_data.get("employmentType", None),
            "date_posted": json_data.get("datePosted", None)
        },
        "company": {
            "name": json_data.get("hiringOrganization", {}).get("name", None),
            "link": json_data.get("hiringOrganization", {}).get("sameAs", None)
        },
        "education": {
            "required_credential": json_data.get("educationRequirements", {}).get("credentialCategory", None)
        },
        "experience": {
            "months_of_experience": None,
            "seniority_level": None
        },
        "salary": {
            "currency": json_data.get("estimatedSalary", {}).get("currency", None),
            "min_value": json_data.get("estimatedSalary", {}).get("value", {}).get("minValue", None),
            "max_value": json_data.get("estimatedSalary", {}).get("value", {}).get("maxValue", None),
            "unit": json_data.get("estimatedSalary", {}).get("value", {}).get("unitText", None)
        },
        "location": {
            "country": json_data.get("jobLocation", {}).get("address", {}).get("addressCountry", None),
            "locality": json_data.get("jobLocation", {}).get("address", {}).get("addressLocality", None),
            "region": json_data.get("jobLocation", {}).get("address", {}).get("addressRegion", None),
            "postal_code": json_data.get("jobLocation", {}).get("address", {}).get("postalCode", None),
            "street_address": json_data.get("jobLocation", {}).get("address", {}).get("streetAddress", None),
            "latitude": json_data.get("jobLocation", {}).get("latitude", None),
            "longitude": json_data.get("jobLocation", {}).get("longitude", None)
        },
    }
    
    # Extract values from experienceRequirements
    experience_requirements = json_data.get("experienceRequirements")

    if isinstance(experience_requirements, str):
        # If it's a string, use its value
        transformed_json_data["experience"]["months_of_experience"] = experience_requirements
    elif isinstance(experience_requirements, dict):
        # If it's a dictionary, extract values
        transformed_json_data["experience"]["months_of_experience"] = experience_requirements.get("monthsOfExperience", None)
        
    # Extract seniority_level from title
    title = transformed_json_data["job"]["title"]
    if title is not None:
        seniority_indicators = ["Senior", "Lead", "Principal", "Manager", "Sr."]

        for indicator in seniority_indicators:
            if indicator in title:
                transformed_json_data["experience"]["seniority_level"] = indicator
                break

    # Clean description
    description = transformed_json_data["job"]["description"]
    transformed_json_data["job"]["description"] = clean_text(description)

    return transformed_json_data


@task()
def transform():
    """Clean and convert extracted elements to json."""

    # Create the 'transformed_data_folder_path' folder if it does not exist
    os.makedirs(transformed_data_folder_path, exist_ok=True)

    for filename in os.listdir(extracted_data_folder_path):
        if filename.endswith('.txt'):
            input_file_path = os.path.join(extracted_data_folder_path, filename)
            
            # Read the content of the text file
            with open(input_file_path, 'r', encoding='utf-8') as file:
                text_content = file.read()

            # Convert the text into a JSON object
            try:
                json_data = json.loads(text_content)
            except json.JSONDecodeError as e:
                print(f"JSON decoding error : {e}")

            # Call the function 'transform_json_data'
            output_data = transform_json_data(json_data)

            # Create the output path for the JSON file
            output_file_path = os.path.join(transformed_data_folder_path, f"{filename.replace('extracted', 'transformed')[:-4]}.json")

            # Write the data to the JSON file
            with open(output_file_path, 'w', encoding='utf-8') as json_file:
                json.dump(output_data, json_file, ensure_ascii=False, indent=2)

