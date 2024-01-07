import json
import os
import re

def clean_description(description):
    return re.sub('<[^<]+?>', '', description)

def transform_data():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    extracted_dir = os.path.join(script_dir, "staging/extracted")
    transformed_dir = os.path.join(script_dir, "staging/transformed")

    # create transformed directory if it doesn't exist
    if not os.path.exists(transformed_dir):
        os.makedirs(transformed_dir)

    for filename in os.listdir(extracted_dir):
        if filename.endswith('.txt'):
            file_path = os.path.join(extracted_dir, filename)

            with open(file_path, 'r') as file:
                try:
                    data = json.load(file)
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON in file {filename}: {e}")
                    continue
            transformed_entry = {
                "job": {
                    "title": data.get("title", ""),
                    "industry": data.get("industry", ""),
                    "description": clean_description(data.get("description", "")),
                    "employment_type": data.get("employmentType", ""),
                    "date_posted": data.get("datePosted", ""),
                },
                "company": {
                    "name": data.get("hiringOrganization", {}).get("name", ""),
                    "link": data.get("hiringOrganization", {}).get("sameAs", ""),
                },
                "education": {
                    "required_credential": data.get("educationRequirements", {}).get("credentialCategory", ""),
                },
                "experience": {
                    "months_of_experience": "",
                    "seniority_level": "",  
                },
                "salary": {
                    "currency": "",  
                    "min_value": "",  
                    "max_value": "",  
                    "unit": "",  
                },
                "location": {
                    "country": data.get("jobLocation", {}).get("address", {}).get("addressCountry", ""),
                    "locality": data.get("jobLocation", {}).get("address", {}).get("addressLocality", ""),
                    "region": data.get("jobLocation", {}).get("address", {}).get("addressRegion", ""),
                    "postal_code": data.get("jobLocation", {}).get("address", {}).get("postalCode", ""),
                    "street_address": data.get("jobLocation", {}).get("address", {}).get("streetAddress", ""),
                    "latitude": data.get("jobLocation", {}).get("latitude", ""),
                    "longitude": data.get("jobLocation", {}).get("longitude", ""),
                },
            }

            output_file_path = os.path.join(transformed_dir, f"transformed_{filename[:-4]}.json")
            with open(output_file_path, 'w') as output_file:
                json.dump(transformed_entry, output_file, indent=4)

#transform_data()
