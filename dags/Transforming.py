import json
import os
import glob
import html
from airflow.decorators import task

@task()
def transform():
    """Transform extracted data and save in the desired schema."""
    extracted_dir = 'staging/extracted'
    transformed_dir = 'staging/transformed'
    os.makedirs(transformed_dir, exist_ok=True)

    for file_path in glob.glob(f'{extracted_dir}/*.txt'):
        try:
            with open(file_path, 'r') as file:
                file_content = file.read().strip()
                if file_content and file_content.lower() != 'nan':
                    data = json.loads(file_content)

                    try:
                        # Transform the data
                        transformed_data = transform_data(data)

                        # Save the transformed data
                        transformed_file_path = os.path.join(transformed_dir, os.path.basename(file_path))
                        with open(transformed_file_path, 'w') as transformed_file:
                            json.dump(transformed_data, transformed_file, indent=4)
                    except Exception as transform_error:
                        print(f"Error transforming data in file {file_path}: {transform_error}")
                        continue
                else:
                    print(f"Skipped empty or invalid content in file {file_path}")

        except json.JSONDecodeError as e:
            print(f"Error while parsing {file_path}: {e}")
def transform_data(original_data):
    """Transform the data into the desired schema."""
    # Clean and map the data to the new schema
    transformed = {
        "job": {
            "title": original_data.get("title", ""),
            "industry": original_data.get("industry", ""),
            "description": clean_description(original_data.get("description", "")),
            "employment_type": original_data.get("employmentType", ""),
            "date_posted": original_data.get("datePosted", "")
        },
        "company": {
            "name": original_data.get("hiringOrganization", {}).get("name", ""),
            "link": original_data.get("hiringOrganization", {}).get("sameAs", "")
        },
        "education": {
            "required_credential": original_data.get("educationRequirements", "")
        },
        "experience": {
            "months_of_experience": original_data.get("experienceRequirements", {}).get("monthsOfExperience", 0),
            "seniority_level": original_data.get("experienceRequirements", {}).get("seniorityLevel", "")
        },
        "salary": {
            "currency": original_data.get("salary", {}).get("currency", ""),
            "min_value": original_data.get("salary", {}).get("value", {}).get("minValue", 0),
            "max_value": original_data.get("salary", {}).get("value", {}).get("maxValue", 0),
            "unit": original_data.get("salary", {}).get("unitText", "")
        },
        "location": {
            "country": original_data.get("jobLocation", {}).get("address", {}).get("addressCountry", ""),
            "locality": original_data.get("jobLocation", {}).get("address", {}).get("addressLocality", ""),
            "region": original_data.get("jobLocation", {}).get("address", {}).get("addressRegion", ""),
            "postal_code": original_data.get("jobLocation", {}).get("address", {}).get("postalCode", ""),
            "street_address": original_data.get("jobLocation", {}).get("address", {}).get("streetAddress", ""),
            "latitude": original_data.get("jobLocation", {}).get("latitude", 0),
            "longitude": original_data.get("jobLocation", {}).get("longitude", 0)
        }
    }
    return transformed

def clean_description(description):
    """Clean the job description by removing HTML tags and entities."""
    return html.unescape(description)  # Converts HTML entities to plain text

