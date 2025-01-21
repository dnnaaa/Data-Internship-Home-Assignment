import os
import json
import re
from html import unescape
from airflow.decorators import task
def clean_text(text):
    if not text:
        return ""
    text = unescape(text)
    text = re.sub(r"<[^>]*>", "", text) 
    return text.strip()

def parse_job_data(raw_data):

    experience_req = raw_data.get("experienceRequirements", {})
    months_of_experience = ""
    seniority_level = ""

    if isinstance(experience_req, dict):
        months_of_experience = experience_req.get("monthsOfExperience", "")
        seniority_level = experience_req.get("@type", "")
    elif isinstance(experience_req, str):
        seniority_level = experience_req

    salary_data = raw_data.get("estimatedSalary", {}).get("value", {})
    address_data = raw_data.get("jobLocation", {}).get("address", {})

    return {
        "job": {
            "title": raw_data.get("title", ""),
            "industry": raw_data.get("industry", ""),
            "description": clean_text(raw_data.get("description", "")),
            "employment_type": raw_data.get("employmentType", ""),
            "date_posted": raw_data.get("datePosted", ""),
        },
        "company": {
            "name": raw_data.get("hiringOrganization", {}).get("name", ""),
            "link": raw_data.get("hiringOrganization", {}).get("sameAs", ""),
        },
        "education": {
            "required_credential": raw_data.get("educationRequirements", {}).get("credentialCategory", ""),
        },
        "experience": {
            "months_of_experience": months_of_experience,
            "seniority_level": seniority_level,
        },
        "salary": {
            "currency": raw_data.get("estimatedSalary", {}).get("currency", ""),
            "min_value": salary_data.get("minValue", ""),
            "max_value": salary_data.get("maxValue", ""),
            "unit": salary_data.get("unitText", ""),
        },
        "location": {
            "country": address_data.get("addressCountry", ""),
            "locality": address_data.get("addressLocality", ""),
            "region": address_data.get("addressRegion", ""),
            "postal_code": address_data.get("postalCode", ""),
            "street_address": address_data.get("streetAddress", ""),
            "latitude": raw_data.get("jobLocation", {}).get("latitude", ""),
            "longitude": raw_data.get("jobLocation", {}).get("longitude", ""),
        },
    }

@task()
def transform(extracted_dir, transformed_dir):

    if not os.path.isdir(extracted_dir):
        raise FileNotFoundError(f"The directory '{extracted_dir}' does not exist.")

    os.makedirs(transformed_dir, exist_ok=True)

    for file_name in os.listdir(extracted_dir):
        input_file_path = os.path.join(extracted_dir, file_name)
        if not file_name.endswith(".txt"):
            continue

        with open(input_file_path, 'r', encoding='utf-8') as input_file:
            try:
                raw_data = json.load(input_file)
            except json.JSONDecodeError as e:
                print(f"Skipping file '{file_name}' due to JSON decoding error: {e}")
                continue

        transformed_data = parse_job_data(raw_data)

        output_file_name = file_name.replace(".txt", ".json")
        output_file_path = os.path.join(transformed_dir, output_file_name)

        with open(output_file_path, 'w', encoding='utf-8') as output_file:
            json.dump(transformed_data, output_file, indent=4)

    print(f"Transformation complete. Transformed files saved in '{transformed_dir}'.")
