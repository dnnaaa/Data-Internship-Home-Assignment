import os
import json
import re
import html
from airflow.decorators import task


def clean_description(description):
    cleaned_description = html.unescape(description)
    # Remove HTML tags and special characters
    cleaned_description = re.sub(r'<.*?>', '', cleaned_description)
    return cleaned_description


def transformLine(line):
    try:
        raw_data = json.loads(line)
    except json.JSONDecodeError as e:
        print(f"JSON decoding error: {e}")
        return None

    experience_req = raw_data.get("experienceRequirements", {})
    if isinstance(experience_req, dict):
        months_of_experience = experience_req.get("monthsOfExperience", "")
        seniority_level = experience_req.get("@type", "")
    else:
        months_of_experience = ""
        seniority_level = str(experience_req)

    transformed_data = {"job": {
        "title": raw_data.get("title", ""),
        "industry": raw_data.get("industry", ""),
        "description": clean_description(raw_data.get("description", "")),
        "employment_type": raw_data.get("employmentType", ""),
        "date_posted": raw_data.get("datePosted", ""),
    }, "company": {
        "name": raw_data.get("hiringOrganization", {}).get("name", ""),
        "link": raw_data.get("hiringOrganization", {}).get("sameAs", ""),
    }, "education": {
        "required_credential": raw_data.get("educationRequirements", {}).get("@type", ""),
    }, "experience": {
        "months_of_experience": months_of_experience,
        "seniority_level": seniority_level,
    }, "salary": {
        "currency": raw_data.get("estimatedSalary", {}).get("currency", ""),
        "min_value": raw_data.get("estimatedSalary", {}).get("value", {}).get("minValue", ""),
        "max_value": raw_data.get("estimatedSalary", {}).get("value", {}).get("maxValue", ""),
        "unit": raw_data.get("estimatedSalary", {}).get("value", {}).get("unitText", ""),
    }, "location": {
        "country": raw_data.get("jobLocation", {}).get("address", {}).get("addressCountry", ""),
        "locality": raw_data.get("jobLocation", {}).get("address", {}).get("addressLocality", ""),
        "region": raw_data.get("jobLocation", {}).get("address", {}).get("addressRegion", ""),
        "postal_code": raw_data.get("jobLocation", {}).get("address", {}).get("postalCode", ""),
        "street_address": raw_data.get("jobLocation", {}).get("address", {}).get("streetAddress", ""),
        "latitude": raw_data.get("jobLocation", {}).get("latitude", ""),
        "longitude": raw_data.get("jobLocation", {}).get("longitude", ""),
    }}

    return transformed_data


@task()
def transform(source_dir, target_dir):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    source_dir_path = os.path.join(script_dir, source_dir)
    target_dir_path = os.path.join(script_dir, target_dir)

    if not os.path.exists(target_dir_path):
        os.makedirs(target_dir_path)

    if not os.path.exists(source_dir_path):
        raise Exception(f"Source directory {source_dir_path} does not exist.")

    for file_name in os.listdir(source_dir_path):
        if file_name.endswith('.txt'):
            file_path = os.path.join(source_dir_path, file_name)
            output_file_path = os.path.join(target_dir_path, f"transformed")
            try:
                with open(file_path, 'r', encoding='utf-8') as file, \
                        open(output_file_path, 'w', encoding='utf-8') as output_file:
                    for line in file:
                        transformed = transformLine(line)
                        if transformed:
                            json.dump(transformed, output_file, indent=4)
                            output_file.write('\n')
            except UnicodeDecodeError as e:
                print(f"Decoding error in file {file_name}: {e}. It might not be UTF-8 encoded or contain binary data.")

