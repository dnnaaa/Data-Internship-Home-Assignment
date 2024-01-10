import json
import os
import re
import html


def clean_description(description):
    # Decode HTML entities
    decoded_description = html.unescape(description)
    # Remove HTML tags
    clean_description = re.sub(r'<[^>]+>', '', decoded_description)
    return clean_description


def extract_salary_values(description):
    salary_string = re.findall(r'\$\d+k - \$\d+k', description)
    if salary_string:
        salary_range = salary_string[0].replace(
            '$', '').replace('k', '').split(' - ')
        if len(salary_range) == 2:
            min_salary, max_salary = salary_range
            return float(min_salary) * 1000, float(max_salary) * 1000
    return 0.0, 0.0


def determine_seniority(months_of_experience):
    if months_of_experience is None or months_of_experience == '':
        return ''
    try:
        months = int(months_of_experience)
    except ValueError:
        return ''  # or some default value

    if months < 12:
        return 'Entry Level'
    elif 12 <= months < 36:
        return 'Mid Level'
    elif 36 <= months < 60:
        return 'Senior Level'
    else:
        return 'Expert Level'


def transform_data(input_directory, output_directory):
    for filename in os.listdir(input_directory):
        if filename.endswith('.txt'):
            with open(os.path.join(input_directory, filename), 'r', encoding='utf-8') as file:
                data = json.load(file)

                min_salary, max_salary = extract_salary_values(
                    data.get("description", ""))

                seniority_level = determine_seniority(
                    data.get("experienceRequirements", {}).get("monthsOfExperience", ""))

                # Transform the schema
                transformed_data = {
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
                        "months_of_experience": data.get("experienceRequirements", {}).get("monthsOfExperience", ""),
                        "seniority_level": seniority_level,
                    },
                    "salary": {
                        "currency": "USD",
                        "min_value": min_salary,
                        "max_value": max_salary,
                        "unit": "per year",
                    },
                    "location": {
                        "country": data.get("jobLocation", {}).get("address", {}).get("addressCountry", ""),
                        "locality": data.get("jobLocation", {}).get("address", {}).get("addressLocality", ""),
                        "region": data.get("jobLocation", {}).get("address", {}).get("addressRegion", ""),
                        "postal_code": data.get("jobLocation", {}).get("address", {}).get("postalCode", ""),
                        "street_address": data.get("jobLocation", {}).get("address", {}).get("streetAddress", ""),
                        "latitude": str(data.get("jobLocation", {}).get("latitude", "")),
                        "longitude": str(data.get("jobLocation", {}).get("longitude", "")),
                    },
                }

                # Write the transformed data to a file
                output_file_path = os.path.join(
                    output_directory, f'transformed_{filename}')
                with open(output_file_path, 'w', encoding='utf-8') as output_file:
                    json.dump(transformed_data, output_file, indent=4)

    return "Transformation complete"
