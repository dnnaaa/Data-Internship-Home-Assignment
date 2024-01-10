import json
import os
import html

def transform_extracted_files(input_dir, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    for file_name in os.listdir(input_dir):
        file_path = f"{input_dir}/{file_name}"
        with open(file_path, 'r') as file:
            file_content = file.read()

            if not file_content.strip():
                print(f"Skipping empty file: {file_name}")
                continue

            try:
                data = json.loads(file_content)
                # Clean the HTML entities in the description
                if 'description' in data:
                    data['description'] = html.unescape(data['description'])
                transformed_data = transform_schema(data)
                output_file = f"{output_dir}/{file_name.replace('.txt', '.json')}"
                with open(output_file, 'w') as outfile:
                    json.dump(transformed_data, outfile, indent=4)
            except json.JSONDecodeError:
                print(f"Error decoding JSON from file: {file_name}")


def transform_schema(data):
    # Extracting fields from the data
    job_title = data.get('title', '')
    job_industry = data.get('industry', '')
    job_description = data.get('description', '')
    job_employment_type = data.get('employmentType', '')
    job_date_posted = data.get('datePosted', '')

    company_name = data.get('hiringOrganization', {}).get('name', '')
    company_linkedin_link = data.get('hiringOrganization', {}).get('sameAs', '')
    education_requirements = data.get('educationRequirements', {})
    job_required_credential = ''
    if isinstance(education_requirements, dict):
        job_required_credential = education_requirements.get('credentialCategory', '')

    experience_requirements = data.get('experienceRequirements', {})
    job_months_of_experience = 0
    if isinstance(experience_requirements, dict):
        job_months_of_experience = experience_requirements.get('monthsOfExperience', 0)
    
    seniority_level = ''  # This field needs to be defined as it's not clear from the given data

    salary_currency = ''  # These fields are not present in the given data structure
    salary_min_value = 0
    salary_max_value = 0
    salary_unit = ''

    country = data.get('jobLocation', {}).get('address', {}).get('addressCountry', '')
    locality = data.get('jobLocation', {}).get('address', {}).get('addressLocality', '')
    region = data.get('jobLocation', {}).get('address', {}).get('addressRegion', '')
    postal_code = data.get('jobLocation', {}).get('address', {}).get('postalCode', '')
    street_address = data.get('jobLocation', {}).get('address', {}).get('streetAddress', '')
    latitude = data.get('jobLocation', {}).get('latitude', 0)
    longitude = data.get('jobLocation', {}).get('longitude', 0)

    # Creating the transformed data according to the desired schema
    transformed_data = {
        "job": {
            "title": job_title,
            "industry": job_industry,
            "description": job_description,
            "employment_type": job_employment_type,
            "date_posted": job_date_posted,
        },
        "company": {
            "name": company_name,
            "link": company_linkedin_link,
        },
        "education": {
            "required_credential": job_required_credential,
        },
        "experience": {
            "months_of_experience": job_months_of_experience,
            "seniority_level": seniority_level,
        },
        "salary": {
            "currency": salary_currency,
            "min_value": salary_min_value,
            "max_value": salary_max_value,
            "unit": salary_unit,
        },
        "location": {
            "country": country,
            "locality": locality,
            "region": region,
            "postal_code": postal_code,
            "street_address": street_address,
            "latitude": latitude,
            "longitude": longitude,
        },
    }

    return transformed_data

