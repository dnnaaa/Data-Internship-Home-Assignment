import json
from pathlib import Path
from airflow.decorators import task

@task()
def transform_to_json():
    extracted_folder = Path("staging/extracted")
    transformed_folder = Path("staging/transformed")
    transformed_folder.mkdir(parents=True, exist_ok=True)

    for file_path in extracted_folder.glob("*.txt"):
        with open(file_path, "r") as file:
            context_data = json.loads(file.read())

        # Clean the job description and transform the schema
        transformed_data = transform_schema(context_data)

        # Save each item to staging/transformed as json file
        transformed_file_path = transformed_folder / f"{file_path.stem}_transformed.json"
        with open(transformed_file_path, "w") as transformed_file:
            json.dump(transformed_data, transformed_file, indent=2)

def transform_schema(context_data):
    transformed_data = {
        "job": {
            "title": context_data.get("title", ""),
            "industry": context_data.get("industry", ""),
            "description": clean_job_description(context_data.get("description", "")),
            "employment_type": context_data.get("employmentType", ""),
            "date_posted": context_data.get("datePosted", ""),
        },
        "company": {
            "name": context_data["hiringOrganization"]["name"],
            "link": context_data["hiringOrganization"]["sameAs"],
        },
        "education": {
            "required_credential": context_data["educationRequirements"]["credentialCategory"],
        },
        "experience": {
            "months_of_experience": context_data["experienceRequirements"]["monthsOfExperience"],
            "seniority_level": context_data["experienceRequirements"]["seniority_level"],
        },
        "salary": {
            "currency": context_data["currency"],
            "min_value": context_data["min_value"],
            "max_value": context_data["max_value"],
            "unit": context_data["unit"],
        },
        "location": {
            "country": context_data["jobLocation"]["address"]["addressCountry"],
            "locality": context_data["jobLocation"]["address"]["addressLocality"],
            "region": context_data["jobLocation"]["address"]["addressRegion"],
            "postal_code": context_data["jobLocation"]["address"]["postalCode"],
            "street_address": context_data["jobLocation"]["address"]["streetAddress"],
            "latitude": context_data["jobLocation"]["latitude"],
            "longitude": context_data["jobLocation"]["longitude"],
        },
    }
    return transformed_data

def clean_job_description(description):
    # Remove HTML tags using regular expression
    cleaned_description = re.sub('<.*?>', '', description)
    
    # Replace special characters or unwanted patterns
    cleaned_description = cleaned_description.replace('&lt;br&gt;', '\n')  # Replace line breaks
    
    
    return cleaned_description