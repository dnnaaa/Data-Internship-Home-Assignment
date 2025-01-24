import os
import json
import logging
from airflow.decorators import task

@task
def transform_task():
    """Transform extracted data."""
    input_dir = "staging/extracted"
    output_dir = "staging/transformed"
    os.makedirs(output_dir, exist_ok=True)

    for file_name in os.listdir(input_dir):
        file_path = os.path.join(input_dir, file_name)

       
        if os.path.getsize(file_path) == 0:
            logging.error(f"File {file_name} is empty. Skipping transformation.")
            continue
        
        try:
            with open(file_path, 'r') as f:
                content = f.read().strip()  
                if not content: 
                    logging.error(f"File {file_name} is empty. Skipping transformation.")
                    continue

                # Log the content for debugging
                logging.info(f"Content of {file_name}: {content}")

                # Skip files with "nan" content
                if content.lower() == "nan":
                    logging.error(f"File {file_name} contains 'nan'. Skipping transformation.")
                    continue

               
                try:
                    data = json.loads(content)
                except json.JSONDecodeError as e:
                    logging.error(f"File {file_name} contains invalid JSON: {str(e)}. Skipping transformation.")
                    continue

                transformed_data = {
                    "job": {
                        "title": data.get("title"),
                        "industry": data.get("industry"),
                        "description": data.get("description"),
                        "employment_type": data.get("employmentType"),
                        "date_posted": data.get("datePosted"),
                    },
                    "company": {
                        "name": data.get("hiringOrganization", {}).get("name"),
                        "link": data.get("hiringOrganization", {}).get("sameAs"),
                    },
                    "education": {
                        "required_credential": data.get("educationRequirements", {}).get("credentialCategory"),
                    },
                    "experience": {
                        "months_of_experience": data.get("experienceRequirements", {}).get("monthsOfExperience"),
                        "seniority_level": data.get("experienceRequirements", {}).get("seniorityLevel", ""),
                    },
                    "salary": {
                        "currency": data.get("estimatedSalary", {}).get("currency"),
                        "min_value": data.get("estimatedSalary", {}).get("value", {}).get("minValue"),
                        "max_value": data.get("estimatedSalary", {}).get("value", {}).get("maxValue"),
                        "unit": data.get("estimatedSalary", {}).get("value", {}).get("unitText"),
                    },
                    "location": {
                        "country": data.get("jobLocation", {}).get("address", {}).get("addressCountry"),
                        "locality": data.get("jobLocation", {}).get("address", {}).get("addressLocality"),
                        "region": data.get("jobLocation", {}).get("address", {}).get("addressRegion"),
                        "postal_code": data.get("jobLocation", {}).get("address", {}).get("postalCode"),
                        "street_address": data.get("jobLocation", {}).get("address", {}).get("streetAddress"),
                        "latitude": data.get("jobLocation", {}).get("latitude"),
                        "longitude": data.get("jobLocation", {}).get("longitude"),
                    },
                }

                
                output_path = os.path.join(output_dir, f"transformed_{file_name}")
                with open(output_path, 'w') as f:
                    json.dump(transformed_data, f, indent=4)

        except Exception as e:
            logging.error(f"An error occurred while processing file {file_name}: {str(e)}. Skipping transformation.")
            continue
    return "Transformation completed successfully."