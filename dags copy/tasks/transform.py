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
        file_path = os.path.join(input_dir, file_name)  # Correct file path

        # Check if the file is empty
        if os.path.getsize(file_path) == 0:
            logging.error(f"File {file_name} is empty. Skipping transformation.")
            continue
        
        try:
            with open(file_path, 'r') as f:  # Use full path here
                content = f.read()
                if not content.strip():  # Check if file is empty
                    logging.error(f"File {file_name} is empty. Skipping transformation.")
                    continue

                # Log the content for debugging
                logging.info(f"Content of {file_name}: {content}")

                data = json.loads(content)
                # Process the data
        except json.JSONDecodeError as e:
            logging.error(f"File {file_name} contains invalid JSON: {str(e)}. Skipping transformation.")
            continue
        except Exception as e:
            logging.error(f"An error occurred while reading file {file_name}: {str(e)}. Skipping transformation.")
            continue

        # Perform the transformation
        transformed_data = {
            "job": {
                "title": data.get("job_title"),
                "industry": data.get("job_industry"),
                "description": data.get("job_description"),
                "employment_type": data.get("job_employment_type"),
                "date_posted": data.get("job_date_posted"),
            },
            "company": {
                "name": data.get("company_name"),
                "link": data.get("company_linkedin_link"),
            },
            "education": {
                "required_credential": data.get("job_required_credential"),
            },
            "experience": {
                "months_of_experience": data.get("job_months_of_experience"),
                "seniority_level": data.get("seniority_level"),
            },
            "salary": {
                "currency": data.get("salary_currency"),
                "min_value": data.get("salary_min_value"),
                "max_value": data.get("salary_max_value"),
                "unit": data.get("salary_unit"),
            },
            "location": {
                "country": data.get("country"),
                "locality": data.get("locality"),
                "region": data.get("region"),
                "postal_code": data.get("postal_code"),
                "street_address": data.get("street_address"),
                "latitude": data.get("latitude"),
                "longitude": data.get("longitude"),
            },
        }

        # Write the transformed data to an output file
        output_path = os.path.join(output_dir, f"transformed_{file_name}")
        with open(output_path, 'w') as f:
            json.dump(transformed_data, f, indent=4)

    # Return a success message or status
    return "Transformation completed successfully."