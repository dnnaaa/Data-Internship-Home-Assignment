import json
import logging
from pathlib import Path
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(level=logging.INFO)

def clean_description(description):
    """Cleans HTML tags and decodes Unicode characters in job descriptions."""
    soup = BeautifulSoup(description, 'html.parser')
    return soup.get_text()

def transform():
    """Transforms extracted text files into the desired JSON schema."""
    # Directories
    extracted_dir = Path('/home/anas/Data-Internship-Home-Assignment/staging/extracted')
    transformed_dir = Path('/home/anas/Data-Internship-Home-Assignment/staging/transformed')

    # Ensure the transformed directory exists
    transformed_dir.mkdir(parents=True, exist_ok=True)

    # Check if there are files to process
    if not any(extracted_dir.glob('*.txt')):
        raise FileNotFoundError("No extracted files found in 'staging/extracted'.")

    # Process each file
    for file_path in extracted_dir.glob('*.txt'):
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)

            # Transform the data
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
                    "seniority_level": data.get("experienceRequirements", {}).get("seniorityLevel", ""),
                },
                "salary": {
                    "currency": data.get("estimatedSalary", {}).get("currency", ""),
                    "min_value": data.get("estimatedSalary", {}).get("value", {}).get("minValue", ""),
                    "max_value": data.get("estimatedSalary", {}).get("value", {}).get("maxValue", ""),
                    "unit": data.get("estimatedSalary", {}).get("value", {}).get("unitText", ""),
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

            # Save the transformed file
            transformed_file_path = transformed_dir / f"{file_path.stem}.json"
            with open(transformed_file_path, 'w', encoding='utf-8') as transformed_file:
                json.dump(transformed_data, transformed_file, indent=4, ensure_ascii=False)

            logging.info(f"Successfully transformed {file_path.stem}")

        except json.JSONDecodeError:
            logging.error(f"JSON decoding error in file {file_path}. Skipping.")
            continue
        except Exception as e:
            logging.error(f"Error processing file {file_path}: {str(e)}")
            continue

    logging.info("Transformation completed. Transformed files saved.")

if __name__ == "__main__":
    try:
        transform()
    except FileNotFoundError as e:
        logging.error(str(e))
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")
