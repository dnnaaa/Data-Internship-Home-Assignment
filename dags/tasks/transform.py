import json
import os
from pathlib import Path
from bs4 import BeautifulSoup
from airflow.decorators import task

def description_cleaning(description: str) -> str:
    """Cleaning of the job description."""
    # Use BeautifulSoup to clean HTML tags and get just the text from the description
    soup = BeautifulSoup(description, 'html.parser')
    description_after_cleaning = soup.get_text()  
    # Make sure any special characters are correctly decoded
    return description_after_cleaning.encode('utf-8').decode('unicode_escape')

@task()
def transform(input_dir: str = "staging/extracted", 
              output_dir: str = "staging/transformed"):
    """Transform job posting data from text files to desired JSON format."""
    # Change input and output folder paths into Path objects to make working with them easier
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    
    # If the output folder does not exist, create it
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Check if there are any text files in the input folder
    if not any(input_path.glob('*.txt')):
        raise FileNotFoundError(f"No text files found in {input_dir}")  # Show an error if no files are found
    
    # Go through each text file in the input folder
    for file_path in input_path.glob('*.txt'):
        try:
            # Read the content of the file and load it as a JSON object
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            # Change the data into the format we want
            transformed_data = {
                "job": {
                    "title": data.get("title", ""),
                    "industry": data.get("industry", ""),
                    "description": description_cleaning(data.get("description", "")),
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
                }
            }
            
            # Save the transformed data as a JSON file
            output_file = output_path / f"{file_path.stem}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(transformed_data, f, indent=4, ensure_ascii=False)
                
            print(f"Successfully transformed {file_path.name}")
            
        except json.JSONDecodeError as e:
            # Show an error if the JSON format is incorrect in the file
            raise ValueError(f"JSON decode error in file {file_path.name}: {str(e)}")  # Critical error, raise ValueError
            
        except Exception as e:
            # Show a more general error if something unexpected happens
            raise RuntimeError(f"Error processing file {file_path.name}: {str(e)}")  # Critical error, raise RuntimeError
    
    print(f"-----Successful Transformation. Transformed files saved in {output_dir}-----")
