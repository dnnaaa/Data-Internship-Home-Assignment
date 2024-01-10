import os
import json
from airflow.decorators import task
import html
import re

@task()
def transform():
    """Clean and convert extracted elements to json."""
    extracted_dir = 'staging/extracted'
    transformed_dir = 'staging/transformed'


    os.makedirs(transformed_dir, exist_ok=True)

    from bs4 import BeautifulSoup

    def clean_description(description):
        if description:
            soup = BeautifulSoup(description, 'html.parser')
            text_content = soup.get_text(separator='\n\n', strip=True)
            clean_text = re.sub(r'<[^>]*>', '', text_content)
            return clean_text
        return ""

    def clean_date(date_str):
        if date_str:
            try:
                parsed_date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
                return parsed_date.strftime("%Y-%m-%d")
            except ValueError:
                pass
        return ""

    def clean_name(name_str):
        if name_str:
            return html.unescape(name_str)
        return ""

    def clean_link(link_str):
        if link_str:
            return html.unescape(link_str)
        return ""

    def clean_title(title_str):
        if title_str:
            return html.unescape(title_str)
        return ""

    def clean_industry(industry_str):
        if industry_str:
            return [html.unescape(industry.strip()) for industry in industry_str.split(",")]
        return []

    for filename in os.listdir(extracted_dir):
        if filename.endswith('.txt'):
            file_path = os.path.join(extracted_dir, filename)
            try:
                with open(file_path, 'r') as file:
                    content = file.read().strip()
                    if content:
                        try:
                            data = json.loads(content)
                            if isinstance(data, dict):
                                transformed_data = {
                                    "job": {
                                        "title": clean_title(data.get("title")),
                                        "industry": clean_industry(data.get("industry")),
                                        "description": clean_description(data.get("description")),
                                        "employment_type": data.get("employmentType"),
                                        "date_posted": clean_date(data.get("datePosted")),
                                    },
                                    "company": {
                                        "name": clean_name(data.get("hiringOrganization", {}).get("name")),
                                        "link": clean_link(data.get("hiringOrganization", {}).get("sameAs")),
                                    },
                                    "education": {
                                        "required_credential": data.get("educationRequirements", {}).get("credentialCategory"),
                                    },
                                    "experience": {
                                        "months_of_experience": data.get("experienceRequirements", {}).get("monthsOfExperience") if isinstance(data.get("experienceRequirements", {}), dict) else None,
                                        "seniority_level": data.get("experience", {}) if isinstance(data.get("experience", {}), dict) else "",
                                    },
                                    "salary": {
                                        "currency": data.get("salary", {}).get("currency", ""),
                                        "min_value": data.get("salary", {}).get("min_value", ""),
                                        "max_value": data.get("salary", {}).get("max_value", ""),
                                        "unit": data.get("salary", {}).get("unit", ""),
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

                                with open(os.path.join(transformed_dir, f'transformed_{filename[:-4]}.json'), 'w') as json_file:
                                    json.dump(transformed_data, json_file, indent=4)
                            else:
                                print(f"Invalid JSON format in file: {file_path}")
                        except json.JSONDecodeError as e:
                            print(f"Error decoding JSON in file: {file_path}")
                            print(f"Error message: {e}")
            except OSError as e:
                print(f"Error reading file: {file_path}")
                print(f"Error message: {e}")
