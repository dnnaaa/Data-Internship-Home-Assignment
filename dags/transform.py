import json
import os
from airflow.decorators import task
import re

def clean_description(description):
    """Clean the job description by removing HTML tags and unnecessary whitespace."""
    patterns = {
        r'&lt;br&gt;': '   ',
        r'<br&gt;': '\n',
        r'&lt;strong&gt;': '',
        r'&lt;/strong&gt;': '',
        r'&lt;u&gt;': '',
        r'&lt;/u&gt;': '',
        r'&lt;li&gt;': '  - ',
        r'&lt;/li&gt;': '   \n',
        r'&lt;ul&gt;': '',
        r'&lt;/ul&gt;': '',
        r'&amp;amp;': '&',
        r'&lt;': '<',
        r'&gt;': '>',
        r'&quot;': '"',
        r'&apos;': "'",
    }
    for pattern, replacement in patterns.items():
        description = re.sub(pattern, replacement, description)
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', description)
    return cleantext.strip()

def extract_salary(description):
    """Extract salary information from the job description."""
    salary_pattern = re.compile(r'Salary Range: (\$)([0-9]+[kK]) - (\$)([0-9]+[kK])')
    match = salary_pattern.search(description)
    if match:
        currency = match.group(1)
        min_salary = match.group(2).replace('k', '000').replace('K', '000')
        max_salary = match.group(4).replace('k', '000').replace('K', '000')
        return currency, int(min_salary), int(max_salary)
    return None, None, None

def extract_seniority_level(title, description, months_of_experience):
    """Extract seniority level from the job title, description, or months of experience."""
    seniority_keywords = ['senior', 'junior']
    for keyword in seniority_keywords:
        if re.search(keyword, title, re.IGNORECASE) or re.search(keyword, description, re.IGNORECASE):
            return keyword.capitalize()
    if months_of_experience is not None:
        if months_of_experience >= 60:
            return 'Senior'
        elif months_of_experience < 24:
            return 'Junior'
    return None
@task()
def transform():
    """Clean and convert extracted elements to json."""
    os.makedirs('/opt/airflow/data/staging/transformed', exist_ok=True)
    for filename in os.listdir('/opt/airflow/data/staging/extracted'):
        with open(f'/opt/airflow/data/staging/extracted/{filename}', 'r') as f:
            content = f.read()
            if not content.strip():  # Skip empty files
                continue
            try:
                data = json.loads(content)  # Attempt to parse JSON
                if not isinstance(data, dict):  # Ensure it's a JSON object
                    raise ValueError("Content is not a valid JSON object")
            except (json.JSONDecodeError, ValueError):
                # Fall back to treating content as plain text
                data = {"description": content.strip()}

            # Safely extract fields from data
            description = data.get("description", "")
            currency, min_salary, max_salary = extract_salary(description)
            months_of_experience = (
                data.get("experienceRequirements", {}).get("monthsOfExperience")
                if isinstance(data.get("experienceRequirements"), dict) else None
            )
            seniority_level = extract_seniority_level(
                data.get("title", ""),
                description,
                months_of_experience,
            )
            address = data.get("jobLocation", {}).get("address", {})
            street_address = address.get("streetAddress") or None

            # Construct the transformed data structure
            transformed_data = {
                "job": {
                    "title": data.get("title"),
                    "industry": data.get("industry"),
                    "description": clean_description(description),
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
                    "months_of_experience": months_of_experience,
                    "seniority_level": seniority_level,
                },
                "salary": {
                    "currency": currency,
                    "min_value": min_salary,
                    "max_value": max_salary,
                    "unit": "YEAR",  # No default salary unit
                },
                "location": {
                    "country": address.get("addressCountry"),
                    "locality": address.get("addressLocality"),
                    "region": address.get("addressRegion"),
                    "postal_code": address.get("postalCode"),
                    "street_address": street_address,
                    "latitude": data.get("jobLocation", {}).get("latitude"),
                    "longitude": data.get("jobLocation", {}).get("longitude"),
                },
            }

            # Write transformed data to a JSON file
            output_filename = f'/opt/airflow/data/staging/transformed/{os.path.splitext(filename)[0]}.json'
            with open(output_filename, 'w') as tf:
                json.dump(transformed_data, tf, indent=4)
