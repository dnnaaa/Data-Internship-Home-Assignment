import json
import os
from .data_cleaner import clean_description

def safe_get(data, keys, default=None):
    """Safely navigate through nested dictionaries"""
    if isinstance(data, dict):
        for key in keys:
            try:
                data = data.get(key, default)
            except AttributeError:
                return default
            if not isinstance(data, dict):
                break
        return data if data is not None else default
    return default


def transform_job(job_data):
    """Transform raw job data with safe nested access"""
    # Helper for nested dictionary access
    def nested_get(parent, *keys):
        obj = safe_get(job_data, [parent], {})
        return safe_get(obj, list(keys), "" if keys[-1] == "value" else 0)

    # Transform all fields with safety checks
    transformed = {
        "job": {
            "title": job_data.get("title", ""),
            "industry": job_data.get("industry", ""),
            "description": clean_description(job_data.get("description", "")),
            "employment_type": job_data.get("employmentType", ""),
            "date_posted": job_data.get("datePosted", ""),
        },
        "company": {
            "name": safe_get(job_data, ["hiringOrganization", "name"], ""),
            "link": safe_get(job_data, ["hiringOrganization", "sameAs"], ""),
        },
        "education": {
            "required_credential": safe_get(job_data, ["educationRequirements", "credentialCategory"], "")
        },
        "experience": {
            "months_of_experience": safe_get(job_data, ["experienceRequirements", "monthsOfExperience"], 0),
            "seniority_level": None  # Field not in source data
        },
        "salary": {
            "currency": safe_get(job_data, ["estimatedSalary", "currency"], ""),
            "min_value": safe_get(job_data, ["estimatedSalary", "value", "minValue"], 0),
            "max_value": safe_get(job_data, ["estimatedSalary", "value", "maxValue"], 0),
            "unit": safe_get(job_data, ["estimatedSalary", "value", "unitText"], ""),
        },
        "location": {
            "country": safe_get(job_data, ["jobLocation", "address", "addressCountry"], ""),
            "locality": safe_get(job_data, ["jobLocation", "address", "addressLocality"], ""),
            "region": safe_get(job_data, ["jobLocation", "address", "addressRegion"], ""),
            "postal_code": safe_get(job_data, ["jobLocation", "address", "postalCode"], ""),
            "street_address": safe_get(job_data, ["jobLocation", "address", "streetAddress"], ""),
            "latitude": safe_get(job_data, ["jobLocation", "latitude"], ""),
            "longitude": safe_get(job_data, ["jobLocation", "longitude"], ""),
        }
    }

    # Remove empty sections
    return {k: v for k, v in transformed.items() if not all(
        val == "" or val == 0 or val is None for val in (v.values() if isinstance(v, dict) else [v])
    )}


def transform_and_save_job():
    # Configure paths
    input_file = "staging/extracted/pure_jobs.json.txt"
    output_dir = "staging/transformed"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "all_jobs.json")

    # Process all jobs
    transformed_jobs = []

    with open(input_file, "r") as f:
        for line in f:
            try:
                job_data = json.loads(line)
                transformed = transform_job(job_data)
                transformed_jobs.append(transformed)
            except (json.JSONDecodeError, TypeError, AttributeError) as e:
                continue  # Skip invalid entries

    # Save all jobs in single file
    with open(output_file, "w") as f:
        json.dump(transformed_jobs, f, indent=2, ensure_ascii=False)