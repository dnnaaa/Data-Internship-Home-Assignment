import re
from bs4 import BeautifulSoup

def clean_job_description(job_description: str) -> str:
    """
    Cleans job description by removing the present html elements, and separates
    by a space each paragraph separated by a <br> tag.
    
    Args:
        job_description: the raw description of the job

    Returns:
        str: the clean version of job_description
    """
    if job_description:
        # Utilisation de BeautifulSoup pour extraire le texte
        clean_text = BeautifulSoup(job_description, "html.parser").text

        # Remplacement des entités HTML
        clean_text = clean_text.replace("&lt;", "<").replace("&gt;", ">").replace("&nbsp;", " ")

        # Suppression des balises HTML restantes
        pattern = re.compile(r"<.*?>")
        clean_text = re.sub(pattern, "", clean_text)

        # Split et rejointure des éléments pour gérer les <br>
        clean_text = clean_text.split("<br>")
        clean_text = [item.strip() for item in clean_text if item.strip()]
        clean_text = "\n".join(clean_text)

        return clean_text

    return job_description  # Retourne le texte original si job_description est None ou vide

def get_salary(data_dict: dict) -> dict:
    """
    Processes salary information from a JSON file.

    Args:
        data_dict: the dictionary containing the information from the json file.

    Returns:
        dict: A dictionary containing processed salary information.
    """
    salary_info = data_dict.get("estimatedSalary", {}).get("value", {})
    return {
        "currency": data_dict.get("estimatedSalary", {}).get("currency", None),
        "min_value": salary_info.get("minValue", None),
        "max_value": salary_info.get("maxValue", None),
        "unit": salary_info.get("unitText", None)
    }

def get_experience_months(data_dict: dict) -> dict:
    """
    Processes experience_requirements informations from a JSON file.

    Args:
        data_dict: the dictionary containing the information from the json file.

    Returns:
        dict: A dictionary containing processed experience_requirements informations.
    """
    experience_requirements = data_dict.get("experienceRequirements", {})
    # assume experience_months none
    experience_months = None
    if isinstance(experience_requirements, dict):
        # if experience_requirements is dict, find experience_months if it exists
        experience_months = experience_requirements.get("monthsOfExperience", None)

    return experience_months

def get_seniority(job_title: str) -> str:
    """
    Determines the seniority level based on the job title.

    Args:
        job_title: the title of the job

    Returns:
        str: The seniority level ('Senior' or 'Junior').
    """
    if job_title is None:
        seniority_level = "NA"

    elif "senior" in job_title.lower():
        seniority_level = "Senior"

    elif "junior" in job_title.lower():
        seniority_level = "Junior"

    else:
        seniority_level = "NA"
    return seniority_level

def transform_data(data_dict: dict) -> dict:
    """
    Transforms the data from a JSON file into a structured format.

    Args:
        data_dict: the dictionary containing the information from the json file.

    Returns:
        dict: A dictionary containing the structured data.
    """
    job_title = data_dict.get("title", None)

    return {
        "job": {
            "title": job_title,
            "industry": data_dict.get("industry", None),
            "description": clean_job_description(data_dict.get("description", None)),
            "employment_type": data_dict.get("employmentType", None),
            "date_posted": data_dict.get("datePosted", None),
        },
        "company": {
            "name": data_dict.get("hiringOrganization", {}).get("name", None),
            "link": data_dict.get("hiringOrganization", {}).get("sameAs", None),
        },
        "education": {
            "required_credential": data_dict.get("educationRequirements", {}).get("credentialCategory", None),
        },
        "experience": {
            "months_of_experience": get_experience_months(data_dict),
            "seniority_level": get_seniority(job_title),
        },
        "salary": get_salary(data_dict),
        "location": {
            "country": data_dict.get("jobLocation", {}).get("address", {}).get("addressCountry", None),
            "locality": data_dict.get("jobLocation", {}).get("address", {}).get("addressLocality", None),
            "region": data_dict.get("jobLocation", {}).get("address", {}).get("addressRegion", None),
            "postal_code": data_dict.get("jobLocation", {}).get("address", {}).get("postalCode", None),
            "street_address": data_dict.get("jobLocation", {}).get("address", {}).get("streetAddress", None),
            "latitude": data_dict.get("jobLocation", {}).get("latitude", None),
            "longitude": data_dict.get("jobLocation", {}).get("longitude", None),
        }
    }