import pandas as pd
import os, json, html 

def load_and_clean(path_to_data):
    data = pd.read_csv(path_to_data)
    data.dropna(axis=0, inplace=True)

    return data

def text_decoder(text):
    decoded_text = html.unescape(text)
    utf8_text = decoded_text.encode('utf-8')

    return utf8_text.decode('utf-8')

def get_data_as_json():
    data_as_json = []
    files = os.listdir(os.path.join('staging', 'extracted'))
    files = [file for file in files if file.split('.')[1] == 'txt']
    for file in files:
        with open(os.path.join('staging', 'extracted', file), 'r') as f:
            # Load the JSON data from the file
            data = json.load(f)
            for key in data.keys():
                if isinstance(data[key], str):
                    data[key] = text_decoder(data[key])
            data_as_json.append(data)
        f.close()
    return data_as_json

def get_values(data_as_json):
    mapping = {
        'job': {
            'date_posted': 'datePosted',
            'description': 'description',
            'employment_type': 'employmentType',
            'industry': 'industry',
            'title': 'title',
        },
        'location': {
            'country': 'jobLocation.address.addressCountry',
            'latitude': 'jobLocation.latitude',
            'locality': 'jobLocation.address.addressLocality',
            'longitude': 'jobLocation.longitude',
            'postal_code': 'jobLocation.address.postalCode',
            'region': 'jobLocation.address.addressRegion',
            'street_address': 'jobLocation.address.streetAddress',
        },
        'company': {
            'link': 'hiringOrganization.sameAs',
            'name': 'hiringOrganization.name',
        },
        'salary': {
            'currency': 'estimatedSalary.currency',
            'max_value': 'estimatedSalary.value.maxValue',
            'min_value': 'estimatedSalary.value.minValue',
            'unit': 'estimatedSalary.value.unitText',
        },
        'education': {
            'required_credential': 'educationRequirements.credentialCategory',
        },
        'experience': {
            'months_of_experience': 'experienceRequirements.monthsOfExperience',
            'seniority_level': "I don't know",
        },
    }
    transformed_data = []
    for data in data_as_json:
        template = {
            "job": {
                "title": None,
                "industry": None,
                "description": None,
                "employment_type": None,
                "date_posted": None,
            },
            "company": {
                "name": None,
                "link": None,
            },
            "education": {
                "required_credential": None,
            },
            "experience": {
                "months_of_experience": None,
                "seniority_level": None,
            },
            "salary": {
                "currency": None,
                "min_value": None,
                "max_value": None,
                "unit": None,
            },
            "location": {
                "country": None,
                "locality": None,
                "region": None,
                "postal_code": None,
                "street_address": None,
                "latitude": None,
                "longitude": None,
            },
        }
        for section, section_mapping in mapping.items():
            for key_in_template, key_in_data in section_mapping.items():
                keys = key_in_data.split('.')
                value = data
                for key in keys:
                    if key in value:
                        value = value[key]
                    else:
                        break
                else:
                    template[section][key_in_template] = value
        transformed_data.append(template)
    return transformed_data

def get_transformed_data():
    transformed_data = []
    files = os.listdir(os.path.join('staging', 'transformed'))
    files = [file for file in files if file.split('.')[1] == 'json']
    for file in files:
        with open(os.path.join('staging', 'transformed', file), 'r') as f:
            data = json.load(f)
            transformed_data.append(data)
        f.close()

    return transformed_data

