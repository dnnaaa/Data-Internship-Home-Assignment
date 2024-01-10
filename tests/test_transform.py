from unittest.mock import patch
from dags.transform import transform_json_data

sample_json_data = {
    "title": "Software Engineer",
    "industry": "Technology",
    "description": "Cleaned description",
    "employmentType": "Full-Time",
    "datePosted": "2022-01-01",
    "hiringOrganization": {
        "name": "ABC Company",
        "sameAs": "https://www.example.com"
    },
    "educationRequirements": {
        "credentialCategory": "Bachelor's Degree"
    },
    "experienceRequirements": {
        "monthsOfExperience": 24
    },
    "estimatedSalary": {
        "currency": "USD",
        "value": {
            "minValue": 50000,
            "maxValue": 80000,
            "unitText": "YEAR"
        }
    },
    "jobLocation": {
        "address": {
            "addressCountry": "USA",
            "addressLocality": "City",
            "addressRegion": "State",
            "postalCode": "12345",
            "streetAddress": "123 Main St"
        },
        "latitude": 40.7128,
        "longitude": -74.0060
    }
}

@patch('dags.transform.clean_text', return_value="Cleaned description")
def test_transform_json_data(mock_clean_text):
    result = transform_json_data(sample_json_data)

    assert result["job"]["title"] == "Software Engineer"
    assert result["company"]["name"] == "ABC Company"
    assert result["education"]["required_credential"] == "Bachelor's Degree"
    assert result["experience"]["months_of_experience"] == 24
    assert result["experience"]["seniority_level"] is None
    assert result["job"]["description"] == "Cleaned description"