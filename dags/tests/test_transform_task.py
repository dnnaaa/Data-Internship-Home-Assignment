import pytest
import json
from unittest.mock import mock_open, patch, MagicMock
from transform_task import transform_data

def test_transform_data_success():
    # mock input JSON
    sample_data = json.dumps({
        "title": "Software Engineer",
        "description": "<p>Job Description</p>",
        "industry": "Technology",
        "employmentType": "Full-time",
        "datePosted": "2021-01-01",
        "hiringOrganization": {
            "name": "Tech Company",
            "sameAs": "http://techcompany.com"
        },
        "jobLocation": {
            "address": {
                "addressCountry": "USA",
                "addressLocality": "New York",
                "addressRegion": "NY",
                "postalCode": "10001",
                "streetAddress": "123 Tech Lane"
            }
        },
      
    })

    expected_output = {
        "job": {
            "title": "Software Engineer",
            "industry": "Technology",
            "description": "Job Description",
            "employment_type": "Full-time",
            "date_posted": "2021-01-01",
        },
        "company": {
            "name": "Tech Company",
            "link": "http://techcompany.com",
        },
        "location": {
            "country": "USA",
            "locality": "New York",
            "region": "NY",
            "postal_code": "10001",
            "street_address": "123 Tech Lane",
            "latitude": "",
            "longitude": "",
        },
        
    }

    mock_input_file = mock_open(read_data=sample_data)
    mock_output_file = MagicMock()

    with patch("builtins.open", mock_input_file), \
         patch("os.path.exists", return_value=True), \
         patch("os.makedirs"), \
         patch("os.listdir", return_value=["test.txt"]), \
         patch("transform_task.open", mock_output_file, create=True):
        transform_data()

    # checkingg if the file was written with the expected output
    mock_output_file.return_value.write.assert_called_with(json.dumps(expected_output, indent=4))

