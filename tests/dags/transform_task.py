import pytest
from unittest.mock import mock_open, patch
from tasks.transform import transform_task

def test_transform_task(tmpdir):
    # Mock data
    json_content = '{"title": "Software Engineer", "industry": "IT", "employmentType": "FULL_TIME"}'
    input_dir = str(tmpdir)
    output_dir = str(tmpdir)

    # Create a mock input file
    input_file = tmpdir.join("context_0.txt")
    input_file.write(json_content)

    # Mock file operations
    with patch("builtins.open", mock_open(read_data=json_content)) as mock_file:
        transform_task()

        # Verify the mock was called with the correct file paths
        mock_file.assert_any_call(str(input_file), "r")
        mock_file.assert_any_call(f"{output_dir}/transformed_context_0.txt", "w")

        # Verify the transformed content
        expected_output = {
            "job": {
                "title": "Software Engineer",
                "industry": "IT",
                "description": None,
                "employment_type": "FULL_TIME",
                "date_posted": None,
            },
            "company": {"name": None, "link": None},
            "education": {"required_credential": None},
            "experience": {"months_of_experience": None, "seniority_level": ""},
            "salary": {"currency": None, "min_value": None, "max_value": None, "unit": None},
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
        mock_file().write.assert_called_with(json.dumps(expected_output, indent=4))