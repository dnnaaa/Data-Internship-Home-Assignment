import json
from dags.transform import transform_extracted_files, transform_schema
from unittest.mock import patch, mock_open, call 


def test_transform_extracted_files():
    # Mock input data for a single file
    mock_data = {
        "title": "Test Job",
        "industry": "Test Industry",
        "description": "Test Description",
        "employmentType": "Test Employment Type",
        "datePosted": "Test Date Posted",
        "hiringOrganization": {
            "name": "Test Company",
            "sameAs": "Test Company LinkedIn Link"
        },
        "educationRequirements": {
            "credentialCategory": "Test Credential Category"
        },
        "experienceRequirements": {
            "monthsOfExperience": 0
        },
        "jobLocation": {
            "address": {
                "addressCountry": "Test Country",
                "addressLocality": "Test Locality",
                "addressRegion": "Test Region",
                "postalCode": "Test Postal Code"
            }
        }

    }
    mock_file_content = json.dumps(mock_data)
    mock_transformed_data = transform_schema(mock_data)

    # Setup mock for os.listdir
    with patch('os.listdir', return_value=['job_1.txt']), \
         patch('os.path.exists', return_value=True), \
         patch('os.makedirs'), \
         patch('builtins.open', mock_open(read_data=mock_file_content)) as mock_file:
        input_dir = 'dummy_input_dir'
        output_dir = 'dummy_output_dir'
        transform_extracted_files(input_dir, output_dir)

        # Check if open was called correctly for reading and writing
        expected_calls = [call(f'{input_dir}/job_1.txt', 'r'), call(f'{output_dir}/job_1.json', 'w')]
        assert mock_file.call_args_list == expected_calls

        written_content = ''.join(args[0] for args, _ in mock_file().write.call_args_list)
        expected_content = json.dumps(mock_transformed_data, indent=4)
        assert written_content == expected_content
