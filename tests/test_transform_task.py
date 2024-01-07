import json
from unittest.mock import patch
from dags.etl import transform_task, clean_description

def test_transform_task(tmp_path):
    # Create a mock file in the staging/extracted directory
    extracted_dir = tmp_path / "staging/extracted"
    extracted_dir.mkdir(parents=True, exist_ok=True)

    input_filename = "test_file.txt"
    input_file_content = '{"title": "Test Job", "description": "Test Description"}'
    input_file_path = extracted_dir / input_filename
    input_file_path.write_text(input_file_content)

    # Set up mocks for listing files and cleaning the description
    with patch("dags.etl.os.listdir", return_value=[input_filename]):
        with patch("dags.etl.clean_description", return_value="Cleaned Test Description") as mock_clean_description:
            # Call the function to be tested
            transform_task()


    # Check if the transformed file was created with the expected content
    output_filename = f"{input_filename.split('.')[0]}.json"
    output_file_path = tmp_path / "staging/transformed" / output_filename

    assert output_file_path.exists()  

    with open(output_file_path, "r") as output_file:
        actual_transformed_data = json.load(output_file)

    expected_transformed_data = {
        'job': {
            'title': 'Test Job',
            'description': 'Cleaned Test Description',
        }
    }

    assert actual_transformed_data == expected_transformed_data  

    # i Check if the clean_description function was called with the expected argument
    mock_clean_description.assert_called_once_with("Test Description")
