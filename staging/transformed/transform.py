import os
import json

def transform():
    """Transform the data in the staging directory."""
    staging_dir = "staging/raw"
    transformed_dir = "staging/transformed"

    # Clear transformed directory
    shutil.rmtree(transformed_dir, ignore_errors=True)
    os.makedirs(transformed_dir)

    # Transform files in staging directory
    file_list = os.listdir(staging_dir)
    for file_name in file_list:
        file_path = os.path.join(staging_dir, file_name)
        transformed_file_path = os.path.join(transformed_dir, file_name)

        with open(file_path, "r") as f:
            data = json.load(f)

        # Perform data transformation
        transformed_data = transform_data(data)

        # Save transformed data
        with open(transformed_file_path, "w") as f:
            json.dump(transformed_data, f)