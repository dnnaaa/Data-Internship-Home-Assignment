import src.constants as constants
import src.io_utils as io_utils

import src.processing_utils as processing_utils


def transform_data():
    # Sort files in the directory OUTPUT_DIR_EXTRACTED using a natural sorting key
    files = io_utils.list_files_sorted(constants.OUTPUT_DIR_EXTRACTED)

    io_utils.create_file_dir(constants.OUTPUT_DIR_TRANSFORMED)
    files_len = len(files)
    print(f"Transforming {files_len} files")

    # Process each file in the sorted list
    for i, file_name in enumerate(files):
        # Construct the full path for each file
        input_dir = f"{constants.OUTPUT_DIR_EXTRACTED}/{file_name}"

        # Read the JSON content from the file
        data = io_utils.read_json(input_dir)

        # Transform the data using the transform_data function
        # and write the transformed data to a new JSON file
        io_utils.write_json(f"{constants.OUTPUT_DIR_TRANSFORMED}/output{i}.json", processing_utils.transform_data(data))
        if i > 0 and i % 100 == 0:
             print(f"Transformed {i+1}/{files_len} files")
