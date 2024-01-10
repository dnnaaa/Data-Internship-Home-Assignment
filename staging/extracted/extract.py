import os
import shutil

def extract():
    """Extract data from the source directory and move it to the staging directory."""
    source_dir = "source"
    staging_dir = "staging/raw"

    # Clear staging directory
    shutil.rmtree(staging_dir, ignore_errors=True)
    os.makedirs(staging_dir)

    # Move files from source to staging
    file_list = os.listdir(source_dir)
    for file_name in file_list:
        source_file = os.path.join(source_dir, file_name)
        destination_file = os.path.join(staging_dir, file_name)
        shutil.copy(source_file, destination_file)