
import os
import pandas as pd
from airflow.decorators import task

@task()
def extract(source_file, output_dir):

    if not os.path.isfile(source_file):
        raise FileNotFoundError(f"Source file '{source_file}' not found.")
    try:
        data = pd.read_csv(source_file)
    except Exception as e:
        raise RuntimeError(f"Failed to load the source file '{source_file}'. Error: {e}")

    if 'context' not in data.columns:
        raise ValueError(f"The column 'context' is missing in the source file '{source_file}'.")
    
    os.makedirs(output_dir, exist_ok=True)
    
    file_paths = []
    
    for idx, value in enumerate(data['context']):
        if pd.notnull(value):
            filename = f"context_{idx + 1}.txt"
            file_path = os.path.join(output_dir, filename)
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(str(value))
            file_paths.append(file_path)

    print(f"Extraction completed. {len(file_paths)} files saved in '{output_dir}'.")
    return output_dir