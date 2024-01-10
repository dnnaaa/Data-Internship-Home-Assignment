import pandas as pd
import os

def extract_job_data(source_csv_path: str, extracted_folder: str) -> None:
    os.makedirs(extracted_folder, exist_ok=True)

    df = pd.read_csv(source_csv_path)

    for index, row in df.iterrows():
        context_data = row['context']
        text_file_path = os.path.join(extracted_folder, f'extracted_{index}.txt')

        with open(text_file_path, 'w') as file:
            file.write(str(context_data))
