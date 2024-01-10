import pandas as pd
import os

def extract_jobs_csv(file_path, output_dir):
    print('hi')
    df = pd.read_csv(file_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    for index, row in df.iterrows():
        file_name = f"{output_dir}/job_{index}.txt"
        with open(file_name, 'w') as file:
            # Convert the context to string before writing
            file.write(str(row['context']))



