import os
import pandas as pd

def extract_job():
    input_file = 'source/jobs.csv'
    output_folder = 'staging/extracted'
    os.makedirs(output_folder, exist_ok=True)

    data = pd.read_csv(input_file)
    for idx, context in enumerate(data['context']):
        with open(os.path.join(output_folder, f'job_{idx}.txt'), 'w') as file:
            file.write(context)
