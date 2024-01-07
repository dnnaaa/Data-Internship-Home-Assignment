import pandas as pd

def extract_data(**kwargs):

    extracted_path = 'staging/extracted'

    data = pd.read_csv("source/jobs.csv")

    for index, row in data.iterrows():
        context_data = row['context']
        with open(f'{extracted_path}/extracted_{index}.txt', 'w') as file:
            file.write(str(context_data))
