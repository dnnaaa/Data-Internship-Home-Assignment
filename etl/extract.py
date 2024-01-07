import pandas as pd
from pathlib import Path
from airflow.decorators import task

@task()
def extract_to_text_file():
    # Read the Dataframe from source/jobs.csv
    df = pd.read_csv("source/jobs.csv")

    # Extract the context column data and save each item to staging/extracted as a text file
    extracted_folder = Path("staging/extracted")
    extracted_folder.mkdir(parents=True, exist_ok=True)

    for index, row in df.iterrows():
        context_data = row["context"]
        with open(extracted_folder / f"extracted_{index}.txt", "w") as file:
            file.write(str(context_data))
