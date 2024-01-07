import pandas as pd
import os
from airflow.decorators import task


@task
def extract(source_file, staging_dir):

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.join(script_dir, '../../')
    source_file_path = os.path.join(project_dir, source_file)
    staging_dir_path = os.path.join(project_dir, staging_dir)

    if not os.path.exists(staging_dir_path):
        os.makedirs(staging_dir_path)

    df = pd.read_csv(source_file_path)
    all_contexts = []

    for index, row in df.iterrows():
        context = row['context']
        if pd.notnull(context):
            context_str = str(context)
            all_contexts.append(context_str)

    all_contexts_text = "\n".join(all_contexts)
    file_path = os.path.join(staging_dir_path, "extracted.txt")
    with open(file_path, 'w') as file:
        file.write(all_contexts_text)

