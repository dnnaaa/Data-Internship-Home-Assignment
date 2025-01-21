import pandas as pd
import json
import os


def extract_from_csv():
        # Read CSV with proper type handling
        df = pd.read_csv('source/jobs.csv',
                        header=None,
                        names=['id', 'job_data'],
                        dtype={'id': 'Int64'})

        # Filter valid entries with both ID and job_data
        valid_jobs = df.dropna(subset=['id', 'job_data'])

        # Create output directory
        output_dir = 'staging/extracted'
        os.makedirs(output_dir, exist_ok=True)

        # Process and save clean JSON data
        output_file = os.path.join(output_dir, 'pure_jobs.json.txt')

        with open(output_file, 'w') as f:
            for job_data_str in valid_jobs['job_data']:
                try:
                    # Validate and parse JSON
                    job_json = json.loads(job_data_str)
                    # Write as compact JSON line
                    f.write(json.dumps(job_json) + '\n')
                except json.JSONDecodeError:
                    continue  # Skip invalid entries silently