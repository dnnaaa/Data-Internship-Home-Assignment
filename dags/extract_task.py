import csv
import json
import os

def extract_jobs():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    source_file_path = os.path.join(script_dir, "jobs.csv")
    staging_dir = os.path.join(script_dir, "staging/extracted")

    # create staging directory if it doesn't exist
    if not os.path.exists(staging_dir):
        os.makedirs(staging_dir)

    with open(source_file_path, 'r') as file:
        csv_reader = csv.DictReader(file)
        for i, row in enumerate(csv_reader):
            try:
                # extractingg the 'context' column data
                context_data = row['context']
                data = json.loads(context_data)

                # save to staging/extracted as a text file
                output_file_path = os.path.join(staging_dir, f"extracted_{i}.txt")
                with open(output_file_path, 'w') as output_file:
                    json.dump(data, output_file)

            except KeyError:
                print(f"Column 'context' not found in row {i+1}")
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON at row {i+1}: {e}")

#extract_jobs()