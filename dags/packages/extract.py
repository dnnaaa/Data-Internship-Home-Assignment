import csv
import json
import os


def extract_csv_data(source_file, destination_dir):
    with open(source_file, mode='r', encoding='utf-8') as file:

        csv_reader = csv.DictReader(file)

        # Iterate over each row in the CSV file
        for i, row in enumerate(csv_reader):
            # Extract the context column data
            context_data = row['context']

            # Check if the context data is not empty
            if context_data.strip():
                # File path for the output text file
                output_file_path = os.path.join(
                    destination_dir, f'extracted_{i}.txt')

                # Write the context data to a text file
                with open(output_file_path, mode='w', encoding='utf-8') as output_file:
                    output_file.write(context_data)
            else:
                # Handle empty context data (skip or log, etc.)
                print(f"Skipping empty context at line {i + 1}")

    return f"files created in {destination_dir}"
