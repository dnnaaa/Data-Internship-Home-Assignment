import pandas as pd
import json
import html
from airflow.decorators import task

def decode_html(json_str):
    """
    Changes a JSON string into a Python object, fixing HTML characters and errors.
    """
    # Check if the string is empty or not a valid string
    if not isinstance(json_str, str) or not json_str.strip():
        # Return None if the string is invalid
        return None
    
    decoded_str = html.unescape(json_str)
    try:
        # Try to turn the cleaned string into a JSON object
        return json.loads(decoded_str)
    except json.JSONDecodeError as e:
        # Print the error and return None if it can't be turned into JSON
        print(f"JSON decoding failed: {e}")
        return None


@task()
def extract(csv_file_path: str, output_dir: str):
    """
    Gets data from a CSV file, cleans the 'context' column, and saves the results into text files.
    
    Parameters:
    - csv_file_path (str): The location of the CSV file with the data.
    - output_dir (str): The folder where the cleaned data will be saved.
    """
    try:
        # Load the CSV file into a table (DataFrame)
        dataframe = pd.read_csv(csv_file_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"CSV file '{csv_file_path}' not found.")  # Show error if file is missing
    except Exception as e:
        raise ValueError(f"Error reading CSV file: {e}")
    
    # Check if the 'context' column exist
    if 'context' not in dataframe.columns:
        raise KeyError("The 'context' column does not exist in the CSV file.")  # Show error if column is missing
    
    # Clean the data in the 'context' column
    try:
        dataframe['context_cleaned'] = dataframe['context'].apply(decode_html)
    except Exception as e:
        raise ValueError(f"Error while cleaning 'context' column: {e}")
    
    # Save each cleaned context as a text file
    for idx, row in dataframe.iterrows():
        try:
            cleaned_context = row['context_cleaned']
            if cleaned_context:
                # Save each cleaned context into its own text file
                with open(f'{output_dir}/context_{idx}.txt', 'w') as file:
                    json.dump(cleaned_context, file)
        except Exception as e:
            raise ValueError(f"Error saving cleaned context for row {idx}: {e}")

    print(f"Extraction completed. {len(dataframe)} files extracted.")
