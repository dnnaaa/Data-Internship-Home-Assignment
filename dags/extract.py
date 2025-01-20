import pandas as pd
import os

class JobsExtractor:
    def __init__(self, source_path="source/jobs.csv", output_path="staging/extracted"):
        self.source_path = source_path
        self.output_path = output_path
        
    def extract(self):
        """Extract context data from jobs.csv and save to individual text files."""
        try:
            # Create staging directory if it doesn't exist
            os.makedirs(self.output_path, exist_ok=True)
            
            # Read CSV file
            df = pd.read_csv(self.source_path)
            
            # Extract and save context column data
            for index, row in df.iterrows():
                output_file = os.path.join(self.output_path, f"job_{index}.txt")
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(row['context'])
            
            return len(df)
            
        except Exception as e:
            raise Exception(f"Error in extraction: {str(e)}")