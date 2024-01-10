import pandas as pd
import src.constants as constants

import src.io_utils as io_utils

def extract_data():
    #added index_col=0 to drop the index column
    data = pd.read_csv(constants.FILE_PATH, index_col=0)

    #drop the null rows from data horizontaly (where all collumns are null, in this case context)
    #empty rows in a collumn a treated as np.nan (float) => error when joining the list by \n
    data = data.dropna(axis=0, how="all")

    #transform context data to list to join with a new line delimiter
    data = data["context"].tolist()

    io_utils.create_file_dir(constants.OUTPUT_DIR_EXTRACTED)
    data_len = len(data)
    print(f"Extracting {data_len} records")
    for i, row in enumerate(data):
        #write output to txt file
        io_utils.write_txt(f"{constants.OUTPUT_DIR_EXTRACTED}/context{i}.txt", row)
        
        if i > 0 and i % 100 == 0:
            print(f"Extracted {i+1}/{data_len} records")
