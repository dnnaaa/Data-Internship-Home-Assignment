import pandas as pd

def extract_task():

    df = pd.read_csv('source/jobs.csv',index_col=0)
    df = df.dropna().drop_duplicates()
    df.drop(df[df['context'] == '{}'].index, inplace=True)
    df.reset_index(drop=True, inplace=True)

    for i in df.index : 
         with open(f"staging/extracted/file_{i}.txt", "w") as f : 
              f.write(df.iloc[i]['context'])

