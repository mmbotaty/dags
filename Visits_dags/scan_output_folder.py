import pandas as pd
import os
import pyodbc


# Function to scan input folder for new files

def scan_output_folder(folder_path):
    
    """
    Reads all Excel files (.xlsx) in a folder and returns a dictionary of DataFrames, where each key is the filename
    (without extension) and the value is the corresponding DataFrame.
    """
    excel_files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx')]
    dataframes = {}
    for file in excel_files:
        file_path = os.path.join(folder_path, file)
        dfs = pd.read_excel(file_path, sheet_name=None)
        for sheet_name, df in dfs.items():
            dataframes[f'{file[:-5]}_{sheet_name}'] = df
    return dataframes