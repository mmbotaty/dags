from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

from datetime import datetime
import pandas as pd
import os


"""
visits_dag.py - Airflow DAG to fetch visits xlsx data from folder and load it to sql server db .

This DAG listnes to a folder for new files. When a new file is added to the folder, the DAG extracts the data from the file, transforms it, and saves it sql database with pyodbc.

DAG parameters:
    - start_date: datetime object representing the start date of the DAG
    - schedule_interval: a string representing the interval at which the DAG runs
    - default_args: a dictionary containing default arguments for the DAG

Tasks:
    - extract_data: extracts worksheets and sheets from the input folder
    - scan_output_folder: scans the input folder for new files
    - scan_row_edit: scans the row edit of the input folder
    - transform_data: transforms the data by adding a timestamp column
    - save_to_sql: saves the transformed data to sql server database

Dependencies:
    - extract_data must run before transform_data
    - transform_data must run before save_to_sql

"""

# Define the DAG

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 4, 10)
}

dag = DAG('extract_data', default_args=default_args, schedule_interval="@daily",catchup=False) 

def extract_visits_data(folder_path):
    """
    Reads all Excel files (.xlsx) in a folder and returns a dictionary of DataFrames, where each key is the filename
    (without extension) and the value is the corresponding DataFrame.
    """
    if processed_files is None:
       processed_files = []
    
    excel_files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx') and f not in processed_files]
    dataframes = {}
    for file in excel_files:
        file_path = os.path.join(folder_path, file)
        dfs = pd.read_excel(file_path, engine='openpyxl', sheet_name=None)      
        for sheet_name, df in dfs.items():
            dataframes[f'{file[:-5]}_{sheet_name}'] = df
        processed_files.append(file)
    
    return dataframes, processed_files





# Define the tasks  
# extact data from the input folder
extract_data = PythonOperator(
    task_id='extract_data',
    python_callable= extract_visits_data,
)

scan_output_folder = BranchPythonOperator(
    task_id='scan_output_folder',
    python_callable= scan_output_folder,
    dag=dag
)
