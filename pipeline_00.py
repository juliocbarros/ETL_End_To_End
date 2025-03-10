import os
import pandas as pd
import duckdb
import gdown
from sqlalchemy import create_engine
from dotenv import load_dotenv


# Download files from Google Drive
def google_drive_file(url_folder, folder_local):
    os.makedirs(folder_local, exist_ok=True)
    gdown.download_folder(url_folder, output=folder_local, quiet=False, use_cookies=False)


# List files local folder 
def list_files_csv(folder_local):
    file_csv = []
    all_files = os.listdir(folder_local)
    for file in all_files:
        if file.endswith(".csv"):
            path = os.path.join(folder_local, file)
            file_csv.append(path)
    
    return file_csv


# Read .csv file and return a Duckdb DataFrame
def read_csv_duckdb(folder_local):
    df_duckdb = duckdb.read_csv(loca_folder)
    print(df_duckdb)
    return df_duckdb 


if __name__ == "__main__":
    url_folder = 'https://drive.google.com/drive/folders/1T100DU3JJ6zzHrjgSdvnNY7VIOfEzVuh'
    folder_local = './folder_gdown'
    #google_drive_file(url_folder,folder_local)
    list_files_csv(folder_local)
    read_csv_duckdb(folder_local)
    