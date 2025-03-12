import os
import pandas as pd
import duckdb
import gdown
from sqlalchemy import create_engine
from dotenv import load_dotenv

from duckdb import DuckDBPyRelation
from pandas import DataFrame

load_dotenv()


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
def read_csv_duckdb(path_file):
    df_duckdb = duckdb.read_csv(path_file)
   
    return df_duckdb 


# Function to convert DuckDB to Pandas and save the DataFrame in PostgreSQL.

def save_postgre(df_duckdb,table):
    DATABASE_URL = os.getenv("DATABASE_URL")
    engine = create_engine(DATABASE_URL)

    # Save the DataFrame Postegre
    df_duckdb.to_sql(table, con=engine, if_exists='append', index=False)


def etl(df: DuckDBPyRelation) -> DataFrame:

    # Execute a SQL query that includes a new column, operating on a virtual table.
    df_etl = duckdb.sql("SELECT *, quantidade * valor AS total_vendas FROM df").df()

    #Remove the record from the virtual table for cleanup.
    return df_etl



if __name__ == "__main__":
    
    url_folder = 'https://drive.google.com/drive/folders/1T100DU3JJ6zzHrjgSdvnNY7VIOfEzVuh'
    folder_local = './folder_gdown'
    #google_drive_file(url_folder,folder_local)
    files_list = list_files_csv(folder_local)  

    for  path_file in files_list:
         duckdb_df_etl = read_csv_duckdb(path_file)
         pnadas_df_etl = etl(duckdb_df_etl)
         save_postgre(pnadas_df_etl, "vendas_calculado") 

    #data_frame_duckdb = read_csv_duckdb(files_list)
    #etl(data_frame_duckdb)
    
    