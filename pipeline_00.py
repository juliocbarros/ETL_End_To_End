import os
import pandas as pd
import duckdb
import gdown
from sqlalchemy import create_engine
from dotenv import load_dotenv

from duckdb import DuckDBPyRelation
from pandas import DataFrame

from datetime import datetime

load_dotenv()


def connect_base():
    """ Connects to the DuckDB database; creates the database if it does not exist. """
    return duckdb.connect(database='duckdb', read_only=False)


def create_table_duckdb(con):
    """Create a table if no Exists."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS log_files (
            file_name VARCHAR,
            time TIMESTAMP
        )
    """)


def register_file(con, file_name):
    """ Registers a new file in the database with the current timestamp """
    con.execute("""
        INSERT INTO log_files (file_name, time)
        VALUES (?, ?)
    """, (file_name, datetime.now()))

def files_return(con):
    """ Returns a set with the names of all previously processed files """
    return set(row[0] for row in con.execute("SELECT file_name FROM log_files").fetchall())




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

def list_identifies_files(folder_local):
    """Lists files and identifies whether they are CSV, JSON, or Parquet."""
    files_types = []
    for file in os.listdir(folder_local):
        if file.endswith(".csv") or file.endswith(".json") or file.endswith(".parquet"):
            path = os.path.join(folder_local, file)
            type = file.split(".")[-1]
            files_types.append((path, type))

    return files_types  


def read_files(path_file, type):
    """ Reads the file according to its type and returns a DataFrame """
    try:
        if type == "csv":
            return duckdb.read_csv(path_file)
        if type == "json":
            return pd.read_json(path_file)
        if type == "parquet":
            # Try using DuckDB first as it has built-in Parquet support
            return duckdb.read_parquet(path_file)
        else:
            raise ValueError(f"Unsupported file type: {type}")
    except Exception as e:
        if type == "parquet" and "Missing optional dependency" in str(e):
            raise ImportError("Please install required dependencies for Parquet support:\n"
                            "pip install pyarrow fastparquet")
        raise e



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

def pipeline():
    url_folder = 'https://drive.google.com/drive/folders/1T100DU3JJ6zzHrjgSdvnNY7VIOfEzVuh'
    folder_local = './folder_gdown'
    
    # Download files from Google Drive
    google_drive_file(url_folder, folder_local)
    
    # Use list_identifies_files instead of list_files_csv for better file type handling
    files_list = list_identifies_files(folder_local)
    
    con = connect_base()
    create_table_duckdb(con)
    processed_files = files_return(con)
    
    log = []
    
    for file_path, file_type in files_list:
        file_name = os.path.basename(file_path)
        if file_name not in processed_files:
            try:
                df = read_files(file_path, file_type)
                df_etl = etl(df)
                save_postgre(df_etl, "vendas_calculado")
                register_file(con, file_name)
                print(f"File {file_name} Save!!")
                log.append(f"File {file_name} save!!")
            except Exception as e:
                print(f"Error processing {file_name}: {str(e)}")
                log.append(f"Error processing {file_name}: {str(e)}")
        else:
            print(f"File {file_name} already processed")
            log.append(f"File {file_name} already processed")

    return log        




if __name__ == "__main__":
    pipeline()  

    #data_frame_duckdb = read_csv_duckdb(files_list)
    #etl(data_frame_duckdb)
    
    