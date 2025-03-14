import os
import pandas as pd
import duckdb
import gdown
from sqlalchemy import create_engine
from dotenv import load_dotenv
from duckdb import DuckDBPyRelation
from pandas import DataFrame

class DataPipeline:
    def __init__(self, url_folder: str, folder_local: str):
        self.url_folder = url_folder
        self.folder_local = folder_local
        self.database_url = os.getenv("DATABASE_URL")
        load_dotenv()

    def google_drive_file(self):
        """Download files from Google Drive"""
        os.makedirs(self.folder_local, exist_ok=True)
        gdown.download_folder(self.url_folder, output=self.folder_local, quiet=False, use_cookies=False)

    def list_files_csv(self) -> list:
        """List CSV files in local folder"""
        file_csv = []
        all_files = os.listdir(self.folder_local)
        for file in all_files:
            if file.endswith(".csv"):
                path = os.path.join(self.folder_local, file)
                file_csv.append(path)
        return file_csv

    def read_csv_duckdb(self, path_file: str) -> DuckDBPyRelation:
        """Read CSV file and return a DuckDB DataFrame"""
        return duckdb.read_csv(path_file)

    def save_postgre(self, df_duckdb: DataFrame, table: str):
        """Save DataFrame to PostgreSQL"""
        engine = create_engine(self.database_url)
        df_duckdb.to_sql(table, con=engine, if_exists='append', index=False)

    def etl(self, df: DuckDBPyRelation) -> DataFrame:
        """Transform data by adding total_vendas column"""
        df_etl = duckdb.sql("SELECT *, quantidade * valor AS total_vendas FROM df").df()
        return df_etl

    def run_pipeline(self):
        """Execute the complete ETL pipeline"""
        # self.google_drive_file()  # Commented as in original
        files_list = self.list_files_csv()

        for path_file in files_list:
            duckdb_df_etl = self.read_csv_duckdb(path_file)
            pandas_df_etl = self.etl(duckdb_df_etl)
            self.save_postgre(pandas_df_etl, "vendas_calculado")


if __name__ == "__main__":
    url_folder = 'https://drive.google.com/drive/folders/1T100DU3JJ6zzHrjgSdvnNY7VIOfEzVuh'
    folder_local = './folder_gdown'
    
    pipeline = DataPipeline(url_folder, folder_local)
    pipeline.run_pipeline()