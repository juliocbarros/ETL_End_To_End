# ETL Pipeline with DuckDB and PostgreSQL

This project implements an ETL (Extract, Transform, Load) pipeline that processes CSV files stored in Google Drive, performs transformations using DuckDB, and saves the results to a PostgreSQL database.

## Features

The pipeline consists of several functions that perform different stages of the ETL process:

### 1. File Download (`google_drive_file`)
- Downloads files from a specific Google Drive folder
- Stores files in a local folder
- Uses the `gdown` library for downloading

### 2. File Listing (`list_files_csv`)
- Lists all CSV files in the local folder
- Returns a list with complete file paths

### 3. CSV Reading with DuckDB (`read_csv_duckdb`)
- Reads CSV files using DuckDB
- Returns a DuckDB DataFrame for processing

### 4. Data Transformation (`etl`)
- Performs data transformations using SQL via DuckDB
- Adds a new column `total_vendas` calculated as `quantidade * valor` (quantity * value)
- Returns a Pandas DataFrame with transformed data

### 5. PostgreSQL Saving (`save_postgre`)
- Saves transformed data to a PostgreSQL table
- Uses SQLAlchemy for database connection
- Allows appending new data to existing table

## Requirements

- Python 3.x
- Required libraries:
  - pandas
  - duckdb
  - gdown
  - sqlalchemy
  - python-dotenv

## Setup

1. Create a `.env` file in the project root with the following variable:
```
DATABASE_URL=your_postgresql_connection_url
```

2. Install dependencies:
```
pip install pandas duckdb gdown sqlalchemy python-dotenv
```

## Usage

The main script (`pipeline_00.py`) can be executed directly:

```
python pipeline_00.py
```

The pipeline will:
1. Download CSV files from Google Drive (if not commented out)
2. List all CSV files in the local folder
3. Process each file, performing necessary transformations
4. Save results to PostgreSQL

## Project Structure

```
.
├── pipeline_00.py     # Main script
├── .env              # Configuration file (not versioned)
└── folder_gdown/     # Folder for downloaded files
```

## Important Notes

- The code includes handling for multiple CSV files
- The Google Drive download function is commented out by default
- Data is processed in batches, file by file
- The PostgreSQL table is called "vendas_calculado" (calculated_sales)

## Possible Improvements

1. Add more robust error handling
2. Implement logging
3. Add data validation
4. Create unit tests
5. Add more detailed function documentation

## Contributing

Feel free to contribute to the project through Pull Requests or by reporting issues.

