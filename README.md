# ETL Pipeline with DuckDB and PostgreSQL

This project implements an ETL (Extract, Transform, Load) pipeline that processes CSV files from Google Drive and loads them into a PostgreSQL database hosted on Render.com.

## Overview

The pipeline performs the following steps:
1. Downloads CSV files from Google Drive
2. Processes them using DuckDB for efficient data manipulation
3. Transforms the data by calculating sales totals
4. Loads the results into a PostgreSQL database

## Technologies Used

- Python 3.x
- DuckDB - For efficient CSV processing and data transformation
- PostgreSQL - As the target database (hosted on Render.com)
- pandas - For data manipulation
- SQLAlchemy - For database connectivity
- gdown - For downloading files from Google Drive

## Project Structure

The main script `pipeline_00.py` contains several key functions:

### Key Functions

- `google_drive_file(url_folder, folder_local)`: Downloads files from a Google Drive folder
- `list_files_csv(folder_local)`: Lists all CSV files in the specified local folder
- `read_csv_duckdb(path_file)`: Reads CSV files into DuckDB relations
- `save_postgre(df_duckdb, table)`: Saves transformed data to PostgreSQL
- `etl(df)`: Performs the transformation logic, calculating total sales

### Database Configuration

The project uses environment variables for database configuration. The PostgreSQL connection is established through Render.com's cloud hosting service.

## Setup and Installation

1. Clone the repository
2. Install required dependencies:
```bash
pip install pandas duckdb gdown sqlalchemy python-dotenv
```

3. Create a `.env` file with your PostgreSQL connection string:
```
DATABASE_URL=postgresql://user:password@render.com:5432/database_name
```

## Usage

1. Set your Google Drive folder URL containing the CSV files
2. Run the pipeline:
```bash
python pipeline_00.py
```

## Data Transformation

The ETL process includes a calculation of total sales by multiplying quantity (`quantidade`) by value (`valor`) for each record. The transformed data is stored in the `vendas_calculado` table in PostgreSQL.

## Cloud Database

This project uses a PostgreSQL database hosted on Render.com, providing:
- Scalable cloud storage
- Reliable database access
- Secure connection through environment variables
- Easy integration with other cloud services

## Notes

- Make sure you have proper access rights to the Google Drive folder
- The PostgreSQL database credentials should be kept secure using environment variables
- The pipeline can handle multiple CSV files in a single execution

## Contributing

Feel free to submit issues and enhancement requests!

## License

This project is licensed under the MIT License - see the LICENSE file for details.
