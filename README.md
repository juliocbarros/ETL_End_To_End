# ETL Pipeline with DuckDB, PostgreSQL, and Streamlit Dashboard

This project implements a complete data solution with an ETL (Extract, Transform, Load) pipeline that processes CSV files from Google Drive, loads them into a PostgreSQL database hosted on Render.com, and displays the results through a Streamlit dashboard.

## Overview

The system consists of three main components:
1. ETL Pipeline: Processes sales data from CSV files
2. Cloud Database: PostgreSQL hosted on Render.com
3. Web Interface: Interactive dashboard built with Streamlit

The pipeline performs the following steps:
1. Downloads CSV files from Google Drive
2. Processes them using DuckDB for efficient data manipulation
3. Transforms the data by calculating sales totals
4. Loads the results into PostgreSQL
5. Displays processed data through Streamlit dashboard

## Technologies Used

- Python 3.x
- DuckDB - For efficient CSV processing
- PostgreSQL - Cloud database (Render.com)
- Streamlit - Web interface and dashboard
- pandas - Data manipulation
- SQLAlchemy - Database connectivity
- gdown - Google Drive integration

## Project Structure

### Main Components

1. `pipeline_00.py` - ETL pipeline implementation
2. `app.py` - Streamlit dashboard
3. `.env` - Environment configuration

### Pipeline Functions Explained

`pipeline_00.py` contains the following key functions:

```python
def google_drive_file(url_folder, folder_local):
    """
    Downloads files from Google Drive folder
    - url_folder: Google Drive folder URL
    - folder_local: Local destination folder
    """

def list_files_csv(folder_local):
    """
    Lists all CSV files in specified folder
    - folder_local: Path to search for CSV files
    Returns: List of CSV file paths
    """

def read_csv_duckdb(path_file):
    """
    Reads CSV into DuckDB relation
    - path_file: Path to CSV file
    Returns: DuckDB relation
    """

def save_postgre(df_duckdb, table):
    """
    Saves data to PostgreSQL
    - df_duckdb: DuckDB dataframe
    - table: Target table name
    """

def etl(df):
    """
    Performs transformation logic
    - Calculates total sales (quantidade * valor)
    - Applies business rules
    Returns: Transformed dataframe
    """
```

## Setup Instructions

### 1. Database Setup on Render.com
1. Create account on Render.com
2. Create new PostgreSQL database
3. Note down connection credentials
4. Set up environment variables

### 2. Local Installation
```bash
pip install pandas duckdb gdown sqlalchemy python-dotenv streamlit
```

### 3. Environment Configuration
Create `.env` file:
```
DATABASE_URL=postgresql://user:password@render.com:5432/database_name
GOOGLE_DRIVE_FOLDER=your_folder_url
```

## Running the Application

### 1. ETL Pipeline
```bash
python pipeline_00.py
```

### 2. Streamlit Dashboard
```bash
streamlit run app.py
```

## Streamlit Dashboard Features

The dashboard (`app.py`) provides:
- Real-time sales data visualization
- Interactive filters and charts
- Sales performance metrics
- Data export capabilities

## Cloud Infrastructure

### PostgreSQL on Render.com
- Automated backups
- Scalable storage
- Secure SSL connections
- Built-in monitoring

### Benefits
- Zero maintenance
- Automatic updates
- High availability
- Cost-effective scaling

## Security Considerations

- Database credentials stored in environment variables
- Secure HTTPS connections
- Access control through Render.com
- Regular security updates

## Monitoring and Maintenance

- Render.com dashboard for database monitoring
- Streamlit analytics for usage tracking
- Automated error logging
- Regular backup verification

## Notes

- Ensure Google Drive folder permissions are set correctly
- Monitor database usage on Render.com
- Keep dependencies updated
- Regular testing of ETL pipeline
- Backup important data regularly

## Future Enhancements

- Additional dashboard features
- Automated testing
- Performance optimization
- Extended data analytics
- API integration capabilities

