# ETL Pipeline with DuckDB, PostgreSQL, and Streamlit Dashboard

![Pipeline Architecture](pipeline00.png)

This project implements an end-to-end data solution featuring an ETL (Extract, Transform, Load) pipeline that processes CSV files from Google Drive, stores them in a PostgreSQL database hosted on Render.com, and presents the data through an interactive Streamlit dashboard.

## Architecture Overview

The system is built on three core components:
1. **ETL Pipeline**: Automated data processing system
2. **Cloud Database**: PostgreSQL instance on Render.com
3. **Web Dashboard**: Interactive Streamlit interface

Pipeline workflow:
1. Downloads CSV files from Google Drive
2. Processes data using DuckDB for optimal performance
3. Applies business transformations
4. Loads processed data into PostgreSQL
5. Visualizes results via Streamlit

## Technology Stack

- **Python 3.x**: Core programming language
- **DuckDB**: High-performance CSV processing
- **PostgreSQL**: Cloud database (Render.com hosted)
- **Streamlit**: Web interface and visualization
- **pandas**: Data manipulation library
- **SQLAlchemy**: Database ORM
- **gdown**: Google Drive file handling

## Project Structure

### Core Components

```
project/
├── pipeline_00.py     # ETL pipeline implementation
├── app.py            # Streamlit dashboard
├── .env             # Configuration variables
└── requirements.txt  # Project dependencies
```

### Pipeline Functions Detailed

The `pipeline_00.py` implements these key functions:

#### 1. Google Drive Integration
```python
def google_drive_file(url_folder, folder_local):
    """
    Downloads files from Google Drive folder
    Parameters:
        url_folder (str): Google Drive folder URL
        folder_local (str): Local destination path
    """
```

#### 2. File Management
```python
def list_files_csv(folder_local):
    """
    Lists all CSV files in specified folder
    Parameters:
        folder_local (str): Directory to scan
    Returns:
        list: Paths of found CSV files
    """
```

#### 3. Data Processing
```python
def read_csv_duckdb(path_file):
    """
    Loads CSV into DuckDB relation
    Parameters:
        path_file (str): CSV file path
    Returns:
        DuckDB relation: Loaded data
    """
```

#### 4. Database Operations
```python
def save_postgre(df_duckdb, table):
    """
    Persists data to PostgreSQL
    Parameters:
        df_duckdb: DuckDB dataframe
        table (str): Target table name
    """
```

#### 5. Data Transformation
```python
def etl(df):
    """
    Executes transformation logic
    - Computes total sales
    - Implements business rules
    Returns:
        DataFrame: Processed data
    """
```

## Setup Guide

### 1. PostgreSQL Setup on Render.com
1. Register on Render.com
2. Create new PostgreSQL instance
3. Save connection details
4. Configure environment variables

### 2. Development Environment
```bash
pip install -r requirements.txt
```

### 3. Environment Setup
Create `.env` file:
```
DATABASE_URL=postgresql://user:password@render.com:5432/db_name
GOOGLE_DRIVE_FOLDER=your_folder_url
```

## Running the System

1. Execute ETL Pipeline:
```bash
python pipeline_00.py
```

2. Launch Dashboard:
```bash
streamlit run app.py
```

## Dashboard Features

The Streamlit interface provides:
- Dynamic data visualization
- Custom filtering options
- Key performance indicators
- Data export functionality

## Cloud Infrastructure

### Render.com PostgreSQL Benefits
- Automated maintenance
- Built-in backups
- SSL security
- Performance monitoring
- Scalable resources

## Security Implementation

- Environment-based credentials
- Encrypted connections
- Role-based access control
- Regular security patches

## Monitoring & Maintenance

- Database performance metrics
- Usage analytics
- Error tracking
- Backup management

## Best Practices

- Configure appropriate Drive permissions
- Monitor database usage
- Update dependencies regularly
- Implement automated testing
- Maintain backup strategy

## Roadmap

- Enhanced analytics features
- Automated testing suite
- Performance optimizations
- Advanced data analysis
- REST API implementation

## Contributing

1. Fork the repository
2. Create feature branch
3. Commit changes
4. Push to branch
5. Create Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

