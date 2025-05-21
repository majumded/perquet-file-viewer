# SQL Server Data Pipeline User Manual

## Overview

The SQL Server Data Pipeline is a tool designed to extract data from SQL Server databases and save it as Parquet files. This manual explains how to set up, configure, and run the pipeline effectively.

## Table of Contents

1. [Features](#features)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Configuration](#configuration)
5. [Running the Pipeline](#running-the-pipeline)
6. [Output Files](#output-files)
7. [Logging](#logging)
8. [Troubleshooting](#troubleshooting)
9. [Advanced Usage](#advanced-usage)

## Features

- Connect to SQL Server databases using Windows authentication
- Read custom SQL queries from files
- Process data in batches to handle large datasets efficiently
- Save data in Parquet format with configurable compression
- Comprehensive logging of all operations

## Prerequisites

- Python 3.6 or higher
- SQL Server database (local or remote)
- SQL Server ODBC Driver 17 (or compatible)
- Required Python packages (listed in [Installation](#installation))

## Installation

1. Clone or download the data pipeline script to your local machine.

2. Install the required Python packages:

```bash
pip install pyodbc pandas pyarrow configparser
```

3. Ensure that SQL Server ODBC Driver 17 is installed on your system.

## Configuration

The pipeline uses a configuration file (`config.ini`) to control its behavior. If this file doesn't exist, the pipeline will create a default one.

### Default Configuration

```ini
[Database]
server = localhost\SQLEXPRESS
database = master

[Query]
sql_file_path = query.sql

[Processing]
batch_size = 1000

[Output]
extract_name = DataExtract
output_directory = output
compression = snappy
row_group_size = 10000
```

### Configuration Settings

#### Database Section

- `server`: SQL Server instance name (default: `localhost\SQLEXPRESS`)
- `database`: Database name to connect to (default: `master`)

#### Query Section

- `sql_file_path`: Path to the SQL file containing the query to execute (default: `query.sql`)

#### Processing Section

- `batch_size`: Number of records to process in each batch (default: `1000`)

#### Output Section

- `extract_name`: Prefix for output file names (default: `DataExtract`)
- `output_directory`: Directory where output files will be saved (default: `output`)
- `compression`: Parquet compression algorithm (default: `snappy`)
- `row_group_size`: Number of rows in each Parquet row group (default: `10000`)

## SQL Query File

Create a file named `query.sql` (or as specified in your configuration) containing the SQL query you want to execute. For example:

```sql
SELECT * FROM Customers
WHERE Region = 'North'
ORDER BY CustomerID
```

## Running the Pipeline

To run the data pipeline:

1. Ensure your configuration file is set up correctly.
2. Make sure your SQL query file exists and contains a valid query.
3. Execute the script:

```bash
python data_pipeline.py
```

The pipeline will:
1. Connect to the SQL Server database
2. Execute the SQL query
3. Process the results in batches
4. Save each batch as a separate Parquet file
5. Log all operations

## Output Files

Output files are saved in the specified output directory with the following naming convention:

```
{extract_name}_{timestamp}_{batch_sequence}.parquet
```

For example: `DataExtract_20250521_123045_0001.parquet`

## Logging

The pipeline logs all operations to both the console and a log file. Log files are saved in the `logs` directory with the following naming convention:

```
data_pipeline_{extract_name}_{timestamp}.log
```

For example: `data_pipeline_DataExtract_20250521_123045.log`

Logs include:
- Timestamp
- Batch sequence number
- Log level
- Message

Example log entry:
```
2025-05-21 12:30:45,123 - Batch 1 - INFO - Fetched 1000 rows in this batch.
```

## Troubleshooting

### Common Issues

#### Connection Errors

- **Error:** `[IM002] [Microsoft][ODBC Driver Manager] Data source name not found and no default driver specified`
  - **Solution:** Ensure the ODBC Driver 17 for SQL Server is installed, or modify the connection string to use your installed driver.

- **Error:** `[HYT00] [Microsoft][ODBC Driver 17 for SQL Server]Login timeout expired`
  - **Solution:** Check that the SQL Server is running and accessible from your machine.

#### File Not Found Errors

- **Error:** `SQL file not found: query.sql`
  - **Solution:** Create the SQL query file or update the `sql_file_path` in your configuration.

#### Permission Errors

- **Error:** `Error creating output directory`
  - **Solution:** Ensure you have write permissions to the specified output directory.

### Recovery

If the pipeline encounters an error, it will:
1. Log the error with details
2. Attempt to close any open database connections
3. Exit gracefully

Check the log files in the `logs` directory for detailed error information.

## Advanced Usage

### Customizing the Parquet Output

Parquet files can be optimized by adjusting the following settings:

- **Compression**: The `compression` setting in the configuration file can be set to:
  - `snappy` (default): Good balance of speed and compression
  - `gzip`: Better compression but slower
  - `none`: No compression (fastest, but largest files)

- **Row Group Size**: The `row_group_size` setting controls the number of rows in each Parquet row group. This affects the performance of later queries on the Parquet files.

### Processing Large Datasets

For large datasets:
1. Increase the `batch_size` for better performance if your system has sufficient memory.
2. Ensure your SQL query is optimized to reduce the load on the SQL Server.
3. Consider adding WHERE clauses to break your data extraction into multiple smaller runs.

### Running Multiple Extracts

To run multiple extracts with different configurations:
1. Create multiple configuration files (e.g., `config1.ini`, `config2.ini`).
2. Run the pipeline with each configuration:
   ```bash
   python data_pipeline.py config1.ini
   python data_pipeline.py config2.ini
   ```

Note: The current implementation requires modifying the code to accept command line arguments for multiple configuration files.
