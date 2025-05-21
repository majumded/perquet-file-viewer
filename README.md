# Parquet Data Viewer
## User Manual

Perquet Viewer - UI
![Parquet Data Viewer Logo](https://github.com/majumded/perquet-file-viewer/blob/main/ParquetViewer.GIF)

Perquet Viewer - File Picker
![Parquet Data Viewer FilePicker](https://github.com/majumded/perquet-file-viewer/blob/main/ParquetViewer_FilePick.GIF)

Perquet Viewer - Data Grid
![Parquet Data Viewer Grid](https://github.com/majumded/perquet-file-viewer/blob/main/ParquetViewer_DataGrid.GIF)

Perquet Viewer - Record Preview
![Parquet Data Viewer Record Preview ](https://github.com/majumded/perquet-file-viewer/blob/main/ParquetViewer_RecordPreview.GIF)

Perquet Viewer - Down Load Parquet File As CSV
![Parquet Data Viewer Download Prarquet data as CSV file ](https://github.com/majumded/perquet-file-viewer/blob/main/ParquetViewer_DownLoad.GIF)

**Version 1.0**  
*May 2025*

---

## Table of Contents
1. [Introduction](#introduction)
2. [Getting Started](#getting-started)
   - [System Requirements](#system-requirements)
   - [Installation](#installation)
   - [Launching the Application](#launching-the-application)
3. [User Interface Overview](#user-interface-overview)
   - [Main Window](#main-window)
   - [Header Section](#header-section)
   - [Data Table](#data-table)
   - [Status Bar](#status-bar)
4. [Basic Operations](#basic-operations)
   - [Opening a Parquet File](#opening-a-parquet-file)
   - [Viewing Data](#viewing-data)
   - [Refreshing Data](#refreshing-data)
   - [Viewing File Information](#viewing-file-information)
5. [Working with Records](#working-with-records)
   - [Viewing Record Details](#viewing-record-details)
   - [Navigating the Data Table](#navigating-the-data-table)
6. [Data Export](#data-export)
   - [Exporting to CSV](#exporting-to-csv)
7. [Configuration](#configuration)
   - [Config.properties File](#configproperties-file)
   - [Verbose Mode](#verbose-mode)
8. [Troubleshooting](#troubleshooting)
   - [Common Issues](#common-issues)
   - [Error Messages](#error-messages)
9. [Contact & Support](#contact--support)

---

## Introduction

Parquet Data Viewer is a lightweight desktop application designed to help users easily view, explore, and export data stored in Apache Parquet files. With a simple and intuitive interface, this tool allows users to:

- Quickly open and browse Parquet files
- View detailed information about file metadata
- Examine individual records in detail
- Export Parquet data to CSV format for broader compatibility

This application is ideal for data analysts, engineers, and anyone who works with data stored in the Parquet file format but requires a simple visualization tool without the need for programming or complex data tools.

---

## Getting Started

### System Requirements

- Operating System: Windows, macOS, or Linux
- Python 3.6 or higher (with tkinter support)
- Required Python packages:
  - pandas
  - pyarrow (for Parquet support)

### Installation

1. Ensure you have Python installed on your system
2. Install required packages using pip:
   ```
   pip install pandas pyarrow configparser
   ```
3. Download the Parquet Data Viewer application files
4. Extract files to your preferred location

### Launching the Application

1. Navigate to the installation directory
2. Run the application using Python:
   ```
   python parquet_viewer.py
   ```
3. Alternatively, if an executable is provided for your operating system, double-click the executable file

---

## User Interface Overview

### Main Window

When you first launch the application, you'll see the welcome screen providing a brief overview of the application's features. After closing the welcome screen, you'll see the main application window with the following components:

### Header Section

Located at the top of the window, the header section contains:

- **Application Title**: "Parquet Data Viewer"
- **File Information**: Shows the currently loaded file name
- **Button Controls**:
  - **Select File**: Opens a file dialog to choose a Parquet file
  - **Refresh Data**: Reloads data from the current file
  - **Download**: Exports the current data to CSV format
  - **File Info**: Shows metadata about the currently loaded Parquet file

### Data Table

The central area of the application contains a table view with the following features:

- **Column Headers**: Click to sort data (if supported)
- **Horizontal and Vertical Scrollbars**: For navigating large datasets
- **Adjustable Column Widths**: Automatically sized to fit data

### Status Bar

Located at the bottom of the window, the status bar provides:

- **Status Messages**: Current application status (Ready, Loading, etc.)
- **Progress Bar**: Appears when loading large files
- **Record Count**: Displays the number of records in the loaded file

---

## Basic Operations

### Opening a Parquet File

1. Click the **Select File** button in the header section
2. In the file dialog that appears, navigate to the location of your Parquet file
3. Select the desired .parquet file and click "Open"
4. The application will load and display the data

**Note**: The application remembers the last directory you accessed, making it easier to navigate to commonly used locations.

### Viewing Data

Once a file is loaded, the data table will display the contents with the following features:

- Each row represents a record in the Parquet file
- Each column represents a field in the data
- Column widths automatically adjust to accommodate the data
- Use the scroll bars to navigate through large datasets

### Refreshing Data

If the Parquet file has been updated externally while the application is running:

1. Click the **Refresh Data** button in the header section
2. The application will reload the current file and update the display

### Viewing File Information

To view detailed information about the loaded Parquet file:

1. Click the **File Info** button in the header section
2. A dialog will appear showing:
   - Full file path
   - File size in MB
   - Last modified date and time
   - Verbose mode setting (if defined in config.properties)

---

## Working with Records

### Viewing Record Details

To examine a specific record in detail:

1. Double-click on any row in the data table or select a row and press Enter
2. A "Record Details" window will appear showing:
   - Each field name and its corresponding value
   - Values are displayed with proper formatting and wrapping for readability
3. Use the scrollbar to navigate through all fields if needed
4. Click the **Close** button to return to the main view

### Navigating the Data Table

- Use the **vertical scrollbar** to move through records (rows)
- Use the **horizontal scrollbar** to view additional fields (columns)
- Use keyboard navigation (arrow keys) to move between cells

---

## Data Export

### Exporting to CSV

To export the currently loaded Parquet data to CSV format:

1. Click the **Download** button in the header section
2. The application will:
   - Create a CSV file with the same name as the Parquet file in the same directory
   - Display a progress indicator during the export process
   - Show a confirmation message when the export is complete, including the file path
3. If a file with the same name already exists, you will be prompted to confirm overwriting

**Note**: The exported CSV file will contain all data from the Parquet file with the same structure and field names.

---

## Configuration

### Config.properties File

The application uses a configuration file named `config.properties` to store settings and file paths. This file is automatically created or updated when you:

- Select a Parquet file through the interface
- Change application settings

The configuration file is located in the same directory as the application and contains:

```
[FILE_PATHS]
parquet_file_path = /path/to/your/last/used/file.parquet

[OPTIONS]
verbose = true
```

### Verbose Mode

The "verbose" setting in the config.properties file controls the level of console output:

- **true**: The application will print additional information to the console, including file paths and record counts
- **false**: The application will minimize console output

To change this setting:
1. Locate the config.properties file in the application directory
2. Open it with a text editor
3. Change the verbose value to either "true" or "false"
4. Save the file and restart the application for the changes to take effect

---

## Troubleshooting

### Common Issues

**Application fails to start:**
- Ensure Python is properly installed
- Verify that all required packages are installed
- Check console output for specific error messages

**Cannot open Parquet files:**
- Verify that the file has a .parquet extension
- Check that the file is not corrupted or empty
- Ensure you have read permissions for the file

**Data table is empty after loading a file:**
- The Parquet file might be empty
- The file might not be in proper Parquet format
- Check the record count in the status bar

### Error Messages

**"Parquet file not found":**
- The specified file no longer exists at the given location
- The file path in config.properties might be outdated

**"Could not get file information":**
- The file might be locked by another process
- You might not have permission to access the file

**"Failed to update config file":**
- The application directory might be write-protected
- The config.properties file might be locked by another process

---

## Contact & Support

For questions, bug reports, or feature requests, please contact:

**Email**: debal-prasad.majumder@dxc.com

When reporting issues, please include:
- Application version
- Operating system details
- Steps to reproduce the issue
- Any error messages displayed

---

# Parquet Data Pipeline User Manual

## Overview

![Parquet Data Pipeline Flow ](https://github.com/majumded/perquet-file-viewer/blob/main/Parquet%20Data%20Pipeline%20Flow.GIF)

The Parquet Data Pipeline is a tool designed to extract data from SQL Server databases and save it as Parquet files. This manual explains how to set up, configure, and run the pipeline effectively.

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


*© 2025 DXC Technology. All rights reserved.*
