"""
SQL Server Data Pipeline

This script implements a data pipeline that:
1. Connects to a local SQL Server Express database using Windows authentication
2. Reads a predefined SQL query from a file
3. Extracts data in batches
4. Writes data to Parquet files with appropriate formatting
5. Logs all operations, with log filenames including the extract_name from config.
"""

import os
import sys
import time
import logging
import datetime
import configparser
import pyodbc
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List, Dict, Any, Optional, Tuple


class DataPipeline:
    """Data pipeline for extracting data from SQL Server and saving as Parquet files."""

    def __init__(self, config_path: str = "config.ini"):
        """Initialize the data pipeline with the given configuration file.

        Args:
            config_path: Path to the config file, defaults to 'config.ini' in the current directory
        """
        self.config_path = config_path
        self.batch_sequence = 0
        # Load config first, so it's available for logging setup
        self.config = self._load_config()
        # Now setup logging, which can use self.config
        self._setup_logging()

    def _load_config(self) -> configparser.ConfigParser:
        """Load configuration from the config file."""
        try:
            if not os.path.exists(self.config_path):
                # Use print for initial logging as logger might not be set up
                print(f"Config file not found: {self.config_path}")
                print("Creating default configuration")

                # Create default config
                config = configparser.ConfigParser()

                # Database section
                config['Database'] = {
                    'server': 'localhost\\SQLEXPRESS',
                    'database': 'master'
                }

                # Query section
                config['Query'] = {
                    'sql_file_path': 'query.sql'
                }

                # Processing section
                config['Processing'] = {
                    'batch_size': '1000'
                }

                # Output section - Modified for Parquet
                config['Output'] = {
                    'extract_name': 'DataExtract',
                    'output_directory': 'output',
                    'compression': 'snappy',
                    'row_group_size': '10000'
                }

                # Write default config to file
                try:
                    with open(self.config_path, 'w') as configfile:
                        config.write(configfile)
                    print(f"Created default configuration file: {self.config_path}")
                except Exception as e:
                    print(f"Error creating default config file: {str(e)}")

                return config

            config = configparser.ConfigParser()
            config.read(self.config_path)
            # Use print for initial logging if logger isn't setup yet, or self._log if it is (depends on call order)
            # Assuming this is called before _setup_logging, print is safer for this specific message.
            print(f"Successfully loaded configuration from {self.config_path}")

            # Validate config has required sections
            required_sections = ['Database', 'Query', 'Processing', 'Output']
            missing_sections = [section for section in required_sections if section not in config]

            if missing_sections:
                print(f"Warning: Missing config sections: {', '.join(missing_sections)}")

                # Add default values for missing sections
                if 'Database' not in config:
                    config['Database'] = {'server': 'localhost\\SQLEXPRESS', 'database': 'master'}
                    print("Added default Database section")

                if 'Query' not in config:
                    config['Query'] = {'sql_file_path': 'query.sql'}
                    print("Added default Query section")

                if 'Processing' not in config:
                    config['Processing'] = {'batch_size': '1000'}
                    print("Added default Processing section")

                if 'Output' not in config:
                    # Modified for Parquet
                    config['Output'] = {
                        'extract_name': 'DataExtract',
                        'output_directory': 'output',
                        'compression': 'snappy',
                        'row_group_size': '10000'
                    }
                    print("Added default Output section")
                
                # Add Parquet settings if using old config file format
                elif 'Output' in config and 'field_delimiter' in config['Output']:
                    # Convert CSV settings to Parquet settings
                    config['Output']['compression'] = 'snappy'
                    config['Output']['row_group_size'] = '10000'
                    print("Added Parquet settings to existing Output section")

            return config
        except Exception as e:
            print(f"Error loading config file: {str(e)}")
            print(f"Exception type: {type(e).__name__}")
            print(f"Exception details: {repr(e)}")

            # Create a minimal default config in case of failure
            default_config = configparser.ConfigParser()
            default_config['Database'] = {'server': 'localhost\\SQLEXPRESS', 'database': 'master'}
            default_config['Query'] = {'sql_file_path': 'query.sql'}
            default_config['Processing'] = {'batch_size': '1000'}
            # Modified for Parquet
            default_config['Output'] = {
                'extract_name': 'DataExtract',
                'output_directory': 'output',
                'compression': 'snappy',
                'row_group_size': '10000'
            }

            print("Using default configuration due to error")
            return default_config

    def _setup_logging(self):
        """Set up logging configuration.
        Log filename will include extract_name from the config.
        """
        try:
            # Get extract_name from config, with a fallback
            # Ensure 'Output' section exists, then get 'extract_name'
            extract_name = "DefaultExtractName" # Fallback
            if 'Output' in self.config and 'extract_name' in self.config['Output']:
                extract_name = self.config['Output'].get('extract_name', 'DefaultExtractName')
            elif hasattr(self, '_log'): # If _log is available, use it for warning
                 self._log("Warning: 'extract_name' not found in config [Output] section for logging. Using fallback.")
            else: # If logger not set up, print warning
                 print("Warning: 'extract_name' not found in config [Output] section for logging. Using fallback.")


            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            # Append extract_name to the log filename
            log_filename = f"data_pipeline_{extract_name}_{timestamp}.log"
            log_dir = "logs"

            # Create logs directory if it doesn't exist
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)

            log_path = os.path.join(log_dir, log_filename)

            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - Batch %(batch_sequence)s - %(levelname)s - %(message)s',
                filename=log_path,
                filemode='w'
            )

            # Add a stream handler to also log to console
            console = logging.StreamHandler()
            console.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s - Batch %(batch_sequence)s - %(levelname)s - %(message)s')
            console.setFormatter(formatter)
            logging.getLogger('').addHandler(console)

            self.logger = logging.getLogger('')
            self._log(f"Logging initialized. Log file: {log_path}")
        except Exception as e:
            # Fallback to print if logging setup itself fails critically
            print(f"Critical error setting up logging: {str(e)}")
            print(f"Exception type: {type(e).__name__}")
            print(f"Exception details: {repr(e)}")
            # Optionally, re-raise or exit if logging is absolutely critical
            # For now, we'll let the application continue if possible, but logging will be impaired.
            # To make it stop, uncomment: raise

    def _log(self, message: str, level: str = "info"):
        """Log a message with the current batch sequence.
        Args:
            message: The message to log.
            level: The logging level ('info', 'warning', 'error', 'critical', 'debug').
        """
        try:
            log_extra = {'batch_sequence': self.batch_sequence}
            if level == "info":
                self.logger.info(message, extra=log_extra)
            elif level == "warning":
                self.logger.warning(message, extra=log_extra)
            elif level == "error":
                self.logger.error(message, extra=log_extra)
            elif level == "critical":
                self.logger.critical(message, extra=log_extra)
            elif level == "debug":
                self.logger.debug(message, extra=log_extra)
            else:
                self.logger.info(f"(Unknown level: {level}) {message}", extra=log_extra)
        except AttributeError:
            # Logger might not be initialized yet (e.g., during early __init__)
            print(f"{datetime.datetime.now()} - Batch {self.batch_sequence} - {level.upper()} - {message}")
        except Exception as e:
            # Fallback to print if logging fails for other reasons
            print(f"Logging error: {str(e)}")
            print(f"{datetime.datetime.now()} - Batch {self.batch_sequence} - {level.upper()} - {message}")


    def _get_connection_string(self) -> str:
        """Build database connection string from config."""
        try:
            server = self.config['Database'].get('server', 'localhost\\SQLEXPRESS')
            database = self.config['Database'].get('database', 'master')
        except KeyError:
            self._log("Database section or keys missing in config. Using defaults.", level="warning")
            server = 'localhost\\SQLEXPRESS'
            database = 'master'

        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
        self._log(f"Created connection string for database: {database} on server: {server}")
        return conn_str

    def _read_sql_file(self) -> str:
        """Read SQL query from file specified in config."""
        try:
            sql_file_path = self.config['Query'].get('sql_file_path', 'query.sql')
        except KeyError:
            self._log("Query section or sql_file_path missing in config. Using default 'query.sql'.", level="warning")
            sql_file_path = 'query.sql'

        if not os.path.exists(sql_file_path):
            self._log(f"SQL file not found: {sql_file_path}", level="error")
            raise FileNotFoundError(f"SQL file not found: {sql_file_path}")

        try:
            with open(sql_file_path, 'r') as f:
                sql_query = f.read()
            self._log(f"Successfully read SQL query from {sql_file_path}")
            return sql_query
        except Exception as e:
            self._log(f"Error reading SQL file {sql_file_path}: {str(e)}", level="error")
            raise

    def _get_output_filename(self) -> str:
        """Generate output filename based on configuration and batch sequence."""
        try:
            extract_name = self.config['Output'].get('extract_name', 'DataExtract')
            output_dir = self.config['Output'].get('output_directory', 'output')
        except KeyError:
            self._log("Output section or keys missing in config. Using defaults.", level="warning")
            extract_name = 'DataExtract'
            output_dir = 'output'

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

        if not os.path.exists(output_dir):
            try:
                os.makedirs(output_dir)
                self._log(f"Created output directory: {output_dir}")
            except Exception as e:
                self._log(f"Error creating output directory {output_dir}: {str(e)}", level="error")
                # Fallback to current directory if creation fails
                output_dir = "." 

        # Changed extension from .csv to .parquet
        filename = f"{extract_name}_{timestamp}_{self.batch_sequence:04d}.parquet"
        full_path = os.path.join(output_dir, filename)
        self._log(f"Generated output filename: {full_path}")
        return full_path

    def _write_batch_to_parquet(self, data: List[Dict[str, Any]], output_file: str):
        """Write a batch of data to a Parquet file."""
        try:
            # Get Parquet-specific settings from config
            compression = self.config['Output'].get('compression', 'snappy')
            try:
                row_group_size = int(self.config['Output'].get('row_group_size', '10000'))
            except ValueError:
                self._log("Invalid row_group_size in config, using default 10000", level="warning")
                row_group_size = 10000
        except KeyError:
            self._log("Output config for Parquet writing missing. Using defaults.", level="warning")
            compression = 'snappy'
            row_group_size = 10000
        
        self._log(f"Writing {len(data)} records to {output_file} with compression={compression}, row_group_size={row_group_size}")
        
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            try:
                os.makedirs(output_dir)
                self._log(f"Created output directory (redundant check): {output_dir}")
            except Exception as e:
                 self._log(f"Error creating output directory {output_dir} during write: {str(e)}", level="error")

        try:
            # Convert list of dicts to pandas DataFrame
            df = pd.DataFrame(data)
            
            # Convert to PyArrow Table
            table = pa.Table.from_pandas(df)
            
            # Write to Parquet file
            pq.write_table(
                table,
                output_file,
                compression=compression,
                row_group_size=row_group_size
            )
            
            self._log(f"Successfully wrote data to Parquet file {output_file}")
        except Exception as e:
            self._log(f"Error writing batch to Parquet {output_file}: {str(e)}", level="error")
            raise

    def _fetch_batch(self, cursor: pyodbc.Cursor, batch_size: int) -> Tuple[List[Dict[str, Any]], List[str], bool]:
        """Fetch a batch of records from the database."""
        try:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                self._log("No more rows to fetch from database.")
                return [], [], True  # No more data

            column_names = [column[0] for column in cursor.description]
            # self._log(f"Retrieved column names: {', '.join(column_names)}") # Can be verbose

            data = [dict(zip(column_names, row)) for row in rows]
            
            self._log(f"Fetched {len(data)} rows in this batch.")
            return data, column_names, False
        except pyodbc.Error as e:
            self._log(f"Database error while fetching batch: {str(e)}", level="error")
            raise
        except Exception as e:
            self._log(f"Generic error processing batch: {str(e)}", level="error")
            raise

    def run(self):
        """Run the data pipeline."""
        self._log("Starting data pipeline run...")
        conn: Optional[pyodbc.Connection] = None
        cursor: Optional[pyodbc.Cursor] = None

        try:
            try:
                batch_size_str = self.config['Processing'].get('batch_size', '1000')
                batch_size = int(batch_size_str)
                if batch_size <= 0:
                    self._log(f"Invalid batch_size '{batch_size_str}', must be > 0. Using default 1000.", level="warning")
                    batch_size = 1000
            except (KeyError, ValueError) as e:
                self._log(f"Error reading batch_size from config ('{str(e)}'). Using default 1000.", level="warning")
                batch_size = 1000
            self._log(f"Using batch size: {batch_size}")

            conn_str = self._get_connection_string()
            self._log(f"Attempting to connect to database...")
            conn = pyodbc.connect(conn_str)
            self._log("Database connection established.")
            
            sql_query = self._read_sql_file()
            # self._log(f"Executing SQL query: {sql_query[:200]}...") # Log snippet of query

            cursor = conn.cursor()
            cursor.execute(sql_query)
            self._log("SQL query executed.")

            end_of_data = False
            total_records_processed = 0
            self.batch_sequence = 0 # Reset for each run

            while not end_of_data:
                self.batch_sequence += 1 # Increment first, so batch 1 is logged as batch 1
                self._log(f"Processing batch {self.batch_sequence}...")
                
                data, column_names, end_of_data = self._fetch_batch(cursor, batch_size)

                if not data and end_of_data:
                    self._log(f"No data in batch {self.batch_sequence}, and end of data reached.")
                    break 
                
                if not data: # Should be caught by the above, but as a safeguard
                    self._log(f"No data returned for batch {self.batch_sequence}, but not marked as end_of_data. Stopping.", level="warning")
                    break

                total_records_processed += len(data)
                
                output_file = self._get_output_filename() # Uses self.batch_sequence
                self._write_batch_to_parquet(data, output_file)
            
            self._log(f"Data pipeline run completed. Total records processed: {total_records_processed}.")

        except FileNotFoundError as e:
            self._log(f"Configuration or SQL file not found: {str(e)}", level="critical")
        except pyodbc.Error as e:
            self._log(f"SQL Database error during pipeline execution: {str(e)}", level="critical")
            # Log more details if available, e.g., SQLSTATE
            sql_state = getattr(e, 'args', [None])[0] if hasattr(e, 'args') and e.args else "N/A"
            self._log(f"SQLSTATE: {sql_state}", level="critical")
        except configparser.Error as e:
            self._log(f"Configuration file error: {str(e)}", level="critical")
        except Exception as e:
            self._log(f"An unexpected critical error occurred in the data pipeline: {str(e)}", level="critical")
            self._log(f"Exception type: {type(e).__name__}", level="critical")
            self._log(f"Exception details: {repr(e)}", level="critical")
        finally:
            if cursor:
                try:
                    cursor.close()
                    self._log("Database cursor closed.")
                except pyodbc.Error as e:
                    self._log(f"Error closing cursor: {str(e)}", level="warning")
            if conn:
                try:
                    conn.close()
                    self._log("Database connection closed.")
                except pyodbc.Error as e:
                    self._log(f"Error closing connection: {str(e)}", level="warning")
            self._log("Data pipeline execution finished.")


def main():
    """Main entry point."""
    print("Application starting...")
    try:
        config_path = "config.ini"
        
        # No need to check for config_path existence here, DataPipeline constructor handles it
        # and creates a default one if not found.
        
        pipeline = DataPipeline(config_path=config_path) # Pass explicitly
        pipeline.run()
        print("Application finished successfully.")
        
    except Exception as e:
        # This will catch exceptions raised from DataPipeline.run() if they weren't handled internally
        # or if DataPipeline initialization failed before logging was set up.
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error_message = f"[{current_time}] Main application critical error: {str(e)}\n"
        error_message += f"Exception type: {type(e).__name__}\n"
        error_message += f"Exception details: {repr(e)}\n"
        
        print(error_message) # Print to console
        
        # Try to write to a fallback error log if possible
        try:
            log_dir = "logs"
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            fallback_log_path = os.path.join(log_dir, "critical_error.log")
            with open(fallback_log_path, "a") as f_err:
                f_err.write(error_message)
            print(f"Critical error details also logged to: {fallback_log_path}")
        except Exception as log_e:
            print(f"Additionally, failed to write to fallback error log: {str(log_e)}")
            
        sys.exit(1)


if __name__ == "__main__":
    main()
