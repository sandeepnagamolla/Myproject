import os
import sys
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import logging
from datetime import datetime

# Setup logging
LOG_FILE = f"{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.log"
logs_path = os.path.join(os.getcwd(), "logs")
os.makedirs(logs_path, exist_ok=True)

LOG_FILE_PATH = os.path.join(logs_path, LOG_FILE)

logging.basicConfig(
    filename=LOG_FILE_PATH,
    format="[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

class SnowflakeIngestion:
    def __init__(self, user, password, account, warehouse, database, schema):
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.connection = None
        logging.info("Initialized SnowflakeIngestion class.")

    def connect(self):
        "connection to Snowflake."
        try:
            self.connection = snowflake.connector.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema
            )
            logging.info("Connected to Snowflake.")
        except Exception as e:
            logging.error(f"Failed to connect to Snowflake: {e}")
            sys.exit(1)

    def ingest_data(self, data_path, table_name):
        
        if not self.connection:
            logging.error("No active Snowflake connection.")
            return

        try:
            # Read the data file into a DataFrame
            df = pd.read_csv(data_path, sep='|', header=None, skipinitialspace=True)
            logging.info(f"Read data from {data_path} into DataFrame.")
            
            #getting the columns from the table
            cursor = self.connection.cursor()
            cursor.execute(f"Describe Table {self.schema}.{table_name}")
            table_columns = [row[0] for row in cursor.fetchall()]

            if len(table_columns) != df.shape[1]:
                logging.error(f"Column mismatch: {len(table_columns)} columns in table, {df.shape[1]} columns in DataFrame.")
                return
            df.columns = table_columns
            logging.info("columns names of df renamed with table_cloumns")

            # Use write_pandas to write the DataFrame to Snowflake
            success, nchunks, nrows, _ = write_pandas(self.connection, df, table_name)
            if success:
                logging.info(f"Successfully ingested {nrows} rows into {table_name}.")
            else:
                logging.error("Failed to ingest data into Snowflake.")

        except Exception as e:
            logging.error(f"Error ingesting data: {e}")
        finally:
            self.close_connection()

    def close_connection(self):
        """Close the connection to Snowflake."""
        if self.connection:
            self.connection.close()
            logging.info("Connection to Snowflake closed.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        logging.error("Usage: python emp_data_ingestion.py <data_file_path> <table_name>")
        sys.exit(1)

    data_file_path = sys.argv[1]
    table_name = sys.argv[2]

    
    snowflake_ingestion = SnowflakeIngestion(
        user='snagamolla06',
        password='Sandeep@2749',
        account='JJNDJCW-DP86849',
        warehouse='COMPUTE_WH',
        database='P01_EMP',
        schema='EMP_RAW'
    )

    logging.info(f"Starting data ingestion with file: {data_file_path} into table: {table_name}")

    

    # Connect to Snowflake and ingest the data
    snowflake_ingestion.connect()
    snowflake_ingestion.ingest_data(data_file_path, table_name)
