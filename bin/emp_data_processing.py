import sys
import snowflake.connector
import os
import logging
from datetime import datetime
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import re


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


class snowflakeProcessing:
    def __init__(self, user, password, account, warehouse, database, schema):
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.connection = None
        logging.info("SnowflakeProcessing class initiated")

    def connect(self):
        try: 
            self.connection = snowflake.connector.connect(
                user = self.user,
                password = self.password,
                account = self.account,
                warehouse = self.warehouse,
                database = self.database,
                schema = self.schema
            )
            logging.info("connected to snowflake")
        except Exception as e:
            logging.error(f"Failed to connect snowflake error is {e}")
            sys.exit(1)
    def execute_dml(self, sql_path):
        
        if not self.connection:
            logging.error("No active connection to snowflake")
            return
        
        try:
            with open(sql_path,'r') as file:
                sql_commands = file.read()
                logging.info(f"Read the sql commands from {sql_path}")

            cur = self.connection.cursor()

            table_names = set()

            for command in sql_commands.split(';'):
                command = command.strip()
                if command:
                    logging.info("Executing each sql command {command}")
                    
                    cur.execute(command)

                    # Extract table names from the SQL command
                    matches = re.findall(r'insert\s+into\s+(\w+\.\w+)', command, re.IGNORECASE)
                    if matches:
                        table_names.update(matches)

                    
            cur.close()
            logging.info("All the DML comands executed successfully")
        
        except Exception as e:
            logging.error(f"Error in executing DML {e}")
            sys.exit(1)
        finally:
            for table_name in table_names:
                self.extract_df(table_name)
            self.close_connection()
    
            
    def extract_df(self, table_name):
        
        try:
            file_path = f"C:\\Users\\Sandeep\\OneDrive\\Desktop\\Project\\extracts\\{table_name}.txt"
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            query = f"select * from {table_name}"
            df = pd.read_sql(query, self.connection)
            df.to_csv(file_path, sep="|", index=False, header=None)
            logging.info(f"Data extracted to {file_path}")

        except Exception as e:
            logging.error(f"Error is {e}")

        
    def close_connection(self):
        if self.connection:
            self.connection.close()
            logging.info("Connection is closed")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        logging.info("Usage: python connector.py /<sql path>/emp_data_processing.sql")
        sys.exit(1)

    sql_path = sys.argv[1]

    # Instantiate the ConnectSnowflake class with appropriate credentials
    snowflake_connection = snowflakeProcessing(
        user='snagamolla06',
        password='Sandeep@2749',
        account='JJNDJCW-DP86849',
        warehouse='COMPUTE_WH',
        database='P01_EMP',
        schema='EMP_PROC'
    )

    # Connect to Snowflake and execute the DDL commands
    snowflake_connection.connect()
    snowflake_connection.execute_dml(sql_path)




                
        



