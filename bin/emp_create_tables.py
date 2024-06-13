import sys
import snowflake.connector
import os
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

class ConnectSnowflake:
    def __init__(self, user, password, account, warehouse, database, schema):
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.connection = None
        logging.info("Initialized the ConnectSnowflake class")

    def connect(self):
        """Establish a connection to Snowflake."""
        try:
            self.connection = snowflake.connector.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema
            )
            logging.info("Connected to Snowflake")
        except Exception as e:
            logging.error(f"Failed to connect to Snowflake: {e}")
            sys.exit(1)

    def execute_ddl(self, sql_path):
        """Execute DDL commands from a specified SQL file."""
        if not self.connection:
            logging.error("No active Snowflake connection.")
            return 

        try:
            # Read the SQL DDL commands from the file
            with open(sql_path, 'r') as file:
                sql_commands = file.read()
                logging.info(f"Read sql commands from {sql_path}")

            cur = self.connection.cursor()

            # Split and execute each SQL command
            for command in sql_commands.split(';'):
                command = command.strip()
                if command:
                    logging.info(f"Executing SQL command: {command}")
                    cur.execute(command)

            # Close the cursor
            cur.close()
            logging.info("All DDL statements executed successfully.")
        except Exception as e:
            logging.error(f"Error executing DDL statements: {e}")
        finally:
            self.close_connection()

    def close_connection(self):
        """Close the connection to Snowflake."""
        if self.connection:
            self.connection.close()
            logging.info("Connection to Snowflake closed.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logging.info("Usage: python connector.py /<sql path>/Employee.sql")
        sys.exit(1)

    sql_path = sys.argv[1]

    # Instantiate the ConnectSnowflake class with appropriate credentials
    snowflake_connection = ConnectSnowflake(
        user='snagamolla06',
        password='Sandeep@2749',
        account='JJNDJCW-DP86849',
        warehouse='COMPUTE_WH',
        database='P01_EMP',
        schema='EMP_PROC'
    )

    # Connect to Snowflake and execute the DDL commands
    snowflake_connection.connect()
    snowflake_connection.execute_ddl(sql_path)
