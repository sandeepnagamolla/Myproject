from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

def test_connection():
    try:
        # Create a SQLAlchemy engine
        engine = create_engine(URL(
            account='UVBOTLP-HH05795',
            user='snagamolla',
            password='Sandeep@2749',
            warehouse='COMPUTE_WH',
            database='P01_EMP',
            schema='EMP_RAW'
        ))
        
        # Test the connection
        with engine.connect() as connection:
            print("Connection successful")
            
    except Exception as e:
        print(f"Error: {e}")

# Test the connection
test_connection()
