import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def import_csv_to_postgres():
    """
    Import all CSV files from a specified directory into PostgreSQL tables.
    Uses environment variables for database connection.
    """
    # Retrieve database connection details from environment variables
    DB_USERNAME = os.getenv('DB_USERNAME')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME')

    

    # Construct connection string
    CONNECTION_STRING = f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    
    
    # Create SQLAlchemy engine
    engine = create_engine(CONNECTION_STRING)
    # Path to CSV files directory
    CSV_DIRECTORY = 'data/csv_files/'
    
    # Iterate through CSV files in the directory
    for filename in os.listdir(CSV_DIRECTORY):
        if filename.endswith('.csv'):
            # Generate table name from filename (remove .csv, convert to lowercase)
            table_name = os.path.splitext(filename)[0].lower()
            
            # Full path to the CSV file
            csv_path = os.path.join(CSV_DIRECTORY, filename)
            
            try:
                print(csv_path + " ...")
                # Read CSV file
                df = pd.read_csv(csv_path)
                
                # Write to PostgreSQL
                df.to_sql(
                    name=table_name,  # table name in database
                    con=engine,  # database connection
                    if_exists='replace',  # what to do if table already exists
                    index=False  # don't write index as a column
                )
                print(f"Successfully imported {filename} to table {table_name}")
            
            except Exception as e:
                print(f"Error importing {filename}: {e}")

# Run the import when the script is executed
if __name__ == '__main__':
    import_csv_to_postgres()