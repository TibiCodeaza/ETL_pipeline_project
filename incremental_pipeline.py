import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
HOST = "localhost"
PORT = "5432" # Replace with your PostgreSQL host
DB_NAME = "dev" #insert database name

# Connection string
CONN_STRING = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}"
engine = create_engine(CONN_STRING)

# Configure logging
logging.basicConfig(
    filename="incremental_etl.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

WATERMARK_TABLE = "etl_state"

def create_watermark_table():
    """
    Create the watermark table if it does not exist.
    """
    try:
        with engine.begin() as conn:  # Use engine.begin() to ensure transactions are committed
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS etl_state (
                    id SERIAL PRIMARY KEY,
                    last_processed_date DATE
                );
            """))
            logging.info(f"Watermark table '{WATERMARK_TABLE}' created or already exists.")
    except SQLAlchemyError as e:
        logging.error(f"Error creating watermark table: {e}")
        raise

def verify_watermark_table():
    """
    Verify that the watermark table exists in the database.
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT * FROM information_schema.tables
                WHERE table_name = '{WATERMARK_TABLE}';
            """))
            if result.rowcount == 0:
                logging.error(f"Watermark table '{WATERMARK_TABLE}' does not exist.")
                raise Exception("Watermark table creation failed.")
            logging.info(f"Watermark table '{WATERMARK_TABLE}' verified successfully.")
    except SQLAlchemyError as e:
        logging.error(f"Error verifying watermark table: {e}")
        raise

def get_last_processed_date():
    """
    Retrieve the last processed transaction_date from the watermark table.
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT last_processed_date FROM {WATERMARK_TABLE} LIMIT 1;"))
            row = result.fetchone()
            return row[0] if row else None
    except SQLAlchemyError as e:
        logging.error(f"Error retrieving last processed date: {e}")
        return None

def update_last_processed_date(new_date):
    """
    Update the last processed transaction_date in the watermark table.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(f"""
                INSERT INTO {WATERMARK_TABLE} (id, last_processed_date)
                VALUES (1, '{new_date}')
                ON CONFLICT (id) DO UPDATE SET last_processed_date = '{new_date}';
            """))
            logging.info(f"Updated last processed date to {new_date}.")
    except SQLAlchemyError as e:
        logging.error(f"Error updating last processed date: {e}")

def process_sales_data(sales_file):
    """
    Incrementally load sales data based on the last processed date.
    """
    try:
        sales_df = pd.read_csv(sales_file)
        sales_df['transaction_date'] = pd.to_datetime(sales_df['transaction_date'], errors='coerce')
        sales_df.dropna(subset=['transaction_date'], inplace=True)
        last_processed_date = get_last_processed_date()
        new_sales_df = sales_df[sales_df['transaction_date'] > last_processed_date] if last_processed_date else sales_df
        if not new_sales_df.empty:
            new_sales_df.to_sql('sales', con=engine, if_exists='append', index=False)
            max_date = new_sales_df['transaction_date'].max().date()
            update_last_processed_date(max_date)
        else:
            logging.info("No new records to process.")
    except Exception as e:
        logging.error(f"Error processing sales data: {e}")

def incremental_pipeline():
    logging.info("Incremental ETL process started.")
    create_watermark_table()
    verify_watermark_table()
    process_sales_data('sales.csv')
    
    logging.info("Incremental ETL process completed.")

if __name__ == "__main__":
    incremental_pipeline()
