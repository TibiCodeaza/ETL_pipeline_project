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
    filename="etl_error_log.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def validate_sales_data(df, valid_product_ids, valid_customer_ids, product_prices):
    """
    Validate the sales data for:
    - Missing or null values in critical fields (product_id, customer_id, quantity, transaction_date).
    - Invalid data types in numeric fields (e.g., quantity).
    - Referential integrity issues with product_id and customer_id.
    """
    logging.info("Starting data validation for sales data.")

    # Check for missing critical fields
    missing_product_ids = df['product_id'].isnull().sum()
    missing_customer_ids = df['customer_id'].isnull().sum()
    missing_transaction_dates = df['transaction_date'].isnull().sum()
    logging.info(f"Missing product_id count: {missing_product_ids}")
    logging.info(f"Missing customer_id count: {missing_customer_ids}")
    logging.info(f"Missing transaction_date count: {missing_transaction_dates}")

    # Drop rows with missing critical fields
    df.dropna(subset=['product_id', 'customer_id', 'transaction_date'], inplace=True)

    # Check for invalid numeric values
    invalid_quantities = df[df['quantity'] <= 0]
    logging.warning(f"Found {len(invalid_quantities)} invalid quantity values.")

    # Replace invalid numeric values
    df['quantity'] = df['quantity'].apply(lambda x: max(x, 1) if x > 0 else 1)

    # Check referential integrity
    invalid_product_ids = df[~df['product_id'].isin(valid_product_ids)]
    invalid_customer_ids = df[~df['customer_id'].isin(valid_customer_ids)]
    logging.warning(f"Found {len(invalid_product_ids)} sales with invalid product_id.")
    logging.warning(f"Found {len(invalid_customer_ids)} sales with invalid customer_id.")

    # Remove rows with invalid product_id or customer_id
    df = df[df['product_id'].isin(valid_product_ids) & df['customer_id'].isin(valid_customer_ids)]

    # Add price from product_prices and calculate revenue
    df['price'] = df['product_id'].map(product_prices)
    df['revenue'] = df['quantity'] * df['price']

    logging.info("Sales data validation completed.")
    return df

def load_sales_data(sales_file):
    """
    Load, validate, and process sales data without directly storing price and revenue in the sales table.
    """
    try:
        # Load sales data
        sales_df = pd.read_csv(sales_file)
        sales_df['transaction_date'] = pd.to_datetime(sales_df['transaction_date'], errors='coerce')
        logging.info(f"Loaded {len(sales_df)} rows from {sales_file}.")

        # Fetch valid product_ids, customer_ids, and product prices
        with engine.connect() as conn:
            products_df = pd.read_sql("SELECT product_id, price FROM products", conn)
            valid_product_ids = products_df['product_id'].tolist()
            product_prices = products_df.set_index('product_id')['price'].to_dict()

            valid_customer_ids = pd.read_sql("SELECT customer_id FROM customers", conn)['customer_id'].tolist()

        # Validate sales data
        sales_df = validate_sales_data(sales_df, valid_product_ids, valid_customer_ids, product_prices)

        # Insert validated data into the sales table (excluding price and revenue)
        sales_df = sales_df.drop(columns=['price', 'revenue'])
        sales_df.to_sql('sales', con=engine, if_exists='append', index=False)
        logging.info(f"Inserted {len(sales_df)} valid sales records into the database.")
    except Exception as e:
        logging.error(f"Error during sales data ETL process: {e}")

def main():
    """
    Main ETL pipeline.
    """
    logging.info("ETL process started.")
    load_sales_data('sales.csv')
    logging.info("ETL process completed.")

if __name__ == "__main__":
    main()
