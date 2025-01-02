import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    filename="etl_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Database connection info
DB_TYPE = "postgresql"
HOST = "localhost"      # Replace with your PostgreSQL host
PORT = "5432"           # Default PostgreSQL port
DB_NAME = "dev"  # Replace with your database name

# Get user and password from environment variables
USER = os.getenv("DB_USER")  # Default to "default_user" if not set
PASSWORD = os.getenv("DB_PASSWORD")  # Default to "default_password" if not set


# Construct connection string
CONN_STRING = f"{DB_TYPE}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}"
engine = create_engine(CONN_STRING)


# Data cleaning functions
def clean_products(df):
    logging.info("Cleaning products data.")
    # Handle missing product names
    missing_names = df['product_name'].isnull().sum()
    df['product_name'] = df['product_name'].fillna("Unknown")
    logging.info(f"Filled {missing_names} missing product names with 'Unknown'.")
    
    # Normalize inconsistent categories
    valid_categories = {"Electronics", "Clothing", "Books", "Food"}
    inconsistent_categories = df[~df['category'].isin(valid_categories)]['category'].unique()
    df['category'] = df['category'].apply(lambda x: x if x in valid_categories else "Other")
    logging.info(f"Normalized inconsistent categories: {inconsistent_categories} to 'Other'.")
    
    # Correct invalid prices
    df['price'] = pd.to_numeric(df['price'], errors='coerce')  # Convert invalid values to NaN
    invalid_prices = df['price'].isnull().sum()
    df['price'] = df['price'].fillna(0).clip(lower=0)  # Replace NaN and negative values with 0
    logging.info(f"Replaced {invalid_prices} invalid or negative prices with 0.")
    
    return df

def clean_customers(df):
    logging.info("Cleaning customers data.")
    # Remove duplicates
    duplicate_count = df.duplicated(subset=['customer_id']).sum()
    df = df.drop_duplicates(subset=['customer_id'], keep='first')
    logging.info(f"Removed {duplicate_count} duplicate customer records.")
    
    # Handle missing countries
    missing_countries = df['country'].isnull().sum()
    df['country'] = df['country'].fillna("Unknown")
    logging.info(f"Filled {missing_countries} missing countries with 'Unknown'.")
    
    # Validate email format
    invalid_emails = df[~df['email'].str.contains("@")]['email'].count()
    df['email'] = df['email'].apply(lambda x: x if "@" in x and "." in x else "invalid@example.com")
    logging.info(f"Replaced {invalid_emails} invalid email addresses with 'invalid@example.com'.")
    
    return df

def clean_sales(df):
    def clean_sales(df):
    # """
    # Clean sales data by handling missing values, invalid dates, and invalid quantities.
    # """
    
        logging.info("Cleaning sales data.")

    # Handle missing product_id and customer_id
    missing_product_ids = df['product_id'].isnull().sum()
    missing_customer_ids = df['customer_id'].isnull().sum()
    df['product_id'] = df['product_id'].fillna(-1).astype(int)
    df['customer_id'] = df['customer_id'].fillna(-1).astype(int)
    logging.info(f"Filled {missing_product_ids} missing product_ids with -1.")
    logging.info(f"Filled {missing_customer_ids} missing customer_ids with -1.")
    
    # Correct invalid transaction dates
    df['transaction_date'] = pd.to_datetime(
        df['transaction_date'],  # The date column
        format='%Y-%m-%d',      # Expected format
        errors='coerce'         # Coerce invalid dates to NaT
    )
    invalid_dates = df['transaction_date'].isnull().sum()
    df['transaction_date'] = df['transaction_date'].fillna(datetime(2000, 1, 1).date())  # Default invalid dates
    logging.info(f"Replaced {invalid_dates} invalid transaction dates with '2000-01-01'.")
    
    # Correct invalid or negative quantities
    invalid_quantities = (df['quantity'] <= 0).sum()
    df['quantity'] = df['quantity'].apply(lambda x: max(x, 1) if x > 0 else 1)
    logging.info(f"Replaced {invalid_quantities} invalid or negative quantities with 1.")
    
    return df

# Load cleaned data into the database
def load_to_db(df, table_name):
    logging.info(f"Loading data into {table_name} table.")
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    logging.info(f"Data loaded into {table_name} successfully.")

# Main ETL process
def etl_pipeline(folder_path):
    logging.info("ETL process started.")
    
    # Extract data
    try:
        products = pd.read_csv(os.path.join(folder_path, 'products.csv'))
        customers = pd.read_csv(os.path.join(folder_path, 'customers.csv'))
        sales = pd.read_csv(os.path.join(folder_path, 'sales.csv'))
        logging.info("Data extracted successfully.")
    except Exception as e:
        logging.error(f"Error extracting data: {e}")
        return
    
    # Transform data
    try:
        products = clean_products(products)
        customers = clean_customers(customers)
        sales = clean_sales(sales)
        logging.info("Data cleaned and transformed successfully.")
    except Exception as e:
        logging.error(f"Error during data cleaning: {e}")
        return
    
    # Load data
    try:
        load_to_db(products, 'products')
        load_to_db(customers, 'customers')
        load_to_db(sales, 'sales')
        logging.info("Data loaded into the database successfully.")
    except Exception as e:
        logging.error(f"Error loading data into the database: {e}")
        return
    
    logging.info("ETL process completed successfully.")

# Run the ETL pipeline
if __name__ == "__main__":
    folder_path = r"C:\Users\Tibi\Desktop\data engineer test"  # Replace with folder path
    etl_pipeline(folder_path)
