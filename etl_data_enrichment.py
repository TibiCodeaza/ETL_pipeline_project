import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import logging
import requests
from datetime import datetime
from sqlalchemy.sql import text

# Load environment variables
load_dotenv()
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
HOST = "localhost"
PORT = "5432" # Replace with your PostgreSQL host
DB_NAME = "dev" #insert database name

# Database connection string
CONN_STRING = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}"
engine = create_engine(CONN_STRING)

# Configure logging
logging.basicConfig(
    filename="data_transformation.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

API_URL = "https://676e7a37df5d7dac1ccac552.mockapi.io/api/v1/products"  # Replace with actual API URL


def fetch_product_metadata(api_url, max_pages=5):
    """
    Fetch product metadata from the external API and ensure consistent data types.
    """
    product_metadata = []
    page = 1

    while page <= max_pages:
        try:
            response = requests.get(f"{api_url}?page={page}", timeout=10)
            if response.status_code == 200:
                data = response.json()
                if not data:
                    logging.info("No more data available from API.")
                    break
                product_metadata.extend(data)
                page += 1
            else:
                logging.error(f"API Error: Status code {response.status_code}")
                break
        except requests.exceptions.RequestException as e:
            logging.error(f"API Request failed: {e}")
            break

    if not product_metadata:
        logging.warning("No records fetched from API.")
        return pd.DataFrame(columns=['id', 'description', 'rating', 'availability_status', 'product_id'])

    # Convert metadata to DataFrame
    metadata_df = pd.DataFrame(product_metadata)

    # Rename product_id from API to avoid conflicts
    metadata_df.rename(columns={'product_id': 'product_id_api', 'id': 'id'}, inplace=True)

    # Normalize types for consistency
    metadata_df['id'] = metadata_df['id'].astype(str).str.strip()
    metadata_df['product_id_api'] = metadata_df['product_id_api'].astype(str).fillna("")
    metadata_df['description'] = metadata_df['description'].astype(str).fillna("No description available")
    metadata_df['availability_status'] = metadata_df['availability_status'].astype(str).fillna("Unknown")
    metadata_df['rating'] = pd.to_numeric(metadata_df['rating'], errors='coerce').fillna(0.0)

    logging.info(f"Metadata DataFrame Sample:\n{metadata_df.head()}")
    return metadata_df

def update_etl_state_table(table_name, engine):
    """
    Updates the etl_state table with the table name and timestamp of the change.

    Args:
        table_name (str): The name of the table that was updated.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine for database connection.

    Returns:
        None
    """
    try:
        # Generate the current timestamp
        current_time = datetime.now()

        # SQL query to insert into etl_state table
        query = text("""
        INSERT INTO etl_state (table_name, time_processed)
        VALUES (:table_name, :time_processed);
        """)
        
        # Execute the query with commit
        with engine.begin() as connection:  # Use engine.begin to auto-commit
            connection.execute(query, {"table_name": table_name, "time_processed": current_time})
        
        logging.info(f"ETL state updated for table '{table_name}' at {current_time}.")
    except SQLAlchemyError as e:
        logging.error(f"Error updating etl_state table for '{table_name}': {e}")

    
def enrich_product_data(products_df, metadata_df):
    """
    Enrich the products DataFrame with metadata from the API.
    """
    if metadata_df.empty:
        logging.warning("No metadata fetched from API. Skipping enrichment.")
        products_df['description'] = "No description available"
        products_df['rating'] = 0.0
        products_df['availability_status'] = "Unknown"
        return products_df

    # Normalize product_id in products_df to match id from metadata
    products_df['product_id'] = products_df['product_id'].astype(str).str.strip()

    # Normalize id in metadata for consistency
    metadata_df['id'] = metadata_df['id'].astype(str).str.strip()

    # Log mismatched IDs
    missing_in_metadata = set(products_df['product_id']) - set(metadata_df['id'])
    missing_in_products = set(metadata_df['id']) - set(products_df['product_id'])

    logging.info(f"Product IDs missing in metadata_df: {missing_in_metadata}")
    logging.info(f"Product IDs missing in products_df: {missing_in_products}")

    # Merge the DataFrames
    enriched_df = pd.merge(
        products_df,
        metadata_df,
        left_on='product_id',
        right_on='id',
        how='left'
    )

    # Drop unnecessary columns after merging
    enriched_df.drop(columns=['id'], inplace=True)

    # Fill missing metadata fields with defaults
    enriched_df['description'] = enriched_df['description'].fillna("No description available")
    enriched_df['rating'] = enriched_df['rating'].fillna(0.0)
    enriched_df['availability_status'] = enriched_df['availability_status'].fillna("Unknown")

    logging.info(f"Enriched DataFrame Sample After Merge:\n{enriched_df.head()}")
    return enriched_df



def transform_products(products_df, sales_df):
    """
    Standardize categories and add transformations to products data.
    """
    # Ensure product_id in sales_df is integer then string
    sales_df['product_id'] = pd.to_numeric(sales_df['product_id'], errors='coerce').astype('Int64').astype(str).str.strip()

    # Standardize category names
    products_df['category'] = products_df['category'].str.lower().str.capitalize()

    # Ensure all price values are positive in products_df
    if 'price' in products_df.columns:
        products_df['price'] = pd.to_numeric(products_df['price'], errors='coerce').abs()
        logging.info("Converted negative price values to positive in products_df.")
    else:
        logging.warning("'price' column not found in products DataFrame.")

    # Calculate total_sales_quantity
    total_sales_quantity = sales_df.groupby('product_id')['quantity'].sum()
    logging.info(f"Total sales quantity:\n{total_sales_quantity}")

    # Calculate max_sales_quantity
    max_sales_quantity = total_sales_quantity.max()
    logging.info(f"Max sales quantity: {max_sales_quantity}")

    if max_sales_quantity > 0:
        products_df['popularity_score'] = products_df['product_id'].map(
            lambda x: (total_sales_quantity.get(x, 0) / max_sales_quantity) * 100
        ).fillna(0)
    else:
        logging.warning("Max sales quantity is 0. Setting all popularity scores to 0.")
        products_df['popularity_score'] = 0

    logging.info("Added popularity_score to products data.")
    return products_df



def transform_sales(sales_df, products_df):
    """
    Transform sales.csv by adding total_sales_value and purchase_month columns.
    """
    try:
        # Normalize product_id columns
        sales_df['product_id'] = pd.to_numeric(sales_df['product_id'], errors='coerce').astype('Int64').astype(str).str.strip()
        products_df['product_id'] = products_df['product_id'].astype(str).str.strip()

        # Log unique product_id values
        logging.info(f"Unique product_id in sales_df: {sales_df['product_id'].unique()}")
        logging.info(f"Unique product_id in products_df: {products_df['product_id'].unique()}")

        # Check for commong ids
        common_ids = set(sales_df['product_id']).intersection(set(products_df['product_id']))
        missing_in_products = set(sales_df['product_id']) - set(products_df['product_id'])

        # Log common ids
        logging.info(f"Common product_ids: {common_ids}")
        logging.warning(f"Product IDs in sales_df missing from products_df: {missing_in_products}")


        # Merge sales and products to include price
        sales_df = sales_df.merge(products_df[['product_id', 'price']], on='product_id', how='left')

        # Check for rows with missing price
        if sales_df.empty:
            logging.warning("Merged sales_df is empty after merge. Check product_id values.")
            return sales_df

        missing_price = sales_df[sales_df['price'].isnull()]
        if not missing_price.empty:
            logging.warning(f"Rows with missing price:\n{missing_price}")

        # Ensure 'price' column in sales_df is numeric
        sales_df['price'] = pd.to_numeric(sales_df['price'], errors='coerce')

        # Drop rows with missing or invalid price
        sales_df = sales_df.dropna(subset=['price'])

        # Calculate total_sales_value
        sales_df['total_sales_value'] = sales_df['quantity'] * sales_df['price']

        # Derive purchase_month
        sales_df['transaction_date'] = pd.to_datetime(sales_df['transaction_date'], errors='coerce')
        sales_df['purchase_month'] = sales_df['transaction_date'].dt.strftime('%Y-%m')

        logging.info(f"Transformed sales.csv: {len(sales_df)} rows processed.")

        # Save transformed data to CSV
        sales_df.to_csv("sales_transformed.csv", index=False)
        return sales_df
    except Exception as e:
        logging.error(f"Error transforming sales data: {e}", exc_info=True)
        return pd.DataFrame(columns=['product_id', 'quantity', 'price', 'total_sales_value', 'transaction_date', 'purchase_month'])





def transform_customers(customers_df, sales_df):
    """
    Transform customers.csv by splitting names and categorizing buyer types.
    """
    try:
        # Split name into first_name and last_name
        customers_df[['first_name', 'last_name']] = customers_df['name'].str.split(' ',n=1, expand=True)

        # Categorize buyers based on transaction counts
        customer_transaction_count = sales_df.groupby('customer_id').size()
        customers_df['buyer_category'] = customers_df['customer_id'].map(
            lambda x: (
                "Frequent Buyer" if customer_transaction_count.get(x, 0) > 10
                else "Occasional Buyer" if 5 <= customer_transaction_count.get(x, 0) <= 10
                else "Rare Buyer"
            )
        )

        # Deduplicate customers based on email
        customers_df = customers_df.sort_values('customer_id').drop_duplicates('email', keep='last')

        logging.info(f"Transformed customers.csv: {len(customers_df)} rows processed.")

        # Save transformed data to CSV
        customers_df.to_csv("customers_transformed.csv", index=False)
        return customers_df
    except Exception as e:
        logging.error(f"Error transforming customers data: {e}")
        return customers_df


def load_to_database(df, table_name):
    """
    Load a DataFrame into the PostgreSQL database.
    """
    try:
        df.to_sql(table_name, con=engine, if_exists="replace", index=False)
        logging.info(f"Loaded DataFrame into table '{table_name}'.")
    except Exception as e:
        logging.error(f"Error loading table '{table_name}' into database: {e}")


def main():
    """
    Main ETL pipeline function.
    """
    logging.info("ETL process started.")

    try:
        # Load datasets
        products_df = pd.read_csv("products.csv")
        sales_df = pd.read_csv("sales.csv")
        customers_df = pd.read_csv("customers.csv")

        # Fetch metadata and enrich products data
        metadata_df = fetch_product_metadata(API_URL)
        products_df = enrich_product_data(products_df, metadata_df)
        products_df = transform_products(products_df, sales_df)
        products_df.to_csv("products_transformed.csv", index=False)
        load_to_database(products_df, "products_transformed")
        update_etl_state_table("products_transformed", engine)

        # Handle missing data and transform sales data
        sales_df = transform_sales(sales_df, products_df)
        load_to_database(sales_df, "sales_transformed")
        update_etl_state_table("sales_transformed", engine)

        # Transform customers data
        customers_df = transform_customers(customers_df, sales_df)
        load_to_database(customers_df, "customers_transformed")
        update_etl_state_table("customers_transformed", engine)

        logging.info("ETL process completed successfully.")
    except Exception as e:
        logging.error(f"Critical error in ETL process: {e}")


if __name__ == "__main__":
    main()
