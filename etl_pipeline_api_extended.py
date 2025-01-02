import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import logging
import requests

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
    filename="etl_log.log",
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


def load_products_data(products_file):
    """
    Load, enrich, and insert products data into the database.
    """
    try:
        # Load products.csv
        products_df = pd.read_csv(products_file)
        logging.info(f"Products DataFrame Sample:\n{products_df.head()}")

        # Fetch metadata from API
        metadata_df = fetch_product_metadata(API_URL)

        # Enrich product data
        enriched_products_df = enrich_product_data(products_df, metadata_df)

        # Insert enriched data into the database
        enriched_products_df.to_sql('products', con=engine, if_exists='replace', index=False)
        logging.info("Inserted data into products table successfully.")
    except Exception as e:
        logging.error(f"Error during products data ETL process: {e}")


def main():
    """
    Main ETL pipeline function.
    """
    logging.info("ETL process started.")
    load_products_data('products.csv')
    logging.info("ETL process completed.")


if __name__ == "__main__":
    main()
