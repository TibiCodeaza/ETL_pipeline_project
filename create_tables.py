import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Database credentials from .env
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")

# Other connection details
HOST = "localhost"      # Hardcoded database host
PORT = "5432"           # Default PostgreSQL port
DB_NAME = "dev"  # Hardcoded database name

# SQL commands to create tables
CREATE_PRODUCTS_TABLE = """
CREATE TABLE IF NOT EXISTS products (
    product_id INT PRIMARY KEY,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    price NUMERIC NOT NULL
);
"""

CREATE_CUSTOMERS_TABLE = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id INT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    country TEXT
);
"""

CREATE_SALES_TABLE = """
CREATE TABLE IF NOT EXISTS sales (
    transaction_id INT PRIMARY KEY,
    product_id INT REFERENCES products(product_id) ON DELETE CASCADE,
    customer_id INT REFERENCES customers(customer_id) ON DELETE CASCADE,
    quantity INT NOT NULL,
    transaction_date DATE NOT NULL
);
"""

def create_tables():
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        cursor = conn.cursor()
        print("Connected to the database.")

        # Execute table creation queries
        cursor.execute(CREATE_PRODUCTS_TABLE)
        print("Created 'products' table.")
        
        cursor.execute(CREATE_CUSTOMERS_TABLE)
        print("Created 'customers' table.")
        
        cursor.execute(CREATE_SALES_TABLE)
        print("Created 'sales' table.")

        # Commit changes and close connection
        conn.commit()
        cursor.close()
        conn.close()
        print("Tables created successfully.")
    
    except Exception as e:
        print(f"Error creating tables: {e}")

if __name__ == "__main__":
    create_tables()
