import pandas as pd
from faker import Faker
import random

# Initialize Faker
fake = Faker()

# Function to create products.csv
def create_products_csv(num_records):
    data = []
    for i in range(1, num_records + 1):
        record = {
            "product_id": i,
            "product_name": fake.word() if random.random() > 0.1 else None,  # 5-10% missing names
            "category": fake.random_element(["Electronics", "Clothing", "Books", "Food", "InvalidCategory", ""]) if random.random() > 0.8 else fake.random_element(["Electronics", "Clothing", "Books", "Food"]),  # 20% inconsistent
            "price": random.choice([fake.random_int(min=5, max=500), fake.random_int(min=-100, max=-1), fake.word()]) if random.random() > 0.9 else fake.random_int(min=5, max=500)  # 10% invalid prices
        }
        data.append(record)
    return pd.DataFrame(data)

# Function to create customers.csv
def create_customers_csv(num_records):
    data = []
    for i in range(1, num_records + 1):
        record = {
            "customer_id": i,
            "name": fake.name(),
            "email": fake.email() if random.random() > 0.1 else fake.word(),  # 10% invalid emails
            "country": fake.country() if random.random() > 0.2 else None  # 20% missing countries
        }
        data.append(record)
    
    # Introduce duplicate records
    for _ in range(num_records // 10):  # 10% duplicates
        data.append(random.choice(data))
    
    return pd.DataFrame(data)

# Function to create sales.csv
def create_sales_csv(num_records, num_products, num_customers):
    data = []
    for i in range(1, num_records + 1):
        record = {
            "transaction_id": i,
            "product_id": random.choice([random.randint(1, num_products), None]) if random.random() > 0.05 else None,  # 5% missing product_id
            "customer_id": random.choice([random.randint(1, num_customers), None]) if random.random() > 0.05 else None,  # 5% missing customer_id
            "quantity": random.choice([fake.random_int(min=1, max=10), 0, fake.random_int(min=-10, max=-1)]) if random.random() > 0.9 else fake.random_int(min=1, max=10),  # 10% invalid quantity
            "transaction_date": random.choice([fake.date_this_year().strftime('%Y-%m-%d'), "2024/13/40", "InvalidDate"]) if random.random() > 0.9 else fake.date_this_year().strftime('%Y-%m-%d')  # 10% invalid dates
        }
        data.append(record)
    return pd.DataFrame(data)

# Generate datasets
products_df = create_products_csv(150)
customers_df = create_customers_csv(600)
sales_df = create_sales_csv(1500, 150, 600)

# Save datasets to CSV
products_df.to_csv("products.csv", index=False)
customers_df.to_csv("customers.csv", index=False)
sales_df.to_csv("sales.csv", index=False)

print("CSV files 'products.csv', 'customers.csv', and 'sales.csv' have been created with data issues.")
