# README: Advanced ETL Pipeline

## Overview
This ETL pipeline performs advanced data transformations and enrichment of datasets. It processes `products`, `sales`, and 'customers' to:
- Standardize categories
- Enrich data with popularity scores and metadata from an external API
- Handle missing and incorrect data
- Calculate derived features such as total sales value and purchase month
- Deduplicate and categorize customer data based on purchase history

The pipeline saves transformed data into both CSV files and a PostgreSQL database.

This implementation was guided entirely by the requirements and scenarios provided in the "Data Engineer Test" document, which outlines tasks related to ETL pipelines, data modeling, query optimization, and error handling. Every feature and enhancement addresses specific objectives and data issues described in the document.

---

Design Choices, Data Quality Issues, and Optimizations

Design Choices

Separation of Concerns:

Divided the ETL process into distinct stages (Extract, Transform, Load) to ensure modularity and clarity.

External API integration was handled in a separate function, enabling easy debugging and retries.

Scalable and Robust Architecture:

Used Pandas for in-memory data transformations due to its simplicity and efficiency for medium-sized datasets.

Integrated logging at every stage to ensure traceability and easier debugging.

Use of Environment Variables:

Utilized .env files for secure and configurable database connections.

Data Transformation Techniques:

Enriched datasets with derived columns like total_sales_value, purchase_month, and popularity_score to provide actionable insights.

Standardized and cleaned input datasets to ensure compatibility during merging and analysis.

Data Quality Issues Detected

Missing Data:

Some price values in products.csv were missing.

product_id values in products.csv did not always match those in sales.csv or the external API metadata.

Inconsistent Data:

Categories in products.csv were inconsistently labeled (e.g., Electronics, electronics, ELECTRONICS).

Negative prices were present, requiring correction.

Duplicate Records:

Duplicate customer records in customers.csv based on email.

API Metadata Gaps:

Some products in products.csv were missing from the API response, leaving enriched fields empty.

Optimizations Made

Data Cleaning:

Standardized category names to ensure consistency.

Replaced missing prices with the category mean and converted negative prices to positive values.

Ensured that all product_id values were strings for consistent merging.

API Integration:

Handled API pagination and timeouts gracefully.

Logged mismatched IDs to track missing metadata.

Used default values for missing API fields, ensuring no blank fields post-enrichment.

Efficient Merging:

Removed duplicate product_id values before merging.

Leveraged indexing for faster lookups during transformations.

Derived Metrics:

Calculated popularity_score using normalized sales data, providing a clear ranking for products.

Categorized customers into Frequent, Occasional, and Rare Buyers, adding segmentation for analysis.

## Prerequisites

### 1. Environment Setup
Ensure you have Python 3.8+ installed on your system. Create a virtual environment for dependency isolation:

```bash
python -m venv dataengineertest
source dataengineertest/bin/activate  # For Linux/Mac
dataengineertest\Scripts\activate  # For Windows
```

---

## Dependencies
Install the required Python libraries using `pip`:

```bash
pip install -r requirements.txt
```

### `requirements.txt`
Here’s a list of dependencies:

```
pandas
numpy
sqlalchemy
psycopg2-binary
requests
dotenv
logging
```

---

## Setting Up the Environment Variables
Create a `.env` file in the root directory to store sensitive database credentials:

```
DB_USER=your_database_user
DB_PASSWORD=your_database_password
DB_NAME=your_database_name
DB_HOST=localhost
DB_PORT=5432
```

---

## Instructions to Run

### 1. Start the PostgreSQL Database
Ensure your PostgreSQL database is running and accessible. The script connects to the database using the credentials in the `.env` file.

### 2. Place the Input CSV Files
Ensure the following input files are in the same directory as the script:
- `products.csv`
- `sales.csv`
- `customers.csv`

### 3. Run the ETL Script
Execute the Python script:

```bash
python advanced_etl_pipeline.py
```

This will:
1. Fetch metadata from the external API.
2. Transform and enrich the datasets.
3. Save the transformed data into CSV files and the database.

---

## Output Files

### Transformed CSVs
- `products_transformed.csv`
- `sales_transformed.csv`
- `customers_transformed.csv`

### Database Tables
The transformed datasets will also be loaded into the PostgreSQL database as:
- `products_transformed`
- `sales_transformed`
- `customers_transformed`

---

## Advanced Features

### 1. Derived Columns
- **`total_sales_value`**: Calculated in `sales_transformed.csv` as `quantity * price`.
- **`purchase_month`**: Extracted from `transaction_date` in the format `YYYY-MM`.
- **`popularity_score`**: Added to `products_transformed.csv`, calculated as:
  ```
  (total_sales_quantity / max(total_sales_quantity)) * 100
  ```

### 2. Customer Enrichment
- Categorizes customers based on their purchase history:
  - `Frequent Buyer`: >10 transactions
  - `Occasional Buyer`: 5-10 transactions
  - `Rare Buyer`: <5 transactions
- Splits the `name` column into `first_name` and `last_name`.

### 3. Handling Missing and Incorrect Data
- Missing `price` values in `products.csv` are replaced with the average price of the same category.
- Negative prices are converted to positive.
- Deduplicates customers based on `email`.

### 4. External API Integration
- Fetches metadata for products, including `description`, `rating`, and `availability_status`.
- Ensures `product_id` from the API is integrated without conflicts.

---

## Troubleshooting

### Common Errors

1. **Database Connection Issues**:
   - Check the `.env` file for correct credentials.
   - Ensure the PostgreSQL database is running on the specified host and port.

2. **Missing Columns**:
   - Ensure input CSV files have the required columns.

3. **API Fetching Issues**:
   - Verify the API URL is correct and accessible.

4. **Empty Output Files**:
   - Check the logs for mismatched `product_id` values between `sales.csv` and `products.csv`.

### Logging
Logs are saved in `data_transformation.log` for debugging purposes. They include detailed information about missing data, transformation steps, and API fetching errors.

---

## Author
This ETL pipeline was designed and implemented to meet the detailed requirements of the "Data Engineer Test" document, addressing complex data challenges with robust solutions.

