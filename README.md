
# E-commerce Data Pipeline and Analysis

This is an Extract, Transform, and Load (ETL) pipeline that processes and cleans e-commerce datasets from [Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data), a Brazilian e-commerce platform, and uploads the cleaned data to a PostgreSQL database.

## Table of Contents
- Overview
- Features
- Requirements
- Installation
- Usage
- File Structure
- Data Cleaning Process
- Database Upload
- Environment Variables
  
&nbsp;
## Overview
This script automates the process of cleaning and preparing e-commerce datasets for analysis and database storage. It handles various datasets including customer information, orders, products, sellers, and more.

&nbsp;
## Features
- Cleans and processes multiple CSV files
- Handles data type conversions and format standardization
- Removes duplicates and invalid entries
- Ensures data consistency across related datasets
- Uploads cleaned data to a PostgreSQL database

&nbsp;
## Requirements
- Python 3.12.2
- pandas
- SQLAlchemy
- python-dotenv
- PostgreSQL database

&nbsp;
## Installation
1. Clone this repository.
```
git clone https://github.com/johnfritzel/ecommerce-pipeline-analysis.git
```

2. Create a virtual environment.
```
python -m venv venv
```

2. Activate the virtual environment.
```
venv\Scripts\activate # Windows
source venv/bin/activate  # Linux/macOS
```

4. Install required dependencies.
```
pip install -r requirements.txt
```

&nbsp;
## File Structure
- `create_db.sql`: The SQL script to create the database.
- `create_tables.sql`: The SQL script to create the tables and their relationships.
- `data_profiling.py`: The Python script that scan CSV files located in the input folder and perform the following checks:

        1. Count null values in each column of the CSV files.
        2. Identify duplicate rows in the CSV files.
        3. Detect unwanted characters in string columns, and display the rows containing them.
- `data_cleaning_and_uploading.py`: The main script that orchestrates the data cleaning and upload process. 
- `requirements.txt`: List of Python dependencies.
- `.env`: Configuration file for environment variables.
- `input`: Directory containing the raw CSV files.
- `output`: Directory where cleaned CSV files are saved.
- `.gitignore`: File that tells Git which files or directories to ignore when committing changes.

&nbsp;
## Data Cleaning Process
The script performs the following cleaning operations:

    1. Removes unwanted characters from text fields.
    2. Standardizes date formats.
    3. Ensures numeric fields are within valid ranges.
    4. Removes duplicate entries.
    5. Standardizes geographic data (e.g., zip codes, state abbreviations).
    6. Ensures data consistency across related datasets.

&nbsp;
## Database Schema
The database schema consists of the following tables:

1. geolocation
    * Primary Key: geolocation_zip_code_prefix (INTEGER)
    * Columns: geolocation_lat, geolocation_lng, geolocation_city, geolocation_state

2. customers
    * Primary Key: customer_id (UUID)
    * Foreign Key: customer_zip_code_prefix references geolocation(geolocation_zip_code_prefix)
    * Columns: customer_unique_id, customer_city, customer_state

3. orders
    * Primary Key: order_id (UUID)
    * Foreign Key: customer_id references customers(customer_id)
    * Columns: order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date

4. order_reviews
    * Primary Key: review_id (UUID)
    * Foreign Key: order_id references orders(order_id)
    * Columns: review_score, review_creation_date, review_answer_timestamp

5. order_payments
    * Primary Key: (order_id, payment_sequential)
    * Foreign Key: order_id references orders(order_id)
    * Columns: payment_type, payment_installments, payment_value

6. product_categories
    * Primary Key: product_category_name (VARCHAR)
    * Columns: product_category_name_english

7. products
    * Primary Key: product_id (UUID)
    * Foreign Key: product_category_name references product_categories(product_category_name)
    * Columns: product_name_length, product_description_length, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm

8. sellers
    * Primary Key: seller_id (UUID)
    * Foreign Key: seller_zip_code_prefix references geolocation(geolocation_zip_code_prefix)
    * Columns: seller_city, seller_state

9. order_items
    * Primary Key: (order_id, order_item_id)
    * Foreign Keys:
        - order_id references orders(order_id)
        - product_id references products(product_id)
        - seller_id references sellers(seller_id)
    * Columns: shipping_limit_date, price, freight_value

Each table includes appropriate indexes to optimize query performance.

&nbsp;
## Environment Variables
Create a `.env` file in the project root with the following variables:
```
DB_NAME=your_database_name
DB_USER=your_database_user
DB_PASSWORD=your_database_password
DB_HOST=your_database_host
DB_PORT=your_database_port
INPUT_FOLDER=path/to/input/folder
OUTPUT_FOLDER=path/to/output/folder
```
