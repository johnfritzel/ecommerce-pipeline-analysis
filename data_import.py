import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database credentials
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

# Create a connection string and SQLAlchemy engine
connection_string = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = create_engine(connection_string)

# Mapping of CSV files to desired table names
table_name_mapping = {
    'cleaned_olist_geolocation_dataset.csv': 'geolocation',
    'cleaned_olist_customers_dataset.csv': 'customers',
    'cleaned_olist_orders_dataset.csv': 'orders',
    'cleaned_olist_order_reviews_dataset.csv': 'reviews',
    'cleaned_olist_order_payments_dataset.csv': 'payments',
    'cleaned_olist_products_dataset.csv': 'products',
    'cleaned_olist_sellers_dataset.csv': 'sellers',
    'cleaned_product_category_name_translation.csv': 'product_categories',
    'cleaned_olist_order_items_dataset.csv': 'items'
}

output_folder = os.getenv('OUTPUT_FOLDER')

def upload_csv_to_mysql(csv_file, table_name):
    file_path = os.path.join(output_folder, csv_file)

    try:
        df = pd.read_csv(file_path)
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f"Uploaded {csv_file} to {table_name} table successfully.\n")

    except Exception as e:
        print(f"Error uploading {csv_file} to {table_name}: {e}")

for csv_file, table_name in table_name_mapping.items():
    upload_csv_to_mysql(csv_file, table_name)