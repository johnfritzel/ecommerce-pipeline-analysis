import pandas as pd
import os
import re
import shutil
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Folder paths
input_folder = os.getenv('INPUT_FOLDER')
output_folder = os.getenv('OUTPUT_FOLDER')

# Dictionary of CSV file names mapped to their cleaning functions
CSV_FILES = {
    'olist_geolocation_dataset.csv': 'geolocation',
    'olist_customers_dataset.csv': 'customers',
    'olist_orders_dataset.csv': 'orders',
    'olist_order_reviews_dataset.csv': 'reviews',
    'olist_order_payments_dataset.csv': 'payments',
    'olist_products_dataset.csv': 'products',
    'olist_sellers_dataset.csv': 'sellers',
    'product_category_name_translation.csv': 'product_categories',
    'olist_order_items_dataset.csv': 'items'
}

# Text cleaning function
def clean_text(text):
    """Clean text by removing unwanted characters."""
    if isinstance(text, str):
        pattern = r'[^a-zA-Z0-9\s,.:"-_]'
        return re.sub(pattern, '', text)
    return text

def clean_all_columns(df):
    """Apply text cleaning to all columns in the DataFrame."""
    return df.apply(lambda col: col.map(clean_text) if col.dtype == 'object' else col)

# Data cleaning functions for specific datasets
def clean_geolocation(df):
    df = clean_all_columns(df)
    df['geolocation_zip_code_prefix'] = df['geolocation_zip_code_prefix'].astype(str).str.zfill(5)
    df = df[df['geolocation_zip_code_prefix'].str.len() == 5]
    df['geolocation_state'] = df['geolocation_state'].str[:2].str.upper()
    df = df[(df['geolocation_lat'].between(-90, 90)) & (df['geolocation_lng'].between(-180, 180))]
    return df.drop_duplicates()

def clean_customers(df):
    df = clean_all_columns(df)
    df = df.drop_duplicates(subset=['customer_id', 'customer_unique_id'], keep='first')
    df['customer_zip_code_prefix'] = df['customer_zip_code_prefix'].astype(str).str.zfill(5)
    df = df[df['customer_zip_code_prefix'].str.len() == 5]
    df['customer_state'] = df['customer_state'].str[:2].str.upper()
    return df

def clean_orders(df):
    df = clean_all_columns(df)
    date_columns = [
        'order_purchase_timestamp', 
        'order_approved_at', 
        'order_delivered_carrier_date', 
        'order_delivered_customer_date', 
        'order_estimated_delivery_date'
    ]
    df[date_columns] = df[date_columns].apply(pd.to_datetime, errors='coerce')
    df = df[df[date_columns].apply(lambda x: x.is_monotonic_increasing, axis=1)]
    return df.dropna()

def clean_order_reviews(df):
    df = clean_all_columns(df)
    df = df[df['review_score'].between(1, 5)]
    df['review_creation_date'] = pd.to_datetime(df['review_creation_date'], errors='coerce')
    df['review_answer_timestamp'] = pd.to_datetime(df['review_answer_timestamp'], errors='coerce')
    df = df[df['review_creation_date'] <= df['review_answer_timestamp']]
    df = df.drop_duplicates(subset='review_id', keep='first')
    return df.drop(columns=[col for col in ['review_comment_title', 'review_comment_message'] if col in df.columns])

def clean_order_payments(df):
    df = clean_all_columns(df)
    return df[(df['payment_installments'] > 0) & (df['payment_value'] > 0)]

def clean_order_items(df):
    # Nothing to do
    return df

def clean_product_categories(df):
    df = clean_all_columns(df)
    df['product_category_name'] = df['product_category_name'].str.strip()
    df['product_category_name_english'] = df['product_category_name_english'].str.strip()
    return df.drop_duplicates(subset='product_category_name_english', keep='first')

def clean_products(df):
    df = clean_all_columns(df)
    df.rename(columns={
        'product_name_lenght': 'product_name_length',
        'product_description_lenght': 'product_description_length'
    }, inplace=True)
    for col in ['product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']:
        df = df[df[col] > 0]
    return df.dropna()

def clean_sellers(df):
    df = clean_all_columns(df)
    df['seller_zip_code_prefix'] = df['seller_zip_code_prefix'].astype(str).str.zfill(5)
    df = df[df['seller_zip_code_prefix'].str.len() == 5]
    df['seller_state'] = df['seller_state'].str[:2].str.upper()
    return df

def clean_order_items(df):
    df = clean_all_columns(df)
    return df[(df['price'] > 0) & (df['freight_value'] >= 0)].assign(
        shipping_limit_date=pd.to_datetime(df['shipping_limit_date'], errors='coerce')
    )

def process_file(file_name, cleaning_function, input_folder, output_folder):
    """Read, clean, and save the CSV file."""
    csv_file_path = os.path.join(input_folder, file_name)

    if os.path.exists(csv_file_path):
        print(f"Processing {file_name}...")
        df = pd.read_csv(csv_file_path)
        cleaned_df = cleaning_function(df)
        cleaned_df.to_csv(os.path.join(output_folder, f'cleaned_{file_name}'), index=False)
        print(f"Data cleaning process for {file_name} successfully completed.\n")
        return cleaned_df  
    return None

# Process pairs of files based on common key columns
def process_file_pair(df1, df2, key_columns, file1_name, file2_name, output_folder):
    """Process pairs of DataFrames based on common key columns."""
    original_shape1 = df1.shape
    original_shape2 = df2.shape

    key1, key2 = (key_columns[0], key_columns[0]) if len(key_columns) == 1 else key_columns

    common_values = set(df1[key1]).intersection(set(df2[key2]))
    df1_cleaned = df1[df1[key1].isin(common_values)]
    df2_cleaned = df2[df2[key2].isin(common_values)]

    rows_removed1 = original_shape1[0] - df1_cleaned.shape[0]
    rows_removed2 = original_shape2[0] - df2_cleaned.shape[0]

    cleaned_file1_path = os.path.join(output_folder, f'cleaned_{file1_name}')
    cleaned_file2_path = os.path.join(output_folder, f'cleaned_{file2_name}')

    df1_cleaned.to_csv(cleaned_file1_path, index=False)
    df2_cleaned.to_csv(cleaned_file2_path, index=False)

    print(f"Processed {file1_name} and {file2_name} using keys {key_columns}:")
    print(f"  {file1_name}: {original_shape1[0]} rows, {original_shape1[1]} columns before cleaning")
    print(f"  {file2_name}: {original_shape2[0]} rows, {original_shape2[1]} columns before cleaning")
    print(f"  {file1_name}: {rows_removed1} rows removed, {df1_cleaned.shape[0]} rows remaining")
    print(f"  {file2_name}: {rows_removed2} rows removed, {df2_cleaned.shape[0]} rows remaining\n")

def main():
    """Main function to process CSV files and their pairs."""
    input_folder = os.getenv('INPUT_FOLDER')
    output_folder = os.getenv('OUTPUT_FOLDER')

    cleaning_functions = {
        'geolocation': clean_geolocation,
        'customers': clean_customers,
        'orders': clean_orders,
        'reviews': clean_order_reviews,
        'payments': clean_order_payments,
        'products': clean_products,
        'sellers': clean_sellers,
        'product_categories': clean_product_categories,
        'items': clean_order_items
    }

    cleaned_dataframes = {}

    # Process individual files
    for csv_file, dataset_type in CSV_FILES.items():
        cleaning_function = cleaning_functions.get(dataset_type)
        if cleaning_function:
            cleaned_df = process_file(csv_file, cleaning_function, input_folder, output_folder)
            cleaned_dataframes[csv_file] = cleaned_df
        else:
            shutil.copy2(os.path.join(input_folder, csv_file), output_folder)
            print(f"No cleaning function found for {dataset_type}. Copying {csv_file} without cleaning.\n")

    # Process file pairs
    file_pairs = [
        ('olist_customers_dataset.csv', 'olist_geolocation_dataset.csv', ['customer_zip_code_prefix', 'geolocation_zip_code_prefix']),
        ('olist_customers_dataset.csv', 'olist_orders_dataset.csv', ['customer_id']),
        ('olist_orders_dataset.csv', 'olist_order_reviews_dataset.csv', ['order_id']),
        ('olist_orders_dataset.csv', 'olist_order_payments_dataset.csv', ['order_id']),
        ('olist_products_dataset.csv', 'product_category_name_translation.csv', ['product_category_name']),
        ('olist_sellers_dataset.csv', 'olist_geolocation_dataset.csv', ['seller_zip_code_prefix', 'geolocation_zip_code_prefix']),
        ('olist_order_items_dataset.csv', 'olist_orders_dataset.csv', ['order_id']),
        ('olist_order_items_dataset.csv', 'olist_products_dataset.csv', ['product_id']),
        ('olist_order_items_dataset.csv', 'olist_sellers_dataset.csv', ['seller_id'])
    ]

    for file1, file2, key_columns in file_pairs:
        df1_cleaned = cleaned_dataframes.get(file1)
        df2_cleaned = cleaned_dataframes.get(file2)

        if df1_cleaned is not None and df2_cleaned is not None:
            process_file_pair(df1_cleaned, df2_cleaned, key_columns, file1, file2, output_folder)


if __name__ == "__main__":
    main()