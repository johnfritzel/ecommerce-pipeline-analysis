import os
import pandas as pd
import re
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def count_nulls_and_duplicates_in_csv_files(folder_path):
    # Pattern to identify unwanted characters
    pattern = r'[^a-zA-Z0-9\s,.:"-_]'

    # Loop through all files in the specified folder
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            
            # Load the CSV file into a DataFrame
            df = pd.read_csv(file_path)
            
            # Count null values in each column
            null_counts = df.isnull().sum()
            
            # Check for duplicate rows
            duplicate_count = df.duplicated().sum()

            # Check for unwanted characters in each column
            columns_with_unwanted_chars = {}
            unwanted_chars_summary = {}
            
            for column in df.columns:
                if df[column].dtype == object:  # Only check string columns
                    # Identify rows with unwanted characters
                    unwanted_char_rows = df[column].str.contains(pattern, na=False)
                    if unwanted_char_rows.any():
                        # Count rows with unwanted characters
                        columns_with_unwanted_chars[column] = unwanted_char_rows.sum()

                        # Find all unwanted characters in these rows
                        unwanted_chars = df[column][unwanted_char_rows].apply(lambda x: ''.join(set(re.findall(pattern, str(x)))))
                        unwanted_chars_summary[column] = set(''.join(unwanted_chars))

            
            # Print the filename, null value counts, duplicate row count, and columns with unwanted characters
            print(f"Data in {filename}:")
            print("  Null values:")
            for column, count in null_counts.items():
                print(f"    {column}: {count}")
            print(f"  Duplicate rows: {duplicate_count}")
            
            if columns_with_unwanted_chars:
                print("  Columns with unwanted characters:")
                for column, count in columns_with_unwanted_chars.items():
                    print(f"    {column}: {count} rows contain unwanted characters")
                    print(f"    Unwanted characters found: {', '.join(unwanted_chars_summary[column])}")
            else:
                print("  No unwanted characters found in columns")
            print()


if __name__ == "__main__":
    # Get the input folder path from environment variables
    input_folder = os.getenv('INPUT_FOLDER')
    
    # Count null values and check for duplicates in all CSV files in the folder
    count_nulls_and_duplicates_in_csv_files(input_folder)