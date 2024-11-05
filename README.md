# E-commerce Analysis with Data Pipeline

This project provides a comprehensive data processing pipeline for cleaning, analyzing, and importing e-commerce dataset from [Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data) into a MySQL database. The pipeline includes data profiling, cleaning, and exploratory data analysis capabilities.

&nbsp;
## Project Structure
```
├── data_profiling.py
├── data_cleaning.py
├── data_import.py
├── exploratory_data_analysis.sql
└── .env
```

&nbsp;
## Features
- **Data Profiling**: Analyzes CSV files for null values, duplicates, and data quality issues
- **Data Cleaning**: Comprehensive cleaning of e-commerce datasets including:
  - Text standardization
  - Date format validation
  - Geographical data validation
  - Foreign key relationship maintenance
  - Duplicate removal
- **Data Import**: Automated MySQL database population
- **Exploratory Analysis**: Extensive SQL queries for business insights

&nbsp;
## Requirements
- Python 3.7+
- pandas
- SQLAlchemy
- python-dotenv
- pymysql
- MySQL Server

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

5. Create a .env file with the following variables:
```
INPUT_FOLDER=/path/to/raw/data
OUTPUT_FOLDER=/path/to/cleaned/data
DB_NAME=ecommerce
DB_USER=your_username
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=3306
```

&nbsp;
## Usage
### 1. Data Profiling
```
python data_profiling.py
```
This script analyzes input CSV files and reports:
- Null value counts per column
- Duplicate row counts
- Unwanted character detection

### 2. Data Cleaning
```
python data_cleaning.py
```
Performs cleaning operations:
- Standardizes text fields
- Validates and corrects date formats
- Ensures data consistency
- Maintains referential integrity
- Outputs cleaned CSV files

### 3. Database Import
```
python data_import.py
```
Imports cleaned data into MySQL tables:
- geolocation
- customers
- orders
- reviews
- payments
- products
- sellers
- product_categories
- items

### 4. Data Analysis
Execute the SQL queries in exploratory_data_analysis.sql to analyze:
- Customer behavior
- Order patterns
- Product performance
- Shipping metrics
- Payment preferences
- Review distributions

&nbsp;
## Data Processing Details
### Cleaning Operations
- Geolocation Data
    - ZIP code standardization
    - State code validation
    - Coordinate range verification

- Customer Data
    - Duplicate customer removal
    - ZIP code validation
    - State code standardization

- Order Data
    - Date sequence validation
    - Missing value handling
    - Status verification

- Review Data
    - Score range validation
    - Date consistency checks
    - Duplicate review removal

- Payment Data
    - Value validation
    - Installment verification

- Product Data
    - Dimension validation
    - Category consistency
    - Description cleanup

&nbsp;
## Analysis Capabilities
- Order timeline analysis
- Price distribution analysis
- Category distribution
- Customer geography analysis
- Payment method preferences
- Delivery performance metrics
- Review score analysis
- Product dimension analysis

