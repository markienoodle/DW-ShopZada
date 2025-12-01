import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys

# Database Connection
db_user = 'shopzada_admin'
db_pass = 'password123'
db_host = 'shopzada-postgres-db'
db_port = '5432'
db_name = 'shopzada_dwh'

engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

def check_file(file_path_partial):
    # 1. Detect Path (Airflow vs ETL App)
    file_name = os.path.basename(file_path_partial)
    
    path_airflow = f"/opt/airflow/Project Dataset/{file_path_partial}"
    path_etl_app = f"/app/Project Dataset/{file_path_partial}"

    if os.path.exists(path_airflow):
        full_path = path_airflow
    elif os.path.exists(path_etl_app):
        full_path = path_etl_app
    else:
        print(f"❌ Error: Could not find {file_path_partial}")
        raise FileNotFoundError(f"File {file_path_partial} missing.")

    # 2. Derive Table Name (filename.ext -> filename_ext)
    # Derive Table Name: filename.ext -> filename_ext AND replace hyphens with underscores
    table_name = file_name.replace('.', '_').replace('-', '_')
    
    print(f"\n--- Verifying: {file_name} vs Table: {table_name} ---")

    # 3. Read File Count
    try:
        if file_name.endswith('.csv'):
            df = pd.read_csv(full_path)
        elif file_name.endswith('.xlsx'):
            df = pd.read_excel(full_path)
        elif file_name.endswith('.json'):
            df = pd.read_json(full_path)
        elif file_name.endswith('.parquet'):
            df = pd.read_parquet(full_path)
        elif file_name.endswith('.html'):
            df = pd.read_html(full_path)[0]
        elif file_name.endswith('.pickle'):
            df = pd.read_pickle(full_path)
            
        # Drop unnamed columns
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        file_count = len(df)
        
    except Exception as e:
        print(f"   ! Error reading file: {e}")
        raise e

    # 4. Read DB Count
    try:
        with engine.connect() as conn:
            # FIX: Added \" around {table_name} to handle hyphens
            query = text(f'SELECT COUNT(*) FROM raw_schema."{table_name}"')
            result = conn.execute(query)
            db_count = result.fetchone()[0]
    except Exception as e:
        print(f"   ! Error reading DB table {table_name}: {e}")
        # If table doesn't exist, we want to know
        raise e

    print(f"   File: {file_count} rows | DB: {db_count} rows")

    if file_count != db_count:
         raise ValueError(f"❌ MISMATCH! {file_name} has {file_count}, but {table_name} has {db_count}")
    else:
         print(f"✅ MATCH.")

# === DEFINE FILES TO CHECK ===
# Add every file you want to verify here. 
# Ensure the path includes the Department folder if applicable.

files_to_check = [
    # Business Dept
    'Business Department/product_list.xlsx',
    
    # Enterprise Dept
    'Enterprise Department/order_with_merchant_data1.parquet',
    'Enterprise Department/order_with_merchant_data2.parquet',
    'Enterprise Department/order_with_merchant_data3.csv',
    'Enterprise Department/merchant_data.html',
    'Enterprise Department/staff_data.html',

    # Operations Dept
    'Operations Department/line_item_data_prices1.csv',
    'Operations Department/line_item_data_prices2.csv',
    
    # Customer Management Dept
    'Customer Management Department/user_job.csv',
    'Customer Management Department/user_data.json',
    'Customer Management Department/user_credit_card.pickle',
    
    # Marketing Dept
    'Marketing Department/campaign_data.csv',
    'Marketing Department/transactional_campaign_data.csv',
    
    #Operations Dept 
    'Operations Department/line_item_data_prices1.csv',
    'Operations Department/line_item_data_prices2.csv',
    'Operations Department/line_item_data_prices3.parquet',
    'Operations Department/line_item_data_products1.csv',
    'Operations Department/line_item_data_products2.csv',
    'Operations Department/line_item_data_products3.parquet',
    'Operations Department/order_data_20200101-20200701.parquet',
    'Operations Department/order_data_20200701-20211001.pickle',
    'Operations Department/order_data_20211001-20220101.csv',
    'Operations Department/order_data_20220101-20221201.xlsx',
    'Operations Department/order_data_20221201-20230601.json',
    'Operations Department/order_data_20230601-20240101.html',
    'Operations Department/order_delays.html'
]

# Run the Loop
error_count = 0
for f in files_to_check:
    try:
        check_file(f)
    except Exception as e:
        print(e)
        error_count += 1

if error_count > 0:
    sys.exit(1)