import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys

# ==========================================
# 1. DATABASE CONNECTION
# ==========================================
db_user = 'shopzada_admin'
db_pass = 'password123'
db_host = 'shopzada-postgres-db'
db_port = '5432'
db_name = 'shopzada_dwh'

# Connect to the database
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

# ==========================================
# 2. VERIFICATION FUNCTION
# ==========================================
def check_file(file_path_partial):
    # Detect Path: Check if we are running in Airflow or the ETL App container
    file_name = os.path.basename(file_path_partial)
    
    path_airflow = f"/opt/airflow/Project Dataset/{file_path_partial}"
    path_etl_app = f"/app/Project Dataset/{file_path_partial}"

    if os.path.exists(path_airflow):
        full_path = path_airflow
    elif os.path.exists(path_etl_app):
        full_path = path_etl_app
    else:
        print(f"❌ Error: Could not find '{file_path_partial}'")
        raise FileNotFoundError(f"File missing from container paths.")

    # Derive Table Name: filename.ext -> filename_ext
    # Fixes hyphens so 'order-data' becomes 'order_data'
    table_name = file_name.replace('.', '_').replace('-', '_')
    
    print(f"\n--- Verifying: {file_name} vs Table: {table_name} ---")

    # Read File Count
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
            # HTML usually returns a list of dfs; we take the first one
            df = pd.read_html(full_path)[0]
        elif file_name.endswith('.pickle'):
            df = pd.read_pickle(full_path)
        else:
            print(f"Skipping unknown file type: {file_name}")
            return

        # Drop "Unnamed" columns (common index artifact)
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        file_count = len(df)
        
    except Exception as e:
        print(f"   ! Error reading file: {e}")
        raise e

    # Read DB Count
    try:
        with engine.connect() as conn:
            # Query the count from the raw_schema
            # Added quotes around {table_name} to handle numbers/hyphens in SQL
            query = text(f'SELECT COUNT(*) FROM raw_schema."{table_name}"')
            result = conn.execute(query)
            db_count = result.fetchone()[0]
    except Exception as e:
        print(f"   ! Error reading DB table {table_name}. Does it exist? {e}")
        raise e

    print(f"   File: {file_count} rows | DB: {db_count} rows")

    if file_count != db_count:
         raise ValueError(f"❌ MISMATCH! {file_name} has {file_count}, but {table_name} has {db_count}")
    else:
         print(f"✅ MATCH.")

# ==========================================
# 3. FILE LIST TO VERIFY
# ==========================================
files_to_check = [
    # Business Dept
    'Business Department/product_list.xlsx',

    # Enterprise Dept
    'Enterprise Department/order_with_merchant_data1.parquet',
    'Enterprise Department/order_with_merchant_data2.parquet',
    'Enterprise Department/order_with_merchant_data3.csv',
    'Enterprise Department/merchant_data.html',
    'Enterprise Department/staff_data.html',

    # Operations Dept (Line Items)
    'Operations Department/line_item_data_prices1.csv',
    'Operations Department/line_item_data_prices2.csv',
    'Operations Department/line_item_data_prices3.parquet',
    'Operations Department/line_item_data_products1.csv',
    'Operations Department/line_item_data_products2.csv',
    'Operations Department/line_item_data_products3.parquet',

    # Operations Dept (Orders)
    'Operations Department/order_data_20200101-20200701.parquet',
    'Operations Department/order_data_20200701-20211001.pickle',
    'Operations Department/order_data_20211001-20220101.csv',
    'Operations Department/order_data_20220101-20221201.xlsx',
    'Operations Department/order_data_20221201-20230601.json',
    'Operations Department/order_data_20230601-20240101.html',
    'Operations Department/order_delays.html',

    # Customer Management Dept
    'Customer Management Department/user_job.csv',
    'Customer Management Department/user_data.json',
    'Customer Management Department/user_credit_card.pickle',

    # Marketing Dept
    'Marketing Department/campaign_data.csv',
    'Marketing Department/transactional_campaign_data.csv'
]

# ==========================================
# 4. EXECUTION & CLEANUP LOOP
# ==========================================
error_count = 0
print(f"Starting verification for {len(files_to_check)} files...\n")

for f in files_to_check:
    try:
        check_file(f)
    except Exception as e:
        print(e)
        error_count += 1

if error_count > 0:
    print(f"\n❌ FAILED: {error_count} mismatches detected.")
    print("⚠️  INITIATING EMERGENCY CLEANUP: Dropping 'raw_schema' to prevent bad data persistence...")
    
    try:
        # Use engine.begin() to handle the transaction safely across versions
        with engine.begin() as conn:
            conn.execute(text("DROP SCHEMA IF EXISTS raw_schema CASCADE;"))
        print("💥 'raw_schema' has been dropped successfully.")
    except Exception as e:
        print(f"CRITICAL ERROR during cleanup: {e}")

    # Fail the Airflow task
    sys.exit(1)
else:
    print("\n✅ SUCCESS: All files verified perfectly.")