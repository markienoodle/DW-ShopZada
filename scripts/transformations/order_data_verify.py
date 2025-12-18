import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text

# =========================================================
#                 DATABASE CONFIG
# =========================================================
DB_USER = os.getenv('DB_USER', 'shopzada_admin')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password123')
DB_HOST = os.getenv('DB_HOST', 'db')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'shopzada_dwh')

if os.path.exists('/opt/airflow'):
    DB_HOST = 'shopzada-postgres-db'

DB_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
STAGING_TABLE = "staging1_schema.order_data_cleaned"

def run_verification():
    engine = create_engine(DB_URI)
    
    try:
        with engine.connect() as conn:
            print(f"--- Starting Airflow Verification for {STAGING_TABLE} ---")

            # 1. Row Count Check
            # Your script merges 6 source tables + 4 additional tables. 
            # We ensure the final table isn't empty.
            row_count = conn.execute(text(f"SELECT COUNT(*) FROM {STAGING_TABLE}")).scalar()
            print(f"[INFO] Row count: {row_count}")
            if row_count == 0:
                print("CRITICAL: Staging table is empty!")
                sys.exit(1)

            # 2. Join Integrity Check
            # Since you joined merchant data on order_id, 
            # we check if merchant_id is missing for a significant chunk of data.
            null_merchants = conn.execute(text(f"""
                SELECT COUNT(*) FROM {STAGING_TABLE} WHERE merchant_id IS NULL
            """)).scalar()
            
            null_pct = (null_merchants / row_count) * 100
            print(f"[INFO] Missing merchant_id: {null_merchants} ({null_pct:.2f}%)")
            
            # If more than 10% of rows failed to join with merchant data, fail the task
            if null_pct > 10:
                print("CRITICAL: Join failure rate too high (>10%). Check merge keys.")
                sys.exit(1)

            # 3. Data Cleaning Check (Integer conversion)
            # Ensure delay_in_days and estimated_arrival are actually numbers
            # If the regex failed, these columns might contain NULLs
            invalid_integers = conn.execute(text(f"""
                SELECT COUNT(*) FROM {STAGING_TABLE} 
                WHERE delay_in_days IS NULL OR estimated_arrival IS NULL
            """)).scalar()
            
            if invalid_integers > 0:
                print(f"WARNING: {invalid_integers} rows have NULL numeric fields. Verify regex cleaning.")
                # We won't exit here unless you want strictly perfect data

            # 4. Date Parsing Check
            null_dates = conn.execute(text(f"SELECT COUNT(*) FROM {STAGING_TABLE} WHERE transaction_date IS NULL")).scalar()
            if null_dates > (row_count * 0.05): # Fail if > 5% of dates are unparseable
                print(f"CRITICAL: {null_dates} dates failed to parse. Check transaction_date format.")
                sys.exit(1)

            print("--- All Airflow Verification Checks Passed ---")
            sys.exit(0)

    except Exception as e:
        print(f"CRITICAL: Verification script encountered an error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_verification()