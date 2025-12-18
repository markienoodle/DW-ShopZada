import os
import pandas as pd
from sqlalchemy import create_engine

DB_USER = os.getenv('DB_USER', 'shopzada_admin')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password123')
DB_HOST = os.getenv('DB_HOST', 'db')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'shopzada_dwh')

# Detect Airflow container
if os.path.exists('/opt/airflow'):
    DB_HOST = 'shopzada-postgres-db'

DB_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
STAGING_TABLE = "staging1_schema.order_data_cleaned"

REQUIRED_COLUMNS = ["order_id", "user_id", "estimated_arrival", "transaction_date"]

def main():
    engine = create_engine(DB_URI)
    df = pd.read_sql(f"SELECT * FROM {STAGING_TABLE}", engine)

    print("=== Verification Report ===")

    # 1. Required Columns
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    print("PASS: All required columns exist" if not missing_cols else f"FAIL: Missing columns: {missing_cols}")

    # 2. transaction_date
    invalid_dates = df[df["transaction_date"].isna()]
    print("PASS: All transaction_date values are valid" if invalid_dates.empty else f"FAIL: {len(invalid_dates)} invalid dates found")

    # 3. estimated_arrival must be integer & non-negative
    if not pd.api.types.is_integer_dtype(df["estimated_arrival"]):
        print("FAIL: estimated_arrival is not integer type")
    elif (df["estimated_arrival"] < 0).any():
        print("FAIL: estimated_arrival contains negative values")
    else:
        print("PASS: estimated_arrival is numeric and non-negative")

    print("=== Verification Complete ===")

if __name__ == "__main__":
    main()