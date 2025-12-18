import os
import pandas as pd
from sqlalchemy import create_engine

# =========================================================
#                 DATABASE CONFIG
# =========================================================
DB_USER = os.getenv('DB_USER', 'shopzada_admin')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password123')
DB_HOST = os.getenv('DB_HOST', 'db')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'shopzada_dwh')

# Detect Airflow container
if os.path.exists('/opt/airflow'):
    DB_HOST = 'shopzada-postgres-db'

DB_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# =========================================================
#                 TABLE CONFIG
# =========================================================
STAGING_TABLE = "staging1_schema.order_with_merchant_data_cleaned"

REQUIRED_COLUMNS = [
    "order_id",
    "merchant_id",
    "staff_id"
]

def main():
    engine = create_engine(DB_URI)
    df = pd.read_sql(f"SELECT * FROM {STAGING_TABLE}", engine)

    print("=== Verification Report: order_with_merchant_data_cleaned ===")

    # ---------------------------------------------------------
    # 1. Required Columns Check
    # ---------------------------------------------------------
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        print(f"FAIL: Missing required columns: {missing}")
    else:
        print("PASS: All required columns exist")

    # ---------------------------------------------------------
    # 2. Check for NULL or empty string values
    # ---------------------------------------------------------
    for col in REQUIRED_COLUMNS:
        nulls = df[df[col].isna()]
        empties = df[df[col].astype(str).str.strip() == ""]

        if not nulls.empty or not empties.empty:
            print(f"FAIL: Column '{col}' contains {len(nulls)} NULL and {len(empties)} empty-string values")
        else:
            print(f"PASS: Column '{col}' contains no NULL or empty values")

    # ---------------------------------------------------------
    # 3. Column length validation (based on VARCHAR specs)
    # ---------------------------------------------------------
    # order_id should be at least 8 chars (UUID-like)
    invalid_order_id = df[df["order_id"].astype(str).str.len() < 8]
    print(
        "PASS: All order_id values have valid length"
        if invalid_order_id.empty
        else f"FAIL: {len(invalid_order_id)} invalid order_id values (too short)"
    )

    # merchant_id must fit VARCHAR(13)
    invalid_merchant = df[df["merchant_id"].astype(str).str.len() > 13]
    print(
        "PASS: All merchant_id values fit VARCHAR(13)"
        if invalid_merchant.empty
        else f"FAIL: {len(invalid_merchant)} merchant_id values exceed VARCHAR(13)"
    )

    # staff_id must fit VARCHAR(12)
    invalid_staff = df[df["staff_id"].astype(str).str.len() > 12]
    print(
        "PASS: All staff_id values fit VARCHAR(12)"
        if invalid_staff.empty
        else f"FAIL: {len(invalid_staff)} staff_id values exceed VARCHAR(12)"
    )

    print("=== Verification Complete ===")


if __name__ == "__main__":
    main()
