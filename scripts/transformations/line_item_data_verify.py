import os
import sys
from sqlalchemy import create_engine, text

# Database Configuration
DB_USER = os.getenv('DB_USER', 'shopzada_admin')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password123')
DB_HOST = os.getenv('DB_HOST', 'shopzada-postgres-db') # Standard Airflow host
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'shopzada_dwh')

DB_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
STAGING_TABLE = "staging1_schema.line_item_data_cleaned"

# Expected row count for 2 million record run
EXPECTED_MIN_ROWS = 1997174 

def verify_pipeline():
    engine = create_engine(DB_URI)
    
    with engine.connect() as conn:
        print(f"Checking data in {STAGING_TABLE}...")

        # 1. Row Count Verification
        row_count = conn.execute(text(f"SELECT COUNT(*) FROM {STAGING_TABLE}")).scalar()
        print(f"Result: {row_count} rows found.")
        
        if row_count < EXPECTED_MIN_ROWS:
            print(f"CRITICAL: Data loss detected. Expected ~2M, found {row_count}")
            sys.exit(1) # Fail the Airflow Task

        # 2. Join Integrity Verification (Unnamed Join Check)
        # If the join on unnamed_0 failed, these columns will be NULL
        null_count = conn.execute(text(f"""
            SELECT COUNT(*) FROM {STAGING_TABLE} 
            WHERE product_name IS NULL OR product_id IS NULL
        """)).scalar()
        
        if null_count > 0:
            print(f"CRITICAL: {null_count} rows failed the broadcast join!")
            sys.exit(1) # Fail the Airflow Task

        # 3. Data Type Validation
        # Ensure quantity was actually converted to an integer and isn't empty
        null_qty = conn.execute(text(f"SELECT COUNT(*) FROM {STAGING_TABLE} WHERE quantity IS NULL")).scalar()
        if null_qty > 0:
            print(f"CRITICAL: {null_qty} rows have invalid/non-numeric quantity values.")
            sys.exit(1)

    print("SUCCESS: All verification checks passed.")
    sys.exit(0)

if __name__ == "__main__":
    verify_pipeline()
