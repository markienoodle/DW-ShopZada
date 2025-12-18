import os
import pandas as pd
from sqlalchemy import create_engine, inspect

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

STAGING_SCHEMA = "staging1_schema"
STAGING_TABLE = "campaign_data_cleaned"

REQUIRED_COLUMNS = [
    "campaign_id",
    "campaign_name",
    "campaign_description",
    "discount"
]

# =========================================================
#                     VERIFICATION SCRIPT
# =========================================================
def verify_staging_table():
    engine = create_engine(DB_URI)
    
    # 1. Check connection
    try:
        with engine.connect() as conn:
            print("✅ Database connection successful.")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return

    # 2. Check table existence
    inspector = inspect(engine)
    if STAGING_TABLE not in inspector.get_table_names(schema=STAGING_SCHEMA):
        print(f"❌ Staging table '{STAGING_SCHEMA}.{STAGING_TABLE}' does not exist.")
        return
    print(f"✅ Staging table '{STAGING_SCHEMA}.{STAGING_TABLE}' exists.")

    # 3. Check required columns
    columns = [col['name'] for col in inspector.get_columns(STAGING_TABLE, schema=STAGING_SCHEMA)]
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in columns]
    if missing_cols:
        print(f"❌ Missing required columns in staging table: {missing_cols}")
    else:
        print(f"✅ All required columns exist: {REQUIRED_COLUMNS}")

    # 4. Check for Unnamed columns
    unnamed_cols = [col for col in columns if col.lower().startswith("unnamed")]
    if unnamed_cols:
        print(f"❌ Found Unnamed columns in staging table: {unnamed_cols}")
    else:
        print("✅ No Unnamed columns detected.")

    # 5. Check discount transformation (allow NULLs)
    try:
        df = pd.read_sql(f"SELECT discount FROM {STAGING_SCHEMA}.{STAGING_TABLE}", engine)
        non_null_discounts = df['discount'].dropna()
        if not pd.api.types.is_numeric_dtype(non_null_discounts):
            print("❌ Discount column (non-NULL values) is not numeric.")
        elif non_null_discounts.between(0, 1).all():
            print("✅ Discount values correctly transformed between 0 and 1 (NULLs allowed).")
        else:
            invalid_rows = non_null_discounts[~non_null_discounts.between(0,1)]
            print(f"❌ Some non-NULL discount values are outside 0-1 range:\n{invalid_rows}")
    except Exception as e:
        print(f"❌ Failed to verify discount column: {e}")

    # 6. Count total rows
    try:
        row_count = pd.read_sql(f"SELECT COUNT(*) AS total_rows FROM {STAGING_SCHEMA}.{STAGING_TABLE}", engine)
        print(f"ℹ️ Total rows in staging table: {row_count['total_rows'][0]}")
    except Exception as e:
        print(f"❌ Failed to count rows: {e}")

    # 7. Preview some rows
    try:
        sample_df = pd.read_sql(f"SELECT * FROM {STAGING_SCHEMA}.{STAGING_TABLE} LIMIT 5", engine)
        print("✅ Sample data from staging table:")
        print(sample_df)
    except Exception as e:
        print(f"❌ Failed to read sample data: {e}")

if __name__ == "__main__":
    verify_staging_table()
