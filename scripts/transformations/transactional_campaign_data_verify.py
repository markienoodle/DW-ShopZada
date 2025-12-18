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
STAGING_TABLE = "transactional_campaign_data_cleaned"

REQUIRED_COLUMNS = [
    "transaction_date",
    "campaign_id",
    "order_id",
    "estimated_arrival",
    "availed"
]

# =========================================================
#                     VERIFICATION SCRIPT
# =========================================================
def verify_transactional_staging():
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

    # 5. Check estimated_arrival (numeric, NULLs allowed)
    try:
        df = pd.read_sql(f"SELECT estimated_arrival FROM {STAGING_SCHEMA}.{STAGING_TABLE}", engine)
        non_null_values = df['estimated_arrival'].dropna()
        if not pd.api.types.is_numeric_dtype(non_null_values):
            print("❌ 'estimated_arrival' column (non-NULL values) is not numeric.")
        else:
            print("✅ 'estimated_arrival' values are numeric (NULLs allowed).")
    except Exception as e:
        print(f"❌ Failed to verify 'estimated_arrival' column: {e}")

    # 6. Check availed column (should be Boolean/NULL)
    try:
        df = pd.read_sql(f"SELECT availed FROM {STAGING_SCHEMA}.{STAGING_TABLE}", engine)
        invalid_values = df[~df['availed'].isin([True, False, None])]
        if not invalid_values.empty:
            print(f"❌ 'availed' column contains invalid values:\n{invalid_values}")
        else:
            print("✅ 'availed' column values are valid Boolean or NULL.")
    except Exception as e:
        print(f"❌ Failed to verify 'availed' column: {e}")

    # 7. Count total rows
    try:
        row_count = pd.read_sql(f"SELECT COUNT(*) AS total_rows FROM {STAGING_SCHEMA}.{STAGING_TABLE}", engine)
        print(f"ℹ️ Total rows in staging table: {row_count['total_rows'][0]}")
    except Exception as e:
        print(f"❌ Failed to count rows: {e}")

    # 8. Preview some rows
    try:
        sample_df = pd.read_sql(f"SELECT * FROM {STAGING_SCHEMA}.{STAGING_TABLE} LIMIT 5", engine)
        print("✅ Sample data from staging table:")
        print(sample_df)
    except Exception as e:
        print(f"❌ Failed to read sample data: {e}")

if __name__ == "__main__":
    verify_transactional_staging()
